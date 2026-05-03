import html
import json
import re
import time
from dataclasses import dataclass

import requests


# ---------------------------------------------------------------------------
# Text utilities — carried forward verbatim from v1 sentimentor.py
# ---------------------------------------------------------------------------

def extract_json_object(s: str) -> dict:
    s = s.strip()
    s = re.sub(r"^```(?:json)?|```$", "", s, flags=re.IGNORECASE | re.MULTILINE).strip()
    s = re.sub(r"<\|[^>]*\|>", "", s)

    try:
        return json.loads(s)
    except Exception:
        pass

    start = s.find("{")
    if start == -1:
        raise ValueError("No JSON object start found.")
    depth = 0
    for i in range(start, len(s)):
        if s[i] == "{":
            depth += 1
        elif s[i] == "}":
            depth -= 1
            if depth == 0:
                candidate = s[start:i+1]
                return json.loads(candidate)
    raise ValueError("No balanced JSON object found.")


def sanitize_text(text: str, max_chars: int = 4000) -> str:
    """Clean and truncate input before sending to LLM."""
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', text)
    text = re.sub(r'<\|[^>]*\|>', '', text)
    text = re.sub(r"```.*?```", "", text, flags=re.S)
    text = re.sub(r'([^\w\s])\1{3,}', r'\1\1', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text[:max_chars]


# ---------------------------------------------------------------------------
# Chunking
# ---------------------------------------------------------------------------

def chunk_text(text: str, chunk_chars: int = 4000) -> list[str]:
    """Split text into chunks of at most chunk_chars, splitting on whitespace boundaries."""
    if not text:
        return []
    chunks = []
    while len(text) > chunk_chars:
        split_at = text.rfind(" ", 0, chunk_chars)
        if split_at == -1:
            split_at = chunk_chars
        chunk = text[:split_at].strip()
        if chunk:
            chunks.append(chunk)
        text = text[split_at:].strip()
    if text:
        chunks.append(text)
    return chunks


# ---------------------------------------------------------------------------
# Classification result
# ---------------------------------------------------------------------------

@dataclass
class ClassificationResult:
    label: str
    score: float
    rationale: str
    partisan_quote: str | None


# ---------------------------------------------------------------------------
# LLM client — OpenRouter and Ollama/george backends
# Both expose OpenAI-compatible /chat/completions
# ---------------------------------------------------------------------------

PROMPT_TEMPLATE = """You are a sentiment classifier looking for ANY partisan \
or accusatory statements on US government websites or ANY potential violations of the \
Hatch Act, such as saying that a political party is responsible for \
shutting down the government.

If the text is accusatory or a potential Hatch Act violation, label it partisan. Otherwise label it neutral.
Score: partisan=1, neutral=0.

Return strict JSON ONLY with keys:
  label ∈ ["partisan","neutral"], score ∈ [0,1], rationale (short), partisan_quote (exact quote or null).

Text:
{text}
JSON:"""


class LLMClient:
    def __init__(self, backend: str, model: str, api_key: str, base_url: str,
                 max_retries: int = 3, timeout: int = 120):
        self.backend = backend
        self.model = model
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self.timeout = timeout

    def _headers(self) -> dict:
        h = {"Content-Type": "application/json"}
        if self.backend == "openrouter" and self.api_key:
            h["Authorization"] = f"Bearer {self.api_key}"
        return h

    def classify(self, chunk: str) -> ClassificationResult:
        sanitized = sanitize_text(chunk, max_chars=len(chunk))
        prompt = PROMPT_TEMPLATE.format(text=sanitized)
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": "Reply with JSON only. No prose, no code fences, no extra tokens."},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.0,
            "top_p": 1.0,
            "max_tokens": 256,
        }

        backoff = 5
        last_error = None
        for attempt in range(self.max_retries + 1):
            try:
                resp = requests.post(
                    f"{self.base_url}/chat/completions",
                    json=payload,
                    headers=self._headers(),
                    timeout=self.timeout,
                )
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", backoff))
                    time.sleep(retry_after)
                    backoff = min(backoff * 2, 60)
                    continue
                resp.raise_for_status()
                raw = resp.json()["choices"][0]["message"]["content"]
                return self._parse(raw)
            except requests.HTTPError as e:
                last_error = e
                if attempt < self.max_retries:
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 60)
            except Exception as e:
                last_error = e
                if attempt < self.max_retries:
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 60)

        return ClassificationResult(
            label="unknown",
            score=0.0,
            rationale=f"LLM error after {self.max_retries + 1} attempts: {last_error}",
            partisan_quote=None,
        )

    def _parse(self, raw: str) -> ClassificationResult:
        try:
            data = extract_json_object(raw)
        except Exception:
            return ClassificationResult(label="unknown", score=0.0, rationale=raw[:400], partisan_quote=None)

        lbl = str(data.get("label", "")).lower()
        if lbl not in {"partisan", "neutral"}:
            lbl = "unknown"
        try:
            score = float(data.get("score", 0))
        except Exception:
            score = 0.0
        score = 1.0 if lbl == "partisan" else 0.0 if lbl == "neutral" else 0.0
        rationale = str(data.get("rationale", ""))[:400]
        partisan_quote = data.get("partisan_quote") or None
        if partisan_quote:
            partisan_quote = str(partisan_quote)[:500]

        return ClassificationResult(label=lbl, score=score, rationale=rationale, partisan_quote=partisan_quote)
