import logging
import os
import socket
import sys
import time

import requests
from dotenv import load_dotenv

from llm import LLMClient, chunk_text

load_dotenv()

# ---------------------------------------------------------------------------
# Module-level config
# ---------------------------------------------------------------------------

COORDINATOR_URL = os.environ.get("COORDINATOR_URL", "").rstrip("/")
API_KEY         = os.environ.get("API_KEY", "")

SESSION = requests.Session()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("sentiment-worker")


# ---------------------------------------------------------------------------
# Backoff
# ---------------------------------------------------------------------------

class Backoff:
    def __init__(self, base=5, cap=60):
        self.base = base
        self.cap = cap
        self._current = base

    def wait(self):
        time.sleep(self._current)
        self._current = min(self._current * 2, self.cap)

    def reset(self):
        self._current = self.base


# ---------------------------------------------------------------------------
# Coordinator calls
# ---------------------------------------------------------------------------

def get_sentiment_work(model: str, prompt_version: str, batch_size: int) -> list[dict] | None:
    resp = SESSION.get(
        f"{COORDINATOR_URL}/sentiment-work",
        params={"model": model, "prompt_version": prompt_version, "batch_size": batch_size},
        timeout=15,
    )
    if resp.status_code == 204:
        return None
    resp.raise_for_status()
    return resp.json()


def post_sentiment_result(payload: dict) -> dict:
    resp = SESSION.post(f"{COORDINATOR_URL}/sentiment-result", json=payload, timeout=15)
    resp.raise_for_status()
    return resp.json()


def post_heartbeat(worker_id: str, url: str | None, action: str):
    try:
        SESSION.post(
            f"{COORDINATOR_URL}/heartbeat",
            json={"worker_id": worker_id, "url": url, "action": action},
            timeout=5,
        )
    except Exception:
        pass  # heartbeat is best-effort, never block the worker


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if not COORDINATOR_URL:
        sys.exit("ERROR: COORDINATOR_URL env var is required")
    if not API_KEY:
        sys.exit("ERROR: API_KEY env var is required")

    SESSION.headers.update({"X-API-Key": API_KEY})

    worker_id = os.environ.get("WORKER_ID", socket.gethostname())

    model         = os.environ.get("LLM_MODEL", "anthropic/claude-haiku-4-5")
    prompt_version = os.environ.get("PROMPT_VERSION", "v1")
    batch_size    = int(os.environ.get("SENTIMENT_BATCH_SIZE", 20))
    chunk_chars   = int(os.environ.get("LLM_CHUNK_CHARS", 4000))

    llm = LLMClient(
        backend     = os.environ.get("LLM_BACKEND", "openrouter"),
        model       = model,
        api_key     = os.environ.get("LLM_API_KEY", ""),
        base_url    = os.environ.get("LLM_BASE_URL", "https://openrouter.ai/api/v1"),
        max_retries = int(os.environ.get("LLM_MAX_RETRIES", 3)),
        timeout     = int(os.environ.get("LLM_TIMEOUT", 120)),
    )

    idle_backoff  = Backoff(base=5, cap=60)
    error_backoff = Backoff(base=5, cap=60)

    log.info("Starting. coordinator=%s model=%s prompt_version=%s", COORDINATOR_URL, model, prompt_version)

    while True:
        # --- poll for work ---
        try:
            batch = get_sentiment_work(model, prompt_version, batch_size)
            error_backoff.reset()
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 401:
                log.error("401 Unauthorized — check API key. Retrying in %ds", error_backoff._current)
            else:
                log.error("GET /sentiment-work HTTP error: %s. Retrying.", e)
            error_backoff.wait()
            continue
        except Exception as e:
            log.error("GET /sentiment-work failed: %s. Retrying.", e)
            error_backoff.wait()
            continue

        if batch is None:
            post_heartbeat(worker_id, None, "waiting")
            log.info("No work available. Backoff %ds", idle_backoff._current)
            idle_backoff.wait()
            continue

        idle_backoff.reset()
        log.info("Got batch of %d scrape result(s)", len(batch))

        for item in batch:
            scrape_result_id = item["scrape_result_id"]
            url              = item.get("url", "")
            text             = item.get("text") or ""

            post_heartbeat(worker_id, url, "analyzing")

            if not text.strip():
                log.warning("scrape_result_id=%d url=%s has empty text — skipping", scrape_result_id, url)
                continue

            chunks = chunk_text(text, chunk_chars=chunk_chars)
            log.info("scrape_result_id=%d url=%s chunks=%d", scrape_result_id, url, len(chunks))

            for chunk_index, chunk in enumerate(chunks):
                try:
                    result = llm.classify(chunk)
                except Exception as e:
                    log.error(
                        "LLM classify failed scrape_result_id=%d chunk=%d — skipping, will retry next poll: %s",
                        scrape_result_id, chunk_index, e,
                    )
                    break
                log.info(
                    "  chunk=%d label=%s score=%.2f",
                    chunk_index, result.label, result.score,
                )

                payload = {
                    "scrape_result_id": scrape_result_id,
                    "model":            model,
                    "prompt_version":   prompt_version,
                    "chunk_index":      chunk_index,
                    "label":            result.label,
                    "score":            result.score,
                    "rationale":        result.rationale,
                    "partisan_quote":   result.partisan_quote,
                    "label_override":   False,
                }

                try:
                    resp = post_sentiment_result(payload)
                    if not resp.get("inserted"):
                        log.info("  chunk=%d already classified (ON CONFLICT DO NOTHING)", chunk_index)
                except Exception as e:
                    log.error(
                        "POST /sentiment-result failed scrape_result_id=%d chunk=%d error=%s — skipping",
                        scrape_result_id, chunk_index, e,
                    )

        post_heartbeat(worker_id, None, "waiting")


if __name__ == "__main__":
    main()
