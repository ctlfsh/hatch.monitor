import hashlib
import logging
import multiprocessing
import os
import random
import socket
import sys
import time
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import stealth_sync

# ---------------------------------------------------------------------------
# Module-level config — read with .get() so missing vars don't crash on import.
# Validation happens in main() with a human-readable error.
# ---------------------------------------------------------------------------

COORDINATOR_URL = os.environ.get("COORDINATOR_URL", "").rstrip("/")
API_KEY = os.environ.get("API_KEY", "")

SESSION = requests.Session()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class WorkerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return f"[{self.extra['worker_id']}] {msg}", kwargs


# ---------------------------------------------------------------------------
# Core scraping
# ---------------------------------------------------------------------------

def clean_text(text: str) -> str:
    lines = [ln.strip() for ln in text.splitlines()]
    lines = [ln for ln in lines if ln]
    return "\n".join(lines)


def extract_text(html: str):
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    title = soup.title.string.strip() if soup.title and soup.title.string else None
    text = soup.get_text(separator="\n")
    return title, clean_text(text)


def fetch(url: str, wait_ms: int = 3000, goto_timeout: int = 30000, headless: bool = True) -> tuple[int, str, list[str]]:
    """Returns (status, html, logs). logs is a list of strings for the parent to emit."""
    logs: list[str] = []

    def log(msg: str):
        logs.append(msg)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
        )
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            locale="en-US",
            timezone_id="America/New_York",
            extra_http_headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Cache-Control": "max-age=0",
            },
        )
        page = context.new_page()
        stealth_sync(page)

        response = None

        # Primary strategy: load — fires on the browser's real load event, equivalent
        # content to networkidle, faster, and not subject to heuristic-based timeout
        # failures caused by analytics/beacon scripts keeping the network perpetually busy.
        t0 = time.time()
        log(f"GOTO START url={url} strategy=load")
        try:
            response = page.goto(url, wait_until="load", timeout=goto_timeout)
            log(f"GOTO OK url={url} strategy=load status={response.status if response else '?'} elapsed={time.time()-t0:.1f}s")
        except PlaywrightTimeoutError:
            # load event timed out — page content is still available in the browser,
            # don't re-navigate. response stays None; status will fall back to 200.
            log(f"GOTO TIMEOUT url={url} strategy=load elapsed={time.time()-t0:.1f}s — continuing with page as-is")
        except Exception as e:
            log(f"GOTO ERROR url={url} strategy=load error={e} elapsed={time.time()-t0:.1f}s — trying domcontentloaded fallback")
            t1 = time.time()
            try:
                response = page.goto(url, wait_until="domcontentloaded", timeout=goto_timeout)
                log(f"GOTO FALLBACK OK url={url} strategy=domcontentloaded status={response.status if response else '?'} elapsed={time.time()-t1:.1f}s")
            except Exception as e2:
                log(f"GOTO FALLBACK FAIL url={url} strategy=domcontentloaded error={e2} elapsed={time.time()-t1:.1f}s")

        random_wait = wait_ms + random.randint(-500, 1000)
        page.wait_for_timeout(max(1000, random_wait))

        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight / 4)")
            page.wait_for_timeout(random.randint(200, 500))
        except Exception:
            pass

        html = page.content()
        status = response.status if response else 200
        log(f"PAGE CONTENT url={url} chars={len(html)} response_set={response is not None}")

        context.close()
        browser.close()
        return status, html, logs


# ---------------------------------------------------------------------------
# Hash — called only on successful fetches
# ---------------------------------------------------------------------------

def compute_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Subprocess isolation — Playwright crash stays in child, not the worker loop
# ---------------------------------------------------------------------------

def _fetch_worker(url, wait_ms, goto_timeout, headless, result_queue):
    try:
        status, html, logs = fetch(url, wait_ms=wait_ms, goto_timeout=goto_timeout, headless=headless)
        result_queue.put({"status": "ok", "status_code": status, "html": html, "logs": logs})
    except Exception as e:
        result_queue.put({"status": "error", "error": str(e), "logs": []})


def run_fetch_subprocess(url, wait_ms, goto_timeout, headless, fetch_timeout, job_id=None) -> tuple[int, str, str | None, list[str]]:
    """Returns (status_code, html, error, logs). On crash/timeout: (0, '', error_str, logs)."""
    q = multiprocessing.Queue()
    p = multiprocessing.Process(target=_fetch_worker, args=(url, wait_ms, goto_timeout, headless, q))
    p.start()
    pid = p.pid
    parent_logs: list[str] = [f"SUBPROCESS START job_id={job_id} url={url} pid={pid}"]

    t0 = time.time()
    p.join(timeout=fetch_timeout)
    elapsed = round(time.time() - t0, 1)

    if p.is_alive():
        p.kill()
        p.join()
        parent_logs.append(f"SUBPROCESS TIMEOUT job_id={job_id} url={url} pid={pid} elapsed={elapsed}s fetch_timeout={fetch_timeout}s")
        return 0, "", f"fetch timed out after {fetch_timeout}s", parent_logs

    if not q.empty():
        result = q.get()
        subprocess_logs = result.get("logs", [])
        all_logs = parent_logs + subprocess_logs
        if result["status"] == "ok":
            all_logs.append(f"SUBPROCESS OK job_id={job_id} url={url} pid={pid} elapsed={elapsed}s")
            return result["status_code"], result["html"], None, all_logs
        else:
            all_logs.append(f"SUBPROCESS ERROR job_id={job_id} url={url} pid={pid} elapsed={elapsed}s error={result['error']}")
            return 0, "", result["error"], all_logs

    parent_logs.append(f"SUBPROCESS NO RESULT job_id={job_id} url={url} pid={pid} — subprocess exited with no output")
    return 0, "", "fetch subprocess exited with no result", parent_logs


# ---------------------------------------------------------------------------
# Coordinator HTTP calls
# ---------------------------------------------------------------------------

def post_heartbeat(worker_id: str, url: str | None, action: str):
    try:
        SESSION.post(
            f"{COORDINATOR_URL}/heartbeat",
            json={"worker_id": worker_id, "url": url, "action": action},
            timeout=5,
        )
    except Exception:
        pass  # heartbeat is best-effort, never block the worker


def get_work() -> dict | None:
    """Returns {job_id, url} or None on 204. Raises on any other status."""
    resp = SESSION.get(f"{COORDINATOR_URL}/work", timeout=10)
    if resp.status_code == 204:
        return None
    resp.raise_for_status()
    return resp.json()


def post_result(payload: dict) -> dict:
    """Raises on non-2xx. Returns parsed JSON response."""
    resp = SESSION.post(f"{COORDINATOR_URL}/result", json=payload, timeout=15)
    resp.raise_for_status()
    return resp.json()


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
# Main
# ---------------------------------------------------------------------------

def main():
    if not COORDINATOR_URL:
        sys.exit("ERROR: COORDINATOR_URL env var is required")
    if not API_KEY:
        sys.exit("ERROR: API_KEY env var is required")
    SESSION.headers.update({"X-API-Key": API_KEY})

    _raw_worker_id = os.environ.get("WORKER_ID", "").strip()
    worker_id = _raw_worker_id if _raw_worker_id else f"{socket.gethostname()}-{os.getpid()}"
    log = WorkerAdapter(logging.getLogger("worker"), {"worker_id": worker_id})

    # spawn: required because Playwright module imports make fork unsafe on Linux
    multiprocessing.set_start_method("spawn", force=True)

    # Read all env config once — these don't change at runtime
    wait_ms       = int(os.environ.get("WAIT_MS", 3000))
    goto_timeout  = int(os.environ.get("GOTO_TIMEOUT", 30000))
    headless      = os.environ.get("HEADLESS", "1") != "0"
    fetch_timeout = int(os.environ.get("FETCH_TIMEOUT", 120))
    max_chars     = int(os.environ.get("MAX_TEXT_CHARS", 500000))

    idle_backoff  = Backoff(base=5, cap=60)
    error_backoff = Backoff(base=5, cap=60)

    log.info("Starting. coordinator=%s worker_id=%s", COORDINATOR_URL, worker_id)
    log.info("config wait_ms=%d goto_timeout=%d fetch_timeout=%d headless=%s max_chars=%d",
             wait_ms, goto_timeout, fetch_timeout, headless, max_chars)

    while True:
        # --- poll for work ---
        try:
            job = get_work()
            error_backoff.reset()
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 401:
                log.error("401 Unauthorized — check API key. Retrying in %ds", error_backoff._current)
            else:
                log.error("GET /work HTTP error: %s. Retrying.", e)
            error_backoff.wait()
            continue
        except Exception as e:
            log.error("GET /work failed: %s. Retrying.", e)
            error_backoff.wait()
            continue

        if job is None:
            post_heartbeat(worker_id, None, "waiting")
            log.info("No work available. Backoff %ds", idle_backoff._current)
            idle_backoff.wait()
            continue

        idle_backoff.reset()
        job_id = job["job_id"]
        url = job["url"]
        log.info("GET /work → job_id=%d url=%s", job_id, url)

        # --- fetch ---
        post_heartbeat(worker_id, url, "scraping")
        fetch_start = time.time()
        status_code, html, fetch_error, subprocess_logs = run_fetch_subprocess(
            url,
            wait_ms=wait_ms,
            goto_timeout=goto_timeout,
            headless=headless,
            fetch_timeout=fetch_timeout,
            job_id=job_id,
        )
        fetch_elapsed = round(time.time() - fetch_start, 1)

        for line in subprocess_logs:
            log.info("[subprocess] %s", line)

        if fetch_error:
            log.warning("FETCH ERROR job_id=%d url=%s error=%s elapsed=%.1fs", job_id, url, fetch_error, fetch_elapsed)
            title, text = None, ""
        else:
            title, text = extract_text(html)

        # --- process text ---
        # For fetch errors, text="" was set above. For successful fetches, apply safety rail.
        # MAX_TEXT_CHARS is a safety rail, not a design target. Store the full page.
        html_chars = len(html)
        text = (text or "")[:max_chars]
        word_count = len(text.split()) if text else 0
        truncated = len(text or "") == max_chars
        # text_hash=None for errors — coordinator stores all error rows (no dedup on errors).
        # text_hash=sha256[:16] for successes — deduped via partial unique index.
        text_hash = compute_hash(text) if not fetch_error else None

        log.info(
            "DONE job_id=%d url=%s status=%d words=%d chars=%d html_chars=%d truncated=%s hash=%s elapsed=%.1fs",
            job_id, url, status_code, word_count, len(text), html_chars,
            truncated, text_hash or "none", fetch_elapsed,
        )

        # --- post result ---
        scraped_at = datetime.now(timezone.utc).isoformat()
        payload = {
            "job_id": job_id,
            "status_code": status_code if not fetch_error else 0,
            "title": title,
            "text": text if text else None,
            "word_count": word_count,
            "text_hash": text_hash,
            "scraped_at": scraped_at,
            "error": fetch_error,
        }

        try:
            result = post_result(payload)
            log.info("POST /result → ok scrape_result_id=%s", result.get("scrape_result_id"))
        except Exception as e:
            log.error("POST /result failed job_id=%d url=%s error=%s — result lost", job_id, url, e)
            # No retry. The URL will be re-scraped when the job is reclaimed after 5 minutes.
        post_heartbeat(worker_id, None, "waiting")


if __name__ == "__main__":
    main()
