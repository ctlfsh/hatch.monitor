import asyncio
import json
import logging
import os
import signal
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request, Response
from pydantic import BaseModel

import db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("coordinator")

KEYS_FILE = Path(os.environ.get("KEYS_FILE", "/app/keys.json"))
api_keys: dict[str, str] = {}


def reload_keys(*_):
    global api_keys
    if KEYS_FILE.exists():
        api_keys = json.loads(KEYS_FILE.read_text())
    else:
        api_keys = {}
    log.info("Keys reloaded: %d key(s)", len(api_keys))


signal.signal(signal.SIGHUP, reload_keys)


async def reclaim_loop():
    while True:
        await asyncio.sleep(60)
        try:
            n = await db.reclaim_timed_out_jobs(app.state.pool)
            if n:
                log.warning("Reclaimed %d timed-out job(s)", n)
        except Exception:
            log.exception("reclaim_loop error")


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await db.get_pool()
    app.state.pool = pool
    reload_keys()
    asyncio.create_task(reclaim_loop())
    yield
    await db.close_pool()


app = FastAPI(lifespan=lifespan)


async def require_api_key(request: Request) -> str:
    key = request.headers.get("X-API-Key", "")
    name = api_keys.get(key)
    if not name:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
    return name


# ── request / response models ────────────────────────────────────────────────

class ScrapeResult(BaseModel):
    job_id: int
    status_code: int
    title: str | None = None
    text: str | None = None
    word_count: int = 0
    text_hash: str
    scraped_at: datetime
    error: str | None = None


class SentimentResult(BaseModel):
    scrape_result_id: int
    model: str
    prompt_version: str
    label: str
    score: float
    rationale: str
    partisan_quote: str | None = None
    label_override: bool = False


# ── endpoints ─────────────────────────────────────────────────────────────────

@app.get("/healthz")
async def healthz():
    return {"ok": True}


@app.get("/work")
async def get_work(
    request: Request,
    worker_name: str = Depends(require_api_key),
):
    job = await db.claim_job(app.state.pool, worker_name)
    if job is None:
        log.info("worker=%s GET /work → 204", worker_name)
        return Response(status_code=204)
    log.info("worker=%s GET /work → job_id=%d url=%s", worker_name, job["job_id"], job["url"])
    return job


@app.post("/result")
async def post_result(
    payload: ScrapeResult,
    worker_name: str = Depends(require_api_key),
):
    scrape_result_id = await db.complete_job(
        app.state.pool,
        payload.job_id,
        payload.status_code,
        payload.title,
        payload.text,
        payload.word_count,
        payload.text_hash,
        payload.error,
        payload.scraped_at,
    )
    log.info("worker=%s POST /result job_id=%d scrape_result_id=%s", worker_name, payload.job_id, scrape_result_id)
    return {"ok": True, "scrape_result_id": scrape_result_id}


@app.get("/sentiment-work")
async def get_sentiment_work(
    model: str,
    prompt_version: str,
    batch_size: int = 20,
    worker_name: str = Depends(require_api_key),
):
    rows = await db.get_unclassified_scrapes(app.state.pool, model, prompt_version, batch_size)
    if not rows:
        log.info("worker=%s GET /sentiment-work model=%s → 204", worker_name, model)
        return Response(status_code=204)
    log.info("worker=%s GET /sentiment-work model=%s → %d item(s)", worker_name, model, len(rows))
    return rows


@app.post("/sentiment-result")
async def post_sentiment_result(
    payload: SentimentResult,
    worker_name: str = Depends(require_api_key),
):
    inserted = await db.insert_sentiment_run(
        app.state.pool,
        payload.scrape_result_id,
        payload.model,
        payload.prompt_version,
        payload.label,
        payload.score,
        payload.rationale,
        payload.partisan_quote,
        payload.label_override,
    )
    log.info("worker=%s POST /sentiment-result scrape_result_id=%d inserted=%s", worker_name, payload.scrape_result_id, inserted)
    return {"ok": True, "inserted": inserted}


@app.get("/status")
async def get_status(worker_name: str = Depends(require_api_key)):
    status = await db.get_status(app.state.pool)
    log.info("worker=%s GET /status", worker_name)
    return status


@app.post("/admin/reload-keys")
async def admin_reload_keys(worker_name: str = Depends(require_api_key)):
    reload_keys()
    log.info("worker=%s POST /admin/reload-keys key_count=%d", worker_name, len(api_keys))
    return {"ok": True, "key_count": len(api_keys)}


@app.post("/admin/reset-failed")
async def admin_reset_failed(worker_name: str = Depends(require_api_key)):
    count = await db.reset_failed_jobs(app.state.pool)
    log.info("worker=%s POST /admin/reset-failed reset_count=%d", worker_name, count)
    return {"ok": True, "reset_count": count}
