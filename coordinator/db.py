import logging
import os
from datetime import datetime

import asyncpg

log = logging.getLogger(__name__)

_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            os.environ["DATABASE_URL"],
            min_size=2,
            max_size=10,
        )
    return _pool


async def close_pool():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


async def claim_job(pool: asyncpg.Pool, worker_name: str) -> dict | None:
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow("""
                SELECT u.id AS url_id, u.url, u.domain
                FROM urls u
                WHERE u.active = true
                  AND NOT EXISTS (
                      SELECT 1 FROM jobs j
                      WHERE j.url_id = u.id
                        AND j.status IN ('pending', 'in_progress')
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM domain_throttle dt
                      WHERE dt.domain = u.domain
                        AND dt.last_dispatched_at > now() - interval '60 seconds'
                  )
                  AND (
                      NOT EXISTS (SELECT 1 FROM scrape_results sr WHERE sr.url_id = u.id)
                      OR (
                          SELECT MAX(sr.scraped_at) FROM scrape_results sr WHERE sr.url_id = u.id
                      ) < now() - (u.scrape_interval_hours || ' hours')::interval
                  )
                ORDER BY RANDOM()
                LIMIT 1
                FOR UPDATE OF u SKIP LOCKED
            """)

            if row is None:
                return None

            job = await conn.fetchrow("""
                INSERT INTO jobs (url_id, job_type, status, claimed_by, claimed_at)
                VALUES ($1, 'scrape', 'in_progress', $2, now())
                RETURNING id AS job_id
            """, row['url_id'], worker_name)

            await conn.execute("""
                INSERT INTO domain_throttle (domain, last_dispatched_at)
                VALUES ($1, now())
                ON CONFLICT (domain) DO UPDATE SET last_dispatched_at = now()
            """, row['domain'])

            return {'job_id': job['job_id'], 'url': row['url']}


async def complete_job(
    pool: asyncpg.Pool,
    job_id: int,
    status_code: int,
    title: str | None,
    text: str | None,
    word_count: int,
    text_hash: str,
    error: str | None,
    scraped_at: datetime,
) -> int:
    async with pool.acquire() as conn:
        async with conn.transaction():
            job = await conn.fetchrow(
                "SELECT url_id, status FROM jobs WHERE id = $1", job_id
            )
            if job is None:
                raise ValueError(f"job_id {job_id} not found")

            if job['status'] != 'in_progress':
                log.warning("job %d arrived late (status=%s) — inserting result anyway", job_id, job['status'])

            result = await conn.fetchrow("""
                INSERT INTO scrape_results
                    (url_id, scraped_at, status_code, title, text, word_count, text_hash, error)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (url_id, text_hash) DO NOTHING
                RETURNING id
            """, job['url_id'], scraped_at, status_code, title, text, word_count, text_hash, error)

            scrape_result_id = result['id'] if result else await conn.fetchval(
                "SELECT id FROM scrape_results WHERE url_id = $1 AND text_hash = $2",
                job['url_id'], text_hash
            )

            await conn.execute(
                "UPDATE jobs SET status = 'completed', completed_at = now() WHERE id = $1",
                job_id
            )

            return scrape_result_id


async def fail_job(pool: asyncpg.Pool, job_id: int, error: str):
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET status = 'failed', error = $1 WHERE id = $2",
            error, job_id
        )


async def reclaim_timed_out_jobs(pool: asyncpg.Pool) -> int:
    async with pool.acquire() as conn:
        result = await conn.execute("""
            UPDATE jobs
            SET status = 'pending', claimed_by = NULL, claimed_at = NULL
            WHERE status = 'in_progress'
              AND claimed_at < now() - interval '5 minutes'
        """)
        count = int(result.split()[-1])
        return count


async def get_unclassified_scrapes(
    pool: asyncpg.Pool,
    model: str,
    prompt_version: str,
    batch_size: int = 20,
) -> list[dict]:
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT sr.id AS scrape_result_id, u.url, sr.title, sr.text, sr.scraped_at
            FROM scrape_results sr
            JOIN urls u ON u.id = sr.url_id
            WHERE sr.status_code = 200
              AND sr.word_count >= 50
              AND NOT EXISTS (
                  SELECT 1 FROM sentiment_runs sn
                  WHERE sn.scrape_result_id = sr.id
                    AND sn.model = $1
                    AND sn.prompt_version = $2
              )
            ORDER BY sr.scraped_at ASC
            LIMIT $3
        """, model, prompt_version, batch_size)
        return [dict(r) for r in rows]


async def insert_sentiment_run(
    pool: asyncpg.Pool,
    scrape_result_id: int,
    model: str,
    prompt_version: str,
    chunk_index: int,
    label: str,
    score: float,
    rationale: str,
    partisan_quote: str | None,
    label_override: bool = False,
) -> bool:
    async with pool.acquire() as conn:
        result = await conn.execute("""
            INSERT INTO sentiment_runs
                (scrape_result_id, model, prompt_version, chunk_index, label, score,
                 rationale, partisan_quote, label_override)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (scrape_result_id, model, prompt_version, chunk_index) DO NOTHING
        """, scrape_result_id, model, prompt_version, chunk_index, label, score,
            rationale, partisan_quote, label_override)
        return result.split()[-1] == '1'


async def get_status(pool: asyncpg.Pool) -> dict:
    async with pool.acquire() as conn:
        job_counts = await conn.fetch("""
            SELECT status, COUNT(*) AS n FROM jobs GROUP BY status
        """)
        jobs = {r['status']: r['n'] for r in job_counts}

        url_counts = await conn.fetchrow("""
            SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE active) AS active FROM urls
        """)

        scrape_total = await conn.fetchval("SELECT COUNT(*) FROM scrape_results")

        sentiment_total = await conn.fetchval("SELECT COUNT(*) FROM sentiment_runs")

        unclassified = await conn.fetchval("""
            SELECT COUNT(*) FROM scrape_results sr
            WHERE sr.status_code = 200
              AND sr.word_count >= 50
              AND NOT EXISTS (SELECT 1 FROM sentiment_runs sn WHERE sn.scrape_result_id = sr.id)
        """)

        by_worker_rows = await conn.fetch("""
            SELECT claimed_by, COUNT(*) AS n
            FROM jobs
            WHERE status = 'completed' AND claimed_by IS NOT NULL
            GROUP BY claimed_by
        """)
        by_worker = {r['claimed_by']: r['n'] for r in by_worker_rows}

        return {
            'jobs': {
                'pending':     jobs.get('pending', 0),
                'in_progress': jobs.get('in_progress', 0),
                'completed':   jobs.get('completed', 0),
                'failed':      jobs.get('failed', 0),
            },
            'urls': {
                'total':  url_counts['total'],
                'active': url_counts['active'],
            },
            'scrape_results': {'total': scrape_total},
            'sentiment_runs': {
                'total':        sentiment_total,
                'unclassified': unclassified,
            },
            'by_worker': by_worker,
        }


async def reset_failed_jobs(pool: asyncpg.Pool) -> int:
    async with pool.acquire() as conn:
        result = await conn.execute("""
            UPDATE jobs SET status = 'pending', error = NULL
            WHERE status = 'failed'
        """)
        return int(result.split()[-1])
