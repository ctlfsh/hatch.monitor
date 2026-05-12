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


async def start_cycle(
    pool: asyncpg.Pool,
    limit: int | None = None,
    started_by: str = "manual",
) -> tuple[int, datetime]:
    async with pool.acquire() as conn:
        async with conn.transaction():
            if limit is None:
                result = await conn.execute("""
                    UPDATE urls
                    SET needs_scraping = true, scrape_attempts = 0
                    WHERE active = true
                """)
            else:
                result = await conn.execute("""
                    UPDATE urls
                    SET needs_scraping = true, scrape_attempts = 0
                    WHERE active = true
                      AND id IN (
                          SELECT id FROM urls WHERE active = true ORDER BY RANDOM() LIMIT $1
                      )
                """, limit)

            count = int(result.split()[-1])

            row = await conn.fetchrow("""
                INSERT INTO cycle_state (id, cycle_started_at, cycle_started_by)
                VALUES (1, now(), $1)
                ON CONFLICT (id) DO UPDATE
                    SET cycle_started_at = now(), cycle_started_by = EXCLUDED.cycle_started_by
                RETURNING cycle_started_at
            """, started_by)

            return count, row['cycle_started_at']


async def get_cycle_state(pool: asyncpg.Pool) -> dict | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT cycle_started_at, cycle_started_by FROM cycle_state WHERE id = 1"
        )
        if row is None:
            return None
        return dict(row)


async def claim_job(pool: asyncpg.Pool, worker_name: str, max_scrape_attempts: int) -> dict | None:
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow("""
                SELECT u.id AS url_id, u.url, u.domain
                FROM urls u
                WHERE u.active = true
                  AND u.needs_scraping = true
                  AND u.scrape_attempts < $1
                  AND NOT EXISTS (
                      SELECT 1 FROM jobs j
                      WHERE j.url_id = u.id
                        AND j.status = 'in_progress'
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM domain_throttle dt
                      WHERE dt.domain = u.domain
                        AND dt.last_dispatched_at > now() - interval '60 seconds'
                  )
                ORDER BY RANDOM()
                LIMIT 1
                FOR UPDATE OF u SKIP LOCKED
            """, max_scrape_attempts)

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
    text_hash: str | None,
    error: str | None,
    scraped_at: datetime,
    worker_name: str | None = None,
    cycle_started_at: datetime | None = None,
) -> int:
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Fetch job first — if missing (already reclaimed), bail without touching urls or scrape_results.
            job = await conn.fetchrow(
                "SELECT url_id, status FROM jobs WHERE id = $1", job_id
            )
            if job is None:
                raise ValueError(f"job_id {job_id} not found")

            if job['status'] != 'in_progress':
                log.warning("job %d arrived late (status=%s) — inserting result anyway", job_id, job['status'])

            url_id = job['url_id']

            if text_hash is None:
                # Error result — always insert, no dedup. Increment scrape_attempts.
                result = await conn.fetchrow("""
                    INSERT INTO scrape_results
                        (url_id, scraped_at, status_code, title, text, word_count,
                         text_hash, error, scraped_by, cycle_started_at)
                    VALUES ($1, $2, $3, $4, $5, $6, NULL, $7, $8, $9)
                    RETURNING id
                """, url_id, scraped_at, status_code, title, text, word_count,
                    error, worker_name, cycle_started_at)

                # Leave needs_scraping = true so the URL can be retried (up to MAX_SCRAPE_ATTEMPTS).
                await conn.execute("""
                    UPDATE urls SET scrape_attempts = scrape_attempts + 1 WHERE id = $1
                """, url_id)
            else:
                # Successful result — dedup on (url_id, text_hash).
                # On conflict (same content), update scraped_at and cycle_started_at so the record stays current.
                result = await conn.fetchrow("""
                    INSERT INTO scrape_results
                        (url_id, scraped_at, status_code, title, text, word_count,
                         text_hash, error, scraped_by, cycle_started_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (url_id, text_hash) WHERE text_hash IS NOT NULL
                    DO UPDATE SET
                        scraped_at       = EXCLUDED.scraped_at,
                        scraped_by       = EXCLUDED.scraped_by,
                        cycle_started_at = EXCLUDED.cycle_started_at
                    RETURNING id
                """, url_id, scraped_at, status_code, title, text, word_count,
                    text_hash, error, worker_name, cycle_started_at)

                # Mark done — whether dedup fired or not, the URL was successfully scraped.
                await conn.execute("""
                    UPDATE urls SET needs_scraping = false WHERE id = $1
                """, url_id)

            scrape_result_id = result['id'] if result else await conn.fetchval(
                "SELECT id FROM scrape_results WHERE url_id = $1 AND text_hash = $2",
                url_id, text_hash
            )

            # Delete the job row — jobs table is now a pure in-flight tracker.
            await conn.execute("DELETE FROM jobs WHERE id = $1", job_id)

            return scrape_result_id


async def fail_job(pool: asyncpg.Pool, job_id: int, error: str):
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET status = 'failed', error = $1 WHERE id = $2",
            error, job_id
        )


async def reclaim_timed_out_jobs(pool: asyncpg.Pool) -> int:
    async with pool.acquire() as conn:
        # Atomically delete timed-out in_progress jobs and increment scrape_attempts on their URLs.
        result = await conn.execute("""
            WITH timed_out AS (
                DELETE FROM jobs
                WHERE status = 'in_progress'
                  AND claimed_at < now() - interval '5 minutes'
                RETURNING url_id
            )
            UPDATE urls SET scrape_attempts = scrape_attempts + 1
            WHERE id IN (SELECT url_id FROM timed_out)
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


async def get_status(
    pool: asyncpg.Pool,
    cycle_started_at: datetime | None = None,
    max_scrape_attempts: int = 3,
) -> dict:
    async with pool.acquire() as conn:
        in_progress = await conn.fetchval(
            "SELECT COUNT(*) FROM jobs WHERE status = 'in_progress'"
        )

        url_counts = await conn.fetchrow("""
            SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE active) AS active FROM urls
        """)

        urls_awaiting = await conn.fetchval("""
            SELECT COUNT(*) FROM urls
            WHERE needs_scraping = true AND active = true AND scrape_attempts < $1
        """, max_scrape_attempts)

        scrape_total = await conn.fetchval("SELECT COUNT(*) FROM scrape_results")

        # This run: resolved URL-jobs for the current cycle (one row per url_id, success wins over error)
        if cycle_started_at is not None:
            this_run_row = await conn.fetchrow("""
                WITH resolved AS (
                    SELECT
                        url_id,
                        bool_or(text_hash IS NOT NULL) AS has_success
                    FROM scrape_results
                    WHERE cycle_started_at = $1
                    GROUP BY url_id
                )
                SELECT
                    COUNT(*)                                    AS total,
                    COUNT(*) FILTER (WHERE has_success)         AS good,
                    COUNT(*) FILTER (WHERE NOT has_success)     AS failed
                FROM resolved
            """, cycle_started_at)
            this_run = {
                'total': this_run_row['total'],
                'good':  this_run_row['good'],
                'failed': this_run_row['failed'],
            }
        else:
            this_run = {'total': 0, 'good': 0, 'failed': 0}

        # All time: resolved URL-jobs across all cycles (grouped by cycle + url_id)
        all_time_row = await conn.fetchrow("""
            WITH resolved AS (
                SELECT
                    cycle_started_at,
                    url_id,
                    bool_or(text_hash IS NOT NULL) AS has_success
                FROM scrape_results
                WHERE cycle_started_at IS NOT NULL
                GROUP BY cycle_started_at, url_id
            )
            SELECT
                COUNT(*)                                    AS total,
                COUNT(*) FILTER (WHERE has_success)         AS good,
                COUNT(*) FILTER (WHERE NOT has_success)     AS failed
            FROM resolved
        """)
        all_time = {
            'total':  all_time_row['total'],
            'good':   all_time_row['good'],
            'failed': all_time_row['failed'],
        }

        sentiment_total = await conn.fetchval("SELECT COUNT(*) FROM sentiment_runs")

        unclassified = await conn.fetchval("""
            SELECT COUNT(*) FROM scrape_results sr
            WHERE sr.status_code = 200
              AND sr.word_count >= 50
              AND NOT EXISTS (SELECT 1 FROM sentiment_runs sn WHERE sn.scrape_result_id = sr.id)
        """)

        by_worker_rows = await conn.fetch("""
            SELECT scraped_by, COUNT(DISTINCT url_id) AS n
            FROM scrape_results
            WHERE scraped_by IS NOT NULL
            GROUP BY scraped_by
        """)
        by_worker = {r['scraped_by']: r['n'] for r in by_worker_rows}

        last_scrape_at = await conn.fetchval(
            "SELECT MAX(scraped_at) FROM scrape_results"
        )

        return {
            'jobs': {
                'in_progress': in_progress,
            },
            'urls': {
                'total':  url_counts['total'],
                'active': url_counts['active'],
            },
            'urls_awaiting': urls_awaiting,
            'this_run': this_run,
            'all_time': all_time,
            'scrape_results': {'total': scrape_total},
            'sentiment_runs': {
                'total':        sentiment_total,
                'unclassified': unclassified,
            },
            'by_worker': by_worker,
            'last_scrape_at': last_scrape_at.isoformat() if last_scrape_at else None,
        }
