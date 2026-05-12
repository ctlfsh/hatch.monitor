CREATE TABLE domains (
    domain           TEXT PRIMARY KEY,
    organization     TEXT,
    city             TEXT,
    state            TEXT,
    domain_type      TEXT,
    security_contact TEXT,
    agency           TEXT,
    first_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE domain_throttle (
    domain             TEXT PRIMARY KEY,
    last_dispatched_at TIMESTAMPTZ
);

CREATE TABLE urls (
    id                    SERIAL PRIMARY KEY,
    domain                TEXT REFERENCES domains(domain),
    url                   TEXT UNIQUE NOT NULL,
    scrape_interval_hours INTEGER NOT NULL DEFAULT 24,
    active                BOOLEAN NOT NULL DEFAULT true,
    needs_scraping        BOOLEAN NOT NULL DEFAULT true,
    scrape_attempts       INTEGER NOT NULL DEFAULT 0,
    organization          TEXT,
    domain_type           TEXT,
    notes                 TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE scrape_results (
    id          SERIAL PRIMARY KEY,
    url_id      INTEGER NOT NULL REFERENCES urls(id),
    scraped_at  TIMESTAMPTZ,
    status_code INTEGER,
    title       TEXT,
    text        TEXT,
    word_count  INTEGER,
    text_hash        TEXT,     -- SHA-256[:16] of extracted text; NULL for error results (all errors stored, no dedup)
    error            TEXT,
    scraped_by       TEXT,
    cycle_started_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX idx_scrape_results_url_text_hash
    ON scrape_results (url_id, text_hash)
    WHERE text_hash IS NOT NULL;

CREATE TABLE sentiment_runs (
    id               SERIAL PRIMARY KEY,
    scrape_result_id INTEGER NOT NULL REFERENCES scrape_results(id),
    model            TEXT NOT NULL,
    prompt_version   TEXT NOT NULL,
    chunk_index      INTEGER NOT NULL DEFAULT 0,  -- 0-based position of this chunk within the stored text
    label            TEXT,
    score            NUMERIC,
    rationale        TEXT,
    partisan_quote   TEXT,
    label_override   BOOLEAN NOT NULL DEFAULT false,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (scrape_result_id, model, prompt_version, chunk_index)
);

CREATE TABLE jobs (
    id           SERIAL PRIMARY KEY,
    url_id       INTEGER NOT NULL REFERENCES urls(id),
    job_type     TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending',
    claimed_by   TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at   TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error        TEXT
);

-- Most recent sentiment result per URL, aggregated across chunks.
-- A URL is 'partisan' if any chunk of its latest scrape is partisan.
-- partisan_quote and rationale come from the highest-scoring partisan chunk (or
-- the highest-scoring chunk overall when no chunk is partisan).
CREATE MATERIALIZED VIEW mv_latest_sentiment AS
WITH latest_scrape AS (
    -- Most recent scrape_result per URL
    SELECT DISTINCT ON (url_id)
        id AS scrape_result_id,
        url_id,
        scraped_at,
        status_code,
        title
    FROM scrape_results
    ORDER BY url_id, scraped_at DESC
),
chunk_agg AS (
    -- Aggregate all chunks for each (scrape_result, model, prompt_version)
    SELECT
        sn.scrape_result_id,
        sn.model,
        sn.prompt_version,
        bool_or(sn.label = 'partisan')                  AS is_partisan,
        MAX(sn.score)                                    AS max_score,
        COUNT(*)                                         AS chunk_count,
        -- Pick the quote from the highest-scoring partisan chunk, else highest overall
        (ARRAY_AGG(sn.partisan_quote ORDER BY
            (sn.label = 'partisan') DESC, sn.score DESC NULLS LAST))[1] AS partisan_quote,
        (ARRAY_AGG(sn.rationale ORDER BY
            (sn.label = 'partisan') DESC, sn.score DESC NULLS LAST))[1] AS rationale,
        bool_or(sn.label_override)                       AS label_override,
        MAX(sn.created_at)                               AS last_classified_at
    FROM sentiment_runs sn
    GROUP BY sn.scrape_result_id, sn.model, sn.prompt_version
),
-- Take the most recent (model, prompt_version) classification per scrape
latest_classification AS (
    SELECT DISTINCT ON (ca.scrape_result_id)
        ca.*
    FROM chunk_agg ca
    ORDER BY ca.scrape_result_id, ca.last_classified_at DESC
)
SELECT
    u.id          AS url_id,
    u.url,
    u.domain,
    u.organization,
    u.domain_type,
    ls.scrape_result_id,
    ls.scraped_at,
    ls.status_code,
    ls.title,
    lc.model,
    lc.prompt_version,
    lc.chunk_count,
    CASE WHEN lc.is_partisan THEN 'partisan' ELSE 'neutral' END AS label,
    lc.max_score  AS score,
    lc.rationale,
    lc.partisan_quote,
    lc.label_override
FROM urls u
JOIN latest_scrape ls ON ls.url_id = u.id
JOIN latest_classification lc ON lc.scrape_result_id = ls.scrape_result_id;

CREATE UNIQUE INDEX ON mv_latest_sentiment (url_id);

-- Partisan count per day across all URLs.
-- One row per (scrape_date, model, prompt_version) — counts URLs partisan that day,
-- not chunks. A URL counts as partisan if any chunk for that scrape is partisan.
CREATE MATERIALIZED VIEW mv_partisan_over_time AS
WITH url_day_label AS (
    -- Collapse chunks: one partisan/neutral label per (url, scrape_date, model, prompt_version)
    SELECT
        DATE(sr.scraped_at)     AS scrape_date,
        sn.model,
        sn.prompt_version,
        sr.url_id,
        bool_or(sn.label = 'partisan') AS is_partisan
    FROM scrape_results sr
    JOIN sentiment_runs sn ON sn.scrape_result_id = sr.id
    GROUP BY DATE(sr.scraped_at), sn.model, sn.prompt_version, sr.url_id
)
SELECT
    scrape_date,
    model,
    prompt_version,
    COUNT(*)                                         AS total_classified,
    COUNT(*) FILTER (WHERE is_partisan)              AS partisan_count,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE is_partisan) / NULLIF(COUNT(*), 0),
        2
    )                                                AS partisan_pct
FROM url_day_label
GROUP BY scrape_date, model, prompt_version
ORDER BY scrape_date, model, prompt_version;

CREATE UNIQUE INDEX ON mv_partisan_over_time (scrape_date, model, prompt_version);

CREATE TABLE cycle_state (
    id               INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    cycle_started_at TIMESTAMPTZ NOT NULL,
    cycle_started_by TEXT NOT NULL DEFAULT 'scheduler'
);

-- Coordinator /work query
CREATE INDEX idx_urls_active                ON urls(active) WHERE active = true;
CREATE INDEX idx_jobs_status                ON jobs(status) WHERE status = 'in_progress';

-- Domain throttling
CREATE INDEX idx_domain_throttle_dispatched ON domain_throttle(last_dispatched_at);

-- Scrape result lookups
CREATE INDEX idx_scrape_results_url_id      ON scrape_results(url_id);
CREATE INDEX idx_scrape_results_scraped_at  ON scrape_results(scraped_at);
CREATE INDEX idx_scrape_results_status      ON scrape_results(status_code) WHERE status_code = 200;
CREATE INDEX idx_scrape_results_text_hash   ON scrape_results(text_hash) WHERE text_hash IS NOT NULL;

-- Sentiment run lookups
CREATE INDEX idx_sentiment_runs_scrape_id    ON sentiment_runs(scrape_result_id);
CREATE INDEX idx_sentiment_runs_model        ON sentiment_runs(model);
CREATE INDEX idx_sentiment_runs_label        ON sentiment_runs(label);
CREATE INDEX idx_sentiment_runs_created_at   ON sentiment_runs(created_at);
CREATE INDEX idx_sentiment_runs_chunk        ON sentiment_runs(scrape_result_id, chunk_index);

-- CISA registry queries
CREATE INDEX idx_domains_last_seen          ON domains(last_seen_at);
CREATE INDEX idx_domains_first_seen         ON domains(first_seen_at);
