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
    text_hash   TEXT,
    error       TEXT,
    UNIQUE (url_id, text_hash)
);

CREATE TABLE sentiment_runs (
    id               SERIAL PRIMARY KEY,
    scrape_result_id INTEGER NOT NULL REFERENCES scrape_results(id),
    model            TEXT NOT NULL,
    prompt_version   TEXT NOT NULL,
    label            TEXT,
    score            NUMERIC,
    rationale        TEXT,
    partisan_quote   TEXT,
    label_override   BOOLEAN NOT NULL DEFAULT false,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (scrape_result_id, model, prompt_version)
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

-- Most recent sentiment result per URL
CREATE MATERIALIZED VIEW mv_latest_sentiment AS
SELECT DISTINCT ON (u.id)
    u.id          AS url_id,
    u.url,
    u.domain,
    u.organization,
    u.domain_type,
    sr.id         AS scrape_result_id,
    sr.scraped_at,
    sr.status_code,
    sr.title,
    sn.id         AS sentiment_run_id,
    sn.model,
    sn.prompt_version,
    sn.label,
    sn.score,
    sn.rationale,
    sn.partisan_quote,
    sn.label_override
FROM urls u
JOIN scrape_results sr ON sr.url_id = u.id
JOIN sentiment_runs sn ON sn.scrape_result_id = sr.id
ORDER BY u.id, sr.scraped_at DESC, sn.created_at DESC;

CREATE UNIQUE INDEX ON mv_latest_sentiment (url_id);

-- Partisan count per day across all URLs
CREATE MATERIALIZED VIEW mv_partisan_over_time AS
SELECT
    DATE(sr.scraped_at)        AS scrape_date,
    COUNT(*)                   AS total_classified,
    COUNT(*) FILTER (WHERE sn.label = 'partisan') AS partisan_count,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE sn.label = 'partisan') / NULLIF(COUNT(*), 0),
        2
    )                          AS partisan_pct
FROM scrape_results sr
JOIN sentiment_runs sn ON sn.scrape_result_id = sr.id
GROUP BY DATE(sr.scraped_at)
ORDER BY scrape_date;

CREATE UNIQUE INDEX ON mv_partisan_over_time (scrape_date);

-- Coordinator /work query
CREATE INDEX idx_urls_active                ON urls(active) WHERE active = true;
CREATE INDEX idx_jobs_status                ON jobs(status) WHERE status IN ('pending', 'in_progress');

-- Domain throttling
CREATE INDEX idx_domain_throttle_dispatched ON domain_throttle(last_dispatched_at);

-- Scrape result lookups
CREATE INDEX idx_scrape_results_url_id      ON scrape_results(url_id);
CREATE INDEX idx_scrape_results_scraped_at  ON scrape_results(scraped_at);
CREATE INDEX idx_scrape_results_status      ON scrape_results(status_code) WHERE status_code = 200;
CREATE INDEX idx_scrape_results_text_hash   ON scrape_results(text_hash);

-- Sentiment run lookups
CREATE INDEX idx_sentiment_runs_scrape_id   ON sentiment_runs(scrape_result_id);
CREATE INDEX idx_sentiment_runs_model       ON sentiment_runs(model);
CREATE INDEX idx_sentiment_runs_label       ON sentiment_runs(label);
CREATE INDEX idx_sentiment_runs_created_at  ON sentiment_runs(created_at);

-- CISA registry queries
CREATE INDEX idx_domains_last_seen          ON domains(last_seen_at);
CREATE INDEX idx_domains_first_seen         ON domains(first_seen_at);
