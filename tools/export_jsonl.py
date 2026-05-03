import argparse
import json
import os
import sys
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

QUERY = """
WITH chunk_agg AS (
    SELECT
        sn.scrape_result_id,
        sn.model,
        sn.prompt_version,
        bool_or(sn.label = 'partisan')  AS is_partisan,
        MAX(sn.score)                    AS max_score,
        (ARRAY_AGG(sn.partisan_quote ORDER BY
            (sn.label = 'partisan') DESC, sn.score DESC NULLS LAST))[1] AS partisan_quote,
        (ARRAY_AGG(sn.rationale ORDER BY
            (sn.label = 'partisan') DESC, sn.score DESC NULLS LAST))[1] AS rationale,
        COUNT(*) AS chunk_count
    FROM sentiment_runs sn
    WHERE sn.model = %(model)s
    GROUP BY sn.scrape_result_id, sn.model, sn.prompt_version
)
SELECT
    u.url,
    u.domain,
    u.organization,
    u.domain_type,
    sr.scraped_at,
    sr.status_code,
    sr.title,
    sr.text,
    sr.word_count,
    ca.model,
    ca.prompt_version,
    ca.chunk_count,
    CASE WHEN ca.is_partisan THEN 'partisan' ELSE 'neutral' END AS label,
    ca.max_score                AS score,
    ca.rationale,
    ca.partisan_quote
FROM scrape_results sr
JOIN urls u ON u.id = sr.url_id
JOIN chunk_agg ca ON ca.scrape_result_id = sr.id
WHERE sr.scraped_at BETWEEN %(from_dt)s AND %(to_dt)s
ORDER BY u.url, sr.scraped_at
"""


def load_env():
    env_path = Path(__file__).parent.parent / ".env"
    load_dotenv(env_path)
    url = os.environ.get("DATABASE_URL")
    if not url:
        sys.exit("ERROR: DATABASE_URL not set — copy .env.example to .env and configure it")
    return url


def main():
    parser = argparse.ArgumentParser(description="Export scrape + sentiment data as JSONL")
    parser.add_argument("--model", required=True, help="LLM model name (e.g. anthropic/claude-haiku-4-5)")
    parser.add_argument("--from", dest="from_dt", required=True, help="Start date inclusive (YYYY-MM-DD)")
    parser.add_argument("--to",   dest="to_dt",   required=True, help="End date inclusive (YYYY-MM-DD)")
    parser.add_argument("--out",  default="-", help="Output file path (default: stdout)")
    args = parser.parse_args()

    db_url = load_env()
    conn = psycopg2.connect(db_url)

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(QUERY, {
                "model":   args.model,
                "from_dt": args.from_dt,
                "to_dt":   f"{args.to_dt} 23:59:59",
            })
            rows = cur.fetchall()
    finally:
        conn.close()

    out = open(args.out, "w", encoding="utf-8") if args.out != "-" else sys.stdout
    try:
        for row in rows:
            record = dict(row)
            # Convert datetime to ISO string for JSON serialisation
            if record.get("scraped_at"):
                record["scraped_at"] = record["scraped_at"].isoformat()
            # Nest sentiment under sentiment_llm key to match v1 format
            record["sentiment_llm"] = {
                "label":          record.pop("label"),
                "score":          float(record.pop("score") or 0),
                "rationale":      record.pop("rationale"),
                "partisan_quote": record.pop("partisan_quote"),
                "model":          record.pop("model"),
                "prompt_version": record.pop("prompt_version"),
                "chunk_count":    record.pop("chunk_count"),
            }
            out.write(json.dumps(record, ensure_ascii=False) + "\n")
    finally:
        if args.out != "-":
            out.close()

    print(f"Exported {len(rows)} record(s)", file=sys.stderr)


if __name__ == "__main__":
    main()
