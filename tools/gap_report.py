import argparse
import csv
import io
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

CISA_URL = "https://raw.githubusercontent.com/cisagov/dotgov-data/main/current-federal.csv"
MIN_ROW_COUNT = 1000
STALE_DAYS = 7


def load_env():
    env_path = Path(__file__).parent.parent / ".env"
    load_dotenv(env_path)
    url = os.environ.get("DATABASE_URL")
    if not url:
        sys.exit("ERROR: DATABASE_URL not set — copy .env.example to .env and configure it")
    return url


def fetch_cisa_csv():
    try:
        resp = requests.get(CISA_URL, timeout=30)
    except requests.RequestException as e:
        sys.exit(f"ERROR: CISA fetch failed ({e}) — report aborted")

    if resp.status_code != 200:
        sys.exit(f"ERROR: CISA fetch failed (HTTP {resp.status_code}) — report aborted")

    last_modified = resp.headers.get("Last-Modified")
    if last_modified:
        try:
            lm_dt = datetime.strptime(last_modified, "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=timezone.utc)
            age = datetime.now(timezone.utc) - lm_dt
            if age > timedelta(days=STALE_DAYS):
                print(f"WARNING: CISA CSV last modified {lm_dt.date()} — data may be stale", file=sys.stderr)
        except ValueError:
            pass

    rows = list(csv.DictReader(io.StringIO(resp.text)))
    if len(rows) < MIN_ROW_COUNT:
        sys.exit(f"ERROR: CISA returned only {len(rows)} rows — suspiciously low, report aborted")

    return rows


def filter_rows(rows, domain_type, all_types):
    if all_types:
        return rows
    return [r for r in rows if r.get("Domain Type", "").strip() == domain_type]


def main():
    parser = argparse.ArgumentParser(description="Report coverage gaps between CISA registry and v2 urls table")
    parser.add_argument("--domain-type", default="Federal - Executive",
                        help='CISA domain type filter (default: "Federal - Executive")')
    parser.add_argument("--all", dest="all_types", action="store_true",
                        help="Include all CISA domain types")
    parser.add_argument("--never-scraped", action="store_true",
                        help="Show only domains in urls table that have never been scraped")
    args = parser.parse_args()

    db_url = load_env()

    print(f"Fetching CISA CSV ...")
    cisa_rows = fetch_cisa_csv()
    filtered = filter_rows(cisa_rows, args.domain_type, args.all_types)
    scope = "all types" if args.all_types else f'"{args.domain_type}"'
    cisa_domains = {r["Domain Name"].strip().lower(): r for r in filtered}

    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    u.domain,
                    u.url,
                    u.organization,
                    u.domain_type,
                    u.active,
                    u.notes,
                    d.last_seen_at,
                    COUNT(sr.id) AS scrape_count
                FROM urls u
                LEFT JOIN domains d ON d.domain = u.domain
                LEFT JOIN scrape_results sr ON sr.url_id = u.id
                GROUP BY u.domain, u.url, u.organization, u.domain_type, u.active, u.notes, d.last_seen_at
            """)
            db_rows = cur.fetchall()
    finally:
        conn.close()

    db_domains = {r["domain"]: r for r in db_rows}

    # Compute sets
    in_cisa_not_db = {d: cisa_domains[d] for d in cisa_domains if d not in db_domains}
    in_db_not_cisa = {d: db_domains[d] for d in db_domains if d not in cisa_domains}
    never_scraped_in_db = {d: db_domains[d] for d in db_domains if db_domains[d]["scrape_count"] == 0}
    in_cisa_and_db = {d for d in cisa_domains if d in db_domains}

    if args.never_scraped:
        print(f"\nDomains in urls table with no scrape_results ({len(never_scraped_in_db)}):")
        for domain, row in sorted(never_scraped_in_db.items()):
            print(f"  {domain:<45}  {row['organization'] or ''}")
        return

    # Summary
    print(f"\n{'CISA ' + scope + ' domains:':<45} {len(cisa_domains):>6}")
    print(f"{'In urls table:':<45} {len(db_domains):>6}")
    print(f"{'In both CISA and urls table:':<45} {len(in_cisa_and_db):>6}")
    print(f"{'In CISA, not in urls table:':<45} {len(in_cisa_not_db):>6}")
    print(f"{'Never scraped (in urls, no results):':<45} {len(never_scraped_in_db):>6}")
    print(f"{'In urls table, not in current CISA:':<45} {len(in_db_not_cisa):>6}")

    if in_cisa_not_db:
        print(f"\nMissing from urls table (in CISA, not yet seeded) — {len(in_cisa_not_db)}:")
        for domain, row in sorted(in_cisa_not_db.items()):
            print(f"  {domain:<45}  {row.get('Agency', '').strip()}")

    if in_db_not_cisa:
        print(f"\nIn urls table, not in current CISA (likely deregistered) — {len(in_db_not_cisa)}:")
        for domain, row in sorted(in_db_not_cisa.items()):
            last_seen = row["last_seen_at"].date() if row["last_seen_at"] else "unknown"
            print(f"  {domain:<45}  last_seen_at: {last_seen}  scrapes: {row['scrape_count']}")

    if never_scraped_in_db:
        print(f"\nIn urls table, never scraped — {len(never_scraped_in_db)}:")
        for domain, row in sorted(never_scraped_in_db.items()):
            print(f"  {domain:<45}  active: {row['active']}")


if __name__ == "__main__":
    main()
