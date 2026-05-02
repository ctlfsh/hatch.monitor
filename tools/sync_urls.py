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
        sys.exit(f"ERROR: CISA fetch failed ({e}) — sync aborted, no DB changes made")

    if resp.status_code != 200:
        sys.exit(f"ERROR: CISA fetch failed (HTTP {resp.status_code}) — sync aborted, no DB changes made")

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
        sys.exit(f"ERROR: CISA returned only {len(rows)} rows — suspiciously low, possible data issue, sync aborted")

    return rows


def filter_rows(rows, domain_type, all_types):
    if all_types:
        return rows
    return [r for r in rows if r.get("Domain Type", "").strip() == domain_type]


def sync(rows, db_url, dry_run, run_start):
    if dry_run:
        print(f"[dry-run] Would upsert {len(rows)} domains and urls")
        for r in rows:
            domain = r["Domain Name"].strip().lower()
            print(f"  domain: {domain}  org: {r.get('Agency', '').strip()}  type: {r.get('Domain Type', '').strip()}")
        return

    conn = psycopg2.connect(db_url)
    try:
        with conn:
            with conn.cursor() as cur:
                domains_upserted = 0
                urls_inserted = 0
                urls_existing = 0

                for r in rows:
                    domain = r["Domain Name"].strip().lower()
                    organization = r.get("Agency", "").strip() or None
                    domain_type = r.get("Domain Type", "").strip() or None
                    city = r.get("City", "").strip() or None
                    state = r.get("State", "").strip() or None
                    security_contact = r.get("Security Contact Email", "").strip() or None

                    cur.execute("""
                        INSERT INTO domains (domain, organization, city, state, domain_type, security_contact, first_seen_at, last_seen_at)
                        VALUES (%s, %s, %s, %s, %s, %s, now(), now())
                        ON CONFLICT (domain) DO UPDATE SET
                            organization  = EXCLUDED.organization,
                            domain_type   = EXCLUDED.domain_type,
                            last_seen_at  = now()
                    """, (domain, organization, city, state, domain_type, security_contact))
                    domains_upserted += 1

                    cur.execute("""
                        INSERT INTO urls (domain, url, scrape_interval_hours, active, organization, domain_type)
                        VALUES (%s, %s, 24, true, %s, %s)
                        ON CONFLICT (url) DO NOTHING
                    """, (domain, f"https://{domain}", organization, domain_type))

                    if cur.rowcount == 1:
                        urls_inserted += 1
                    else:
                        urls_existing += 1

                cur.execute("SELECT COUNT(*) FROM domains WHERE last_seen_at < %s", (run_start,))
                not_seen = cur.fetchone()[0]

        print(f"Domains upserted:  {domains_upserted}")
        print(f"URLs inserted:     {urls_inserted}")
        print(f"URLs already exist:{urls_existing}")
        if not_seen:
            print(f"WARNING: {not_seen} domain(s) not seen this run (last_seen_at < run start) — possible deregistrations")
        else:
            print("All known domains seen in this CISA pull.")
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Sync CISA federal domain registry into v2 Postgres")
    parser.add_argument("--domain-type", default="Federal - Executive",
                        help='CISA domain type filter (default: "Federal - Executive")')
    parser.add_argument("--all", dest="all_types", action="store_true",
                        help="Include all CISA domain types (overrides --domain-type)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print planned changes without writing to DB")
    args = parser.parse_args()

    db_url = load_env()
    run_start = datetime.now(timezone.utc)

    print(f"Fetching CISA CSV from {CISA_URL} ...")
    rows = fetch_cisa_csv()
    print(f"Fetched {len(rows)} total rows from CISA")

    filtered = filter_rows(rows, args.domain_type, args.all_types)
    scope = "all types" if args.all_types else f'"{args.domain_type}"'
    print(f"Filtered to {len(filtered)} rows ({scope})")

    sync(filtered, db_url, args.dry_run, run_start)


if __name__ == "__main__":
    main()
