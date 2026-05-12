import os
import sys
import subprocess
import time
from datetime import datetime, timedelta, timezone

import requests

SCHEDULE_HOUR = int(os.environ.get("SCHEDULE_HOUR", "3"))
SCHEDULE_INTERVAL_HOURS = os.environ.get("SCHEDULE_INTERVAL_HOURS")
CYCLE_URL_LIMIT = os.environ.get("CYCLE_URL_LIMIT")
COORDINATOR_URL = os.environ["COORDINATOR_URL"]
ADMIN_KEY = os.environ["ADMIN_KEY"]


def log(msg):
    print(f"{datetime.now(timezone.utc).isoformat()} {msg}", flush=True)


def seconds_until_next_run():
    if SCHEDULE_INTERVAL_HOURS:
        return float(SCHEDULE_INTERVAL_HOURS) * 3600
    now = datetime.now(timezone.utc)
    target = now.replace(hour=SCHEDULE_HOUR, minute=0, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    return (target - now).total_seconds()


def run_sync_urls():
    log("Running sync_urls.py ...")
    cmd = [sys.executable, "tools/sync_urls.py"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    for line in result.stdout.splitlines():
        log(f"  [sync_urls] {line}")
    for line in result.stderr.splitlines():
        log(f"  [sync_urls] STDERR: {line}")
    if result.returncode != 0:
        log(f"  [sync_urls] FAILED (exit {result.returncode})")
        return False
    log("sync_urls.py complete.")
    return True


def start_cycle():
    log("Starting new scrape cycle ...")
    params = {"started_by": "scheduler"}
    if CYCLE_URL_LIMIT:
        params["limit"] = CYCLE_URL_LIMIT
    resp = requests.post(
        f"{COORDINATOR_URL}/admin/start-cycle",
        params=params,
        headers={"X-API-Key": ADMIN_KEY},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    log(f"Cycle started: {data['urls_activated']} URL(s) activated.")


def main():
    if SCHEDULE_INTERVAL_HOURS:
        log(f"Scheduler started in TEST MODE. Interval: every {SCHEDULE_INTERVAL_HOURS}h. "
            f"URL limit: {CYCLE_URL_LIMIT or 'all'}.")
    else:
        log(f"Scheduler started. Will run daily at {SCHEDULE_HOUR:02d}:00 UTC.")

    while True:
        wait = seconds_until_next_run()
        log(f"Next run in {wait / 3600:.1f} hours.")
        time.sleep(wait)

        log("=== Daily run starting ===")
        try:
            run_sync_urls()
        except Exception as e:
            log(f"sync_urls.py raised an exception: {e}")

        try:
            start_cycle()
        except Exception as e:
            log(f"start_cycle raised an exception: {e}")

        log("=== Daily run complete ===")

        # sleep 60s before recalculating to avoid re-triggering immediately
        time.sleep(60)


if __name__ == "__main__":
    main()
