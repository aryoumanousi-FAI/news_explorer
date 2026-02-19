from __future__ import annotations

import os
import subprocess
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPTS_DIR.parent

SCRAPY_ROOT = REPO_ROOT / "jpt_scraper"
DATA_DIR = SCRAPY_ROOT / "data"

DAILY_CSV = DATA_DIR / "jpt_daily.csv"

# Comma-separated list of spiders to run into the same daily CSV
SPIDERS = os.getenv("SPIDERS", "jpt_latest")
MAX_PAGES = int(os.getenv("MAX_PAGES", "10"))

# Optional hard-stop against the merged CSV (so you only fetch new pages)
STOP_AT_LAST_DATE = int(os.getenv("STOP_AT_LAST_DATE", "1"))
MERGED_CSV_PATH = os.getenv("MERGED_CSV_PATH", str(DATA_DIR / "jpt.csv"))


def run_spider(spider: str, mode: str) -> None:
    """
    mode:
      - "overwrite" uses -O (create/overwrite)
      - "append" uses -o (append)
    """
    feed_flag = "-O" if mode == "overwrite" else "-o"

    cmd = [
        "scrapy",
        "crawl",
        spider,
        "-a",
        f"max_pages={MAX_PAGES}",
    ]

    # pass hard-stop params to spiders that support it
    if STOP_AT_LAST_DATE:
        cmd += ["-a", "stop_at_last_date=1", "-a", f"csv_path={MERGED_CSV_PATH}"]

    cmd += [feed_flag, str(DAILY_CSV)]

    subprocess.run(cmd, cwd=str(SCRAPY_ROOT), check=True)


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    spiders = [s.strip() for s in SPIDERS.split(",") if s.strip()]
    if not spiders:
        raise ValueError("SPIDERS env var is empty.")

    # always start fresh
    if DAILY_CSV.exists():
        DAILY_CSV.unlink()

    print("--- Scrape step (daily) ---")
    print(f"Scrapy root: {SCRAPY_ROOT}")
    print(f"Spiders:     {spiders}")
    print(f"MAX_PAGES:   {MAX_PAGES}")
    print(f"Output:      {DAILY_CSV}")
    print(f"Hard stop:   {STOP_AT_LAST_DATE} (csv_path={MERGED_CSV_PATH})")

    for i, spider in enumerate(spiders):
        run_spider(spider, mode="overwrite" if i == 0 else "append")


if __name__ == "__main__":
    main()
