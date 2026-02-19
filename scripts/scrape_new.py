from __future__ import annotations

import os
import subprocess
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPTS_DIR.parent

SCRAPY_ROOT = REPO_ROOT / "jpt_scraper"
DATA_DIR = SCRAPY_ROOT / "data"

DAILY_CSV = DATA_DIR / "jpt_daily.csv"

# ---- JPT ONLY ----
ALLOWED_JPT_SPIDERS = {"jpt_latest"}  # add other JPT spiders here if you have them

# Change these lines to be JPT-specific
SPIDERS = os.getenv("JPT_SPIDERS", "jpt_latest")
MAX_PAGES = int(os.getenv("JPT_MAX_PAGES", "10"))
STOP_AT_LAST_DATE = int(os.getenv("JPT_STOP_AT_LAST_DATE", "1"))
# This is the critical one:
MERGED_CSV_PATH = os.getenv("JPT_MERGED_CSV_PATH", str(DATA_DIR / "jpt.csv"))


def run_spider(spider: str, mode: str) -> None:
    """
    mode:
      - "overwrite" uses -O (create/overwrite)
      - "append" uses -o (append)
    """
    feed_flag = "-O" if mode == "overwrite" else "-o"

    cmd = ["scrapy", "crawl", spider, "-a", f"max_pages={MAX_PAGES}"]

    if STOP_AT_LAST_DATE:
        cmd += ["-a", "stop_at_last_date=1", "-a", f"csv_path={MERGED_CSV_PATH}"]

    cmd += [feed_flag, str(DAILY_CSV)]
    subprocess.run(cmd, cwd=str(SCRAPY_ROOT), check=True)


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    spiders = [s.strip() for s in SPIDERS.split(",") if s.strip()]
    if not spiders:
        raise ValueError("SPIDERS env var is empty.")

    bad = [s for s in spiders if s not in ALLOWED_JPT_SPIDERS]
    if bad:
        raise ValueError(f"JPT-only script: disallowed spiders: {bad}. Allowed: {sorted(ALLOWED_JPT_SPIDERS)}")

    # always start fresh
    if DAILY_CSV.exists():
        DAILY_CSV.unlink()

    for i, spider in enumerate(spiders):
        run_spider(spider, mode="overwrite" if i == 0 else "append")


if __name__ == "__main__":
    main()
