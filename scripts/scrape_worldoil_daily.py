from __future__ import annotations

import os
import subprocess
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPTS_DIR.parent

SCRAPY_ROOT = REPO_ROOT / "jpt_scraper"
DATA_DIR = SCRAPY_ROOT / "data"

DAILY_CSV = DATA_DIR / "worldoil_daily.csv"

SPIDERS = os.getenv("SPIDERS", "worldoil_news")  # only worldoil spiders
MAX_PAGES = int(os.getenv("MAX_PAGES", "10"))

STOP_AT_LAST_DATE = int(os.getenv("STOP_AT_LAST_DATE", "1"))
FULL_CSV_PATH = os.getenv("FULL_CSV_PATH", str(DATA_DIR / "worldoil_full.csv"))


def run_spider(spider: str, mode: str) -> None:
    feed_flag = "-O" if mode == "overwrite" else "-o"

    cmd = ["scrapy", "crawl", spider, "-a", f"max_pages={MAX_PAGES}"]

    if STOP_AT_LAST_DATE:
        cmd += ["-a", "stop_at_last_date=1", "-a", f"csv_path={FULL_CSV_PATH}"]

    cmd += [feed_flag, str(DAILY_CSV)]
    subprocess.run(cmd, cwd=str(SCRAPY_ROOT), check=True)


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    spiders = [s.strip() for s in SPIDERS.split(",") if s.strip()]
    if not spiders:
        raise ValueError("SPIDERS env var is empty.")

    if DAILY_CSV.exists():
        DAILY_CSV.unlink()

    for i, spider in enumerate(spiders):
        run_spider(spider, mode="overwrite" if i == 0 else "append")


if __name__ == "__main__":
    main()
