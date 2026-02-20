from __future__ import annotations

import os
import subprocess
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPTS_DIR.parent

SCRAPY_ROOT = REPO_ROOT / "jpt_scraper"
DATA_DIR = SCRAPY_ROOT / "data"

DAILY_CSV = DATA_DIR / "worldoil_daily.csv"

SPIDER_NAME = os.getenv("SPIDER_NAME", "worldoil_latest")
MAX_PAGES = int(os.getenv("MAX_PAGES", "10"))


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if DAILY_CSV.exists():
        DAILY_CSV.unlink()

    cmd = [
        "scrapy",
        "crawl",
        SPIDER_NAME,
        "-a",
        f"max_pages={MAX_PAGES}",
        "-O",
        str(DAILY_CSV),
    ]

    subprocess.run(cmd, cwd=str(SCRAPY_ROOT), check=True)


if __name__ == "__main__":
    main()
