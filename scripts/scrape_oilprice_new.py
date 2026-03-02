from __future__ import annotations

import os
import subprocess
from pathlib import Path

# Paths based on your project structure
SCRIPTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPTS_DIR.parent

# Adjusting to your specific Scrapy root
SCRAPY_ROOT = REPO_ROOT / "jpt_scraper" 
DATA_DIR = SCRAPY_ROOT / "jpt_scraper" / "data"

# File name for the fresh scrape
DAILY_CSV = DATA_DIR / "oilprice_daily.csv"

# Spider name matches the one we defined in oilprice_latest.py
SPIDER_NAME = os.getenv("SPIDER_NAME", "oilprice_latest")
MAX_PAGES = int(os.getenv("MAX_PAGES", "5")) # Defaulting to 5 pages for daily runs


def main() -> None:
    # Ensure the data directory exists
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Delete the old daily file if it exists to ensure a fresh '-O' (overwrite)
    if DAILY_CSV.exists():
        DAILY_CSV.unlink()

    # Construct the Scrapy command
    # -a max_pages passes the argument to the spider's __init__
    # -O (capital O) overwrites the file with exactly what was scraped this session
    cmd = [
        "scrapy",
        "crawl",
        SPIDER_NAME,
        "-a",
        f"max_pages={MAX_PAGES}",
        "-O",
        str(DAILY_CSV),
    ]

    print(f"--- Launching {SPIDER_NAME} ---")
    print(f"--- Output: {DAILY_CSV} ---")

    # Execute the command
    subprocess.run(cmd, cwd=str(SCRAPY_ROOT), check=True)


if __name__ == "__main__":
    main()
