from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
CSV_PATH = REPO_ROOT / "jpt_scraper" / "data" / "jpt.csv"
TMP_OUT = REPO_ROOT / "jpt_scraper" / "data" / "_new.csv"

# Change these if your spider uses different names/args
SPIDER_NAME = os.getenv("SPIDER_NAME", "jpt")  # e.g. "jpt"
LAST_N_PAGES = int(os.getenv("LAST_N_PAGES", "10"))


def run_scrape() -> None:
    """
    Runs your scraper and writes ONLY newly scraped rows to TMP_OUT.
    You have two options:
      A) Call scrapy spider directly (recommended if you already have Scrapy)
      B) Call a custom python scraper script if you have one
    """

    if TMP_OUT.exists():
        TMP_OUT.unlink()

    # Option A: Scrapy spider (most common)
    # Assumes your spider supports passing max_pages or last_pages.
    # If it doesn't yet, I explain below how to add it.
    cmd = [
        "scrapy",
        "crawl",
        SPIDER_NAME,
        "-a",
        f"last_pages={LAST_N_PAGES}",
        "-O",
        str(TMP_OUT),  # -O overwrites file each run
    ]

    subprocess.run(cmd, cwd=str(REPO_ROOT), check=True)


def merge_dedupe() -> int:
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Existing CSV not found: {CSV_PATH}")
    if not TMP_OUT.exists():
        print("No new scrape output found; nothing to merge.")
        return 0

    old_df = pd.read_csv(CSV_PATH)
    new_df = pd.read_csv(TMP_OUT)

    # Basic sanity check
    if "url" not in new_df.columns:
        raise ValueError("New scrape output must include a 'url' column for de-duplication.")

    combined = pd.concat([old_df, new_df], ignore_index=True)

    # De-dupe by URL (keep newest row if same URL appears)
    combined["scraped_at"] = pd.to_datetime(combined.get("scraped_at"), errors="coerce")
    combined = combined.sort_values(by=["scraped_at"], ascending=True)
    combined = combined.drop_duplicates(subset=["url"], keep="last")

    # Optional: sort for readability
    if "published_date" in combined.columns:
        combined["published_date"] = pd.to_datetime(combined["published_date"], errors="coerce")
        combined = combined.sort_values(by=["published_date", "scraped_at"], ascending=[False, False])

    combined.to_csv(CSV_PATH, index=False)

    added = len(combined) - len(old_df)
    return max(added, 0)


def main() -> None:
    run_scrape()
    added = merge_dedupe()
    print(f"Done. Added ~{added} new rows (after de-dupe).")


if __name__ == "__main__":
    main()
