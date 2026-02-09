from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pandas as pd


# -------------------
# PATH CONFIGURATION
# -------------------
# Script:  repo_root/scripts/update_csv.py
# Root:    repo_root
SCRIPTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPTS_DIR.parent

SCRAPY_ROOT = REPO_ROOT / "jpt_scraper"
DATA_DIR = SCRAPY_ROOT / "data"

MASTER_CSV = DATA_DIR / "jpt.csv"
TMP_OUT = DATA_DIR / "_new.csv"

SPIDER_NAME = os.getenv("SPIDER_NAME", "jpt_latest")
MAX_PAGES = int(os.getenv("MAX_PAGES", "10"))

# Behavior flags
# - If True: new rows replace old rows when URL duplicates occur (recommended).
# - If False: old rows win; new rows only add truly new URLs.
NEW_WINS_ON_DUPLICATE = True

# If True: abort if merged master would shrink vs existing master (prevents accidental wipe).
GUARD_AGAINST_SHRINK = True


# -------------------
# SCRAPE
# -------------------
def run_scrape() -> None:
    """Runs Scrapy and writes results to TMP_OUT (_new.csv)."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if TMP_OUT.exists():
        TMP_OUT.unlink()

    print("--- Starting Scrape ---")
    print(f"Repo root:        {REPO_ROOT}")
    print(f"Scrapy root:      {SCRAPY_ROOT}")
    print(f"Spider:           {SPIDER_NAME}")
    print(f"Master CSV:       {MASTER_CSV}")
    print(f"Temp output CSV:  {TMP_OUT}")
    print(f"MAX_PAGES:        {MAX_PAGES}")

    if MASTER_CSV.exists():
        print(f"  -> Found master CSV at {MASTER_CSV}")
    else:
        print(f"  -> WARNING: Master CSV NOT found at {MASTER_CSV}. Will create it after merge.")

    # NOTE:
    # We pass master path into the spider ONLY for reading "stop_at_last_date" logic.
    # The spider should NOT write to this path (it should only write to TMP_OUT via -O).
    cmd = [
        "scrapy",
        "crawl",
        SPIDER_NAME,
        "-a",
        f"max_pages={MAX_PAGES}",
        "-a",
        "stop_at_last_date=1",
        "-a",
        f"master_csv_path={MASTER_CSV}",
        "-O",
        str(TMP_OUT),
    ]

    subprocess.run(cmd, cwd=str(SCRAPY_ROOT), check=True)


# -------------------
# MERGE + DEDUPE
# -------------------
def merge_dedupe() -> int:
    print("\n--- Starting Merge ---")
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if not TMP_OUT.exists():
        print("No new scrape output found (_new.csv missing). Exiting.")
        return 0

    # Load new
    new_df = pd.read_csv(TMP_OUT)
    print(f"New rows scraped: {len(new_df)}")

    if "url" not in new_df.columns:
        raise ValueError("New scrape output missing required 'url' column.")

    # Load old (if exists)
    if MASTER_CSV.exists():
        old_df = pd.read_csv(MASTER_CSV)
        print(f"Existing rows loaded: {len(old_df)}")
    else:
        old_df = pd.DataFrame()
        print("Master CSV not found. Starting fresh.")

    # Combine
    combined = pd.concat([old_df, new_df], ignore_index=True)

    # Normalize datetimes for sorting (safe if columns absent)
    if "scraped_at" in combined.columns:
        combined["scraped_at"] = pd.to_datetime(combined["scraped_at"], errors="coerce")

    if "published_date" in combined.columns:
        combined["published_date"] = pd.to_datetime(combined["published_date"], errors="coerce")

    # Sort so dedupe keeps the row you intend
    # If NEW_WINS_ON_DUPLICATE=True, we want the newest version of a URL -> keep last
    # We sort by scraped_at (ascending) so newest tends to be last.
    if "scraped_at" in combined.columns:
        combined = combined.sort_values(by="scraped_at", ascending=True, kind="mergesort")

    # Dedupe on URL
    keep_rule = "last" if NEW_WINS_ON_DUPLICATE else "first"
    combined = combined.drop_duplicates(subset=["url"], keep=keep_rule)

    # Final presentation sort (newest published first)
    if "published_date" in combined.columns:
        if "scraped_at" in combined.columns:
            combined = combined.sort_values(
                by=["published_date", "scraped_at"],
                ascending=[False, False],
                kind="mergesort",
            )
        else:
            combined = combined.sort_values(by=["published_date"], ascending=[False], kind="mergesort")
    elif "scraped_at" in combined.columns:
        combined = combined.sort_values(by=["scraped_at"], ascending=[False], kind="mergesort")

    # Guard: never overwrite with a smaller master (prevents accidental wipe)
    if GUARD_AGAINST_SHRINK and MASTER_CSV.exists() and not old_df.empty:
        old_unique = old_df["url"].nunique() if "url" in old_df.columns else len(old_df)
        new_unique = combined["url"].nunique()
        if new_unique < old_unique:
            raise RuntimeError(
                f"ABORT: merged master would shrink (unique urls {new_unique} < {old_unique}). "
                "Not overwriting jpt.csv. Check paths / spider output."
            )

    # Atomic write
    tmp_master = MASTER_CSV.with_suffix(".csv.tmp")
    combined.to_csv(tmp_master, index=False)
    tmp_master.replace(MASTER_CSV)

    final_count = len(combined)
    added = final_count - (len(old_df) if not old_df.empty else 0)
    print(f"Merge Complete. Final Total: {
