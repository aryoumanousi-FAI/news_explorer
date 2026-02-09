from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pandas as pd


# -------------------
# PATH CONFIG
# -------------------
SCRIPTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPTS_DIR.parent

SCRAPY_ROOT = REPO_ROOT / "jpt_scraper"
DATA_DIR = SCRAPY_ROOT / "data"

MASTER_CSV = DATA_DIR / "jpt.csv"
NEW_CSV = DATA_DIR / "_new.csv"

SPIDER_NAME = os.getenv("SPIDER_NAME", "jpt_latest")
MAX_PAGES = int(os.getenv("MAX_PAGES", "10"))

# If True: new rows replace old rows when URL duplicates occur (recommended)
NEW_WINS_ON_DUPLICATE = True

# Abort if merged master would shrink (prevents accidental wipe)
GUARD_AGAINST_SHRINK = True


# -------------------
# STEP 1: SCRAPE ONLY
# -------------------
def scrape_to_new() -> None:
    """Runs Scrapy and writes ONLY to _new.csv (does not read/write master)."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if NEW_CSV.exists():
        NEW_CSV.unlink()

    print("--- Step 1: Scrape to _new.csv ---")
    print(f"Scrapy root: {SCRAPY_ROOT}")
    print(f"Spider:      {SPIDER_NAME}")
    print(f"MAX_PAGES:   {MAX_PAGES}")
    print(f"Output:      {NEW_CSV}")

    cmd = [
        "scrapy",
        "crawl",
        SPIDER_NAME,
        "-a",
        f"max_pages={MAX_PAGES}",
        "-O",
        str(NEW_CSV),
    ]

    subprocess.run(cmd, cwd=str(SCRAPY_ROOT), check=True)


# -------------------
# STEP 2: MERGE + DEDUPE
# -------------------
def merge_new_into_master() -> int:
    """Merges _new.csv into jpt.csv and deduplicates by url."""
    print("\n--- Step 2: Merge _new.csv into jpt.csv ---")

    if not NEW_CSV.exists():
        print("No _new.csv found. Nothing to merge.")
        return 0

    new_df = pd.read_csv(NEW_CSV)
    print(f"New rows: {len(new_df)}")

    if "url" not in new_df.columns:
        raise ValueError("_new.csv missing required 'url' column.")

    if MASTER_CSV.exists():
        old_df = pd.read_csv(MASTER_CSV)
        print(f"Old rows: {len(old_df)}")
    else:
        old_df = pd.DataFrame()
        print("Master not found. Creating fresh jpt.csv from _new.csv.")

    combined = pd.concat([old_df, new_df], ignore_index=True)

    # Parse datetimes if present (helps consistent sorting)
    if "scraped_at" in combined.columns:
        combined["scraped_at"] = pd.to_datetime(combined["scraped_at"], errors="coerce")
    if "published_date" in combined.columns:
        combined["published_date"] = pd.to_datetime(combined["published_date"], errors="coerce")

    # Sort so dedupe keeps correct row
    # If NEW wins, keep="last" and sort scraped_at ascending so newest tends to be last
    if "scraped_at" in combined.columns:
        combined = combined.sort_values("scraped_at", ascending=True, kind="mergesort")

    keep_rule = "last" if NEW_WINS_ON_DUPLICATE else "first"
    combined = combined.drop_duplicates(subset=["url"], keep=keep_rule)

    # Final sort for readability
    if "published_date" in combined.columns:
        sort_cols = ["published_date"] + (["scraped_at"] if "scraped_at" in combined.columns else [])
        asc = [False] + ([False] if "scraped_at" in combined.columns else [])
        combined = combined.sort_values(sort_cols, ascending=asc, kind="mergesort")

    # Guard against accidental shrink
    if GUARD_AGAINST_SHRINK and MASTER_CSV.exists() and not old_df.empty:
        old_unique = old_df["url"].nunique() if "url" in old_df.columns else len(old_df)
        new_unique = combined["url"].nunique()
        if new_unique < old_unique:
            raise RuntimeError(
                f"ABORT: merged master would shrink (unique urls {new_unique} < {old_unique}). "
                "Not overwriting jpt.csv."
            )

    # Atomic write
    tmp_master = MASTER_CSV.with_suffix(".csv.tmp")
    combined.to_csv(tmp_master, index=False)
    tmp_master.replace(MASTER_CSV)

    added_net = len(combined) - (len(old_df) if not old_df.empty else 0)
    print(f"Merge complete. Master rows: {len(combined)} (Net added: {added_net})")

    # Optional: keep _new.csv for debugging, or delete it
    # NEW_CSV.unlink(missing_ok=True)

    return max(added_net, 0)


def main() -> None:
    scrape_to_new()
    merge_new_into_master()


if __name__ == "__main__":
    main()
