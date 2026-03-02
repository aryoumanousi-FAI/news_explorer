from __future__ import annotations

from pathlib import Path
import pandas as pd

# -------------------
# PATHS
# -------------------
SCRIPTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPTS_DIR.parent

# Adjusting to your specific Scrapy root and data folder
SCRAPY_ROOT = REPO_ROOT / "jpt_scraper"
DATA_DIR = SCRAPY_ROOT / "jpt_scraper" / "data"

MASTER_CSV = DATA_DIR / "oilprice_master.csv"  # The permanent archive
DAILY_CSV = DATA_DIR / "oilprice_new.csv"     # Freshly scraped today
MERGED_CSV = DATA_DIR / "oilprice.csv"        # The final file for the app


def load_csv(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        print(f"{label} not found: {path} (returning empty DataFrame)")
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
        print(f"{label} loaded: {len(df)} rows")
        return df
    except Exception as e:
        print(f"Error loading {label}: {e}")
        return pd.DataFrame()


def main() -> None:
    # 1. Load data
    master_df = load_csv(MASTER_CSV, "MASTER")
    daily_df = load_csv(DAILY_CSV, "DAILY")

    if master_df.empty and daily_df.empty:
        print("Both MASTER and DAILY are empty. Nothing to merge.")
        return

    # 2. Safety Check: Verify 'url' column exists as it is our unique identifier
    for name, df in [("MASTER", master_df), ("DAILY", daily_df)]:
        if not df.empty and "url" not in df.columns:
            raise ValueError(f"{name} CSV missing required 'url' column.")

    # 3. Combine: Stack them up. 
    # DAILY is placed second so it 'wins' in drop_duplicates(keep='last')
    combined = pd.concat([master_df, daily_df], ignore_index=True)

    # 4. Normalize 'scraped_at' for proper sorting
    if "scraped_at" in combined.columns:
        combined["scraped_at"] = pd.to_datetime(combined["scraped_at"], errors="coerce")
        # Mergesort is stable, keeping original order for identical timestamps
        combined = combined.sort_values("scraped_at", ascending=True, kind="mergesort")

    # 5. Deduplicate: Remove duplicate URLs, keeping the most recent entry (from DAILY)
    merged = combined.drop_duplicates(subset=["url"], keep="last")

    # 6. Final Ordering (Newest articles first)
    if "published_date" in merged.columns:
        merged["published_date"] = pd.to_datetime(merged["published_date"], errors="coerce")
        
        sort_cols = ["published_date"]
        asc = [False]
        
        if "scraped_at" in merged.columns:
            sort_cols.append("scraped_at")
            asc.append(False)
            
        merged = merged.sort_values(sort_cols, ascending=asc, kind="mergesort")
    elif "scraped_at" in merged.columns:
        merged = merged.sort_values("scraped_at", ascending=False, kind="mergesort")

    # 7. Atomic Write: Write to a temp file first, then replace the target
    # This prevents file corruption if the script crashes during the write process.
    tmp = MERGED_CSV.with_suffix(".csv.tmp")
    merged.to_csv(tmp, index=False)
    tmp.replace(MERGED_CSV)

    print("-" * 30)
    print(f"Final Merge Complete: {MERGED_CSV}")
    print(f"Total Rows: {len(merged)}")
    print(f"Unique URLs: {merged['url'].nunique()}")
    print("-" * 30)


if __name__ == "__main__":
    main()
