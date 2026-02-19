from __future__ import annotations

from pathlib import Path
import pandas as pd


# -------------------
# PATHS
# -------------------
SCRIPTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPTS_DIR.parent

SCRAPY_ROOT = REPO_ROOT / "jpt_scraper"
DATA_DIR = SCRAPY_ROOT / "data"

FULL_CSV = DATA_DIR / "worldoil_full.csv"      # MASTER (do not change)
DAILY_CSV = DATA_DIR / "worldoil_daily.csv"    # overwritten daily
MERGED_CSV = DATA_DIR / "worldoil.csv"         # rebuilt every run (app reads this)


def load_csv(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        print(f"{label} not found: {path} (using empty)")
        return pd.DataFrame()
    df = pd.read_csv(path)
    print(f"{label} loaded: {len(df)} rows")
    return df


def atomic_write_csv(df: pd.DataFrame, path: Path) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(tmp, index=False)
    tmp.replace(path)


def main() -> None:
    full_df = load_csv(FULL_CSV, "FULL(master)")
    daily_df = load_csv(DAILY_CSV, "DAILY")

    if full_df.empty and daily_df.empty:
        raise RuntimeError("Both FULL and DAILY are empty. Nothing to merge.")

    for name, df in [("FULL", full_df), ("DAILY", daily_df)]:
        if not df.empty and "url" not in df.columns:
            raise ValueError(f"{name} CSV missing required 'url' column.")

    combined = pd.concat([full_df, daily_df], ignore_index=True)

    # Normalize datetimes if present
    if "scraped_at" in combined.columns:
        combined["scraped_at"] = pd.to_datetime(combined["scraped_at"], errors="coerce")

    if "published_date" in combined.columns:
        combined["published_date"] = pd.to_datetime(combined["published_date"], errors="coerce")

    # Sort oldest -> newest so "daily wins" when we keep last
    sort_cols = []
    if "published_date" in combined.columns:
        sort_cols.append("published_date")
    if "scraped_at" in combined.columns:
        sort_cols.append("scraped_at")
    if sort_cols:
        combined = combined.sort_values(sort_cols, ascending=True, kind="mergesort")

    # Deduplicate by url so DAILY wins
    merged = combined.drop_duplicates(subset=["url"], keep="last")

    # Final ordering: newest first
    if "published_date" in merged.columns:
        sort_cols = ["published_date"] + (["scraped_at"] if "scraped_at" in merged.columns else [])
        merged = merged.sort_values(sort_cols, ascending=[False] * len(sort_cols), kind="mergesort")
    elif "scraped_at" in merged.columns:
        merged = merged.sort_values("scraped_at", ascending=False, kind="mergesort")

    # Write ONLY merged output; do not touch FULL_CSV
    atomic_write_csv(merged, MERGED_CSV)

    print(f"Merged written: {MERGED_CSV}")
    print(f"Rows: {len(merged)} | Unique URLs: {merged['url'].nunique()}")


if __name__ == "__main__":
    main()
