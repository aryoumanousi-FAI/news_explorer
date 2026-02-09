# scripts/merge_jpt_csv.py
from __future__ import annotations

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
MASTER = ROOT / "jpt_scraper" / "data" / "jpt.csv"
NEW = ROOT / "jpt_scraper" / "data" / "jpt_new.csv"
OUT = MASTER

EXPECTED_COLS = [
    "url",
    "title",
    "excerpt",
    "published_date",
    "topics",
    "tags",
    "scraped_at",
]

# Safety expectations for your dataset
MIN_UNIQUE_URLS = 7000
MAX_ALLOWED_MIN_DATE = pd.Timestamp("2014-01-01")


def _read_csv(p: Path) -> pd.DataFrame:
    if not p.exists():
        return pd.DataFrame()

    # Your file is a proper comma CSV with multiline quoted fields
    return pd.read_csv(
        p,
        sep=",",
        dtype=str,
        keep_default_na=False,
        engine="python",
        quotechar='"',
        doublequote=True,
    )


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df.columns = [c.strip().lstrip("\ufeff") for c in df.columns]

    # Ensure expected columns exist
    for c in EXPECTED_COLS:
        if c not in df.columns:
            df[c] = ""

    for c in EXPECTED_COLS:
        df[c] = df[c].astype(str).str.strip()

    # Keep only valid URLs
    df = df[df["url"] != ""].copy()
    return df


def _guard_master(old: pd.DataFrame) -> None:
    unique_urls = old["url"].nunique()
    dates = pd.to_datetime(old["published_date"], errors="coerce")
    min_date = dates.min() if dates.notna().any() else pd.NaT

    if unique_urls < MIN_UNIQUE_URLS:
        raise RuntimeError(
            f"ABORT: master has too few URLs ({unique_urls} < {MIN_UNIQUE_URLS})."
        )

    if pd.notna(min_date) and min_date > MAX_ALLOWED_MIN_DATE:
        raise RuntimeError(
            f"ABORT: master history too recent (starts {min_date.date()})."
        )


def main() -> None:
    old_raw = _read_csv(MASTER)
    new_raw = _read_csv(NEW)

    old = _normalize(old_raw)
    new = _normalize(new_raw)

    if old.empty and new.empty:
        print("Both master and new are empty. Nothing to do.")
        return

    # Protect the historical dataset
    if MASTER.exists() and not old.empty:
        _guard_master(old)

    combined = pd.concat([old, new], ignore_index=True)

    # De-dupe by URL (prefer newest scrape)
    combined["_scraped"] = pd.to_datetime(combined["scraped_at"], errors="coerce")
    combined = combined.sort_values(["url", "_scraped"])
    combined = combined.drop_duplicates(subset=["url"], keep="last")
    combined = combined.drop(columns=["_scraped"])

    # Sort newest first
    combined["_pub"] = pd.to_datetime(combined["published_date"], errors="coerce")
    combined = combined.sort_values("_pub", ascending=False, na_position="last")
    combined = combined.drop(columns=["_pub"])

    OUT.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(OUT, index=False, encoding="utf-8")

    print(
        f"Merged OK: old={len(old)} new={len(new)} "
        f"=> out={len(combined)} | unique_urls={combined['url'].nunique()}"
    )
    print(f"Wrote: {OUT}")


if __name__ == "__main__":
    main()
