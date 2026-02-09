# scripts/merge_jpt_csv.py
from __future__ import annotations

from pathlib import Path
import pandas as pd


# ---------- Absolute paths (works no matter where you run from) ----------
ROOT = Path(__file__).resolve().parents[1]  # repo root
MASTER = ROOT / "jpt_scraper" / "data" / "jpt.csv"
NEW = ROOT / "jpt_scraper" / "data" / "jpt_new.csv"
OUT = MASTER

EXPECTED_COLS = ["url", "title", "excerpt", "published_date", "topics", "tags", "scraped_at"]


def _detect_sep(p: Path) -> str:
    """
    Your master file is currently tab-separated (TSV) even though it is named .csv.
    This detects whether the first line contains tabs; otherwise defaults to comma.
    """
    try:
        first_line = p.read_text(encoding="utf-8", errors="ignore").splitlines()[0]
    except Exception:
        return ","
    # If the file looks TSV, use tab. Otherwise CSV.
    if "\t" in first_line and "," not in first_line:
        return "\t"
    return ","


def _read_csv(p: Path) -> pd.DataFrame:
    if not p.exists():
        return pd.DataFrame()
    sep = _detect_sep(p)
    return pd.read_csv(p, sep=sep, dtype=str, keep_default_na=False)


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    # Strip whitespace and BOM-like chars from headers
    df.columns = [c.strip().lstrip("\ufeff") for c in df.columns]

    # If someone ever changes the URL header, try to map it back
    lower_cols = {c.lower(): c for c in df.columns}
    for candidate in ["url", "link", "permalink", "href"]:
        if "url" not in df.columns and candidate in lower_cols:
            df = df.rename(columns={lower_cols[candidate]: "url"})
            break

    return df


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = _normalize_columns(df)

    # Ensure expected columns exist
    for c in EXPECTED_COLS:
        if c not in df.columns:
            df[c] = ""

    # Clean strings
    for c in EXPECTED_COLS:
        df[c] = df[c].astype(str).str.strip()

    # Keep only rows with a URL
    df = df[df["url"] != ""].copy()

    # Keep published_date as string, but normalize parseability
    # (We sort later using parsed datetime)
    return df


def main() -> None:
    old_raw = _read_csv(MASTER)
    new_raw = _read_csv(NEW)

    old = _normalize(old_raw)
    new = _normalize(new_raw)

    if MASTER.exists() and not old_raw.empty and old.empty:
        raise RuntimeError(
            f"MASTER read {len(old_raw)} rows but normalized to 0 rows. "
            f"Likely header or delimiter issue in {MASTER}."
        )

    if old.empty and new.empty:
        print("Both master and new are empty. Nothing to do.")
        return

    combined = pd.concat([old, new], ignore_index=True)

    # Prefer newest scrape per URL
    combined["_scraped"] = pd.to_datetime(combined["scraped_at"], errors="coerce")
    combined = combined.sort_values(["url", "_scraped"], ascending=[True, True])
    combined = combined.drop_duplicates(subset=["url"], keep="last").drop(columns=["_scraped"])

    # Sort newest first by published_date if possible
    combined["_pub"] = pd.to_datetime(combined["published_date"], errors="coerce")
    combined = combined.sort_values("_pub", ascending=False, na_position="last").drop(columns=["_pub"])

    # Safety guard: prevent accidentally shrinking the dataset
    if len(old) >= 1000 and len(combined) < int(len(old) * 0.98):
        raise RuntimeError(
            f"Safety abort: merged rows ({len(combined)}) is smaller than old ({len(old)}). "
            "Not overwriting MASTER."
        )

    OUT.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(OUT, index=False, encoding="utf-8")
    print(f"Merged: old={len(old)} new={len(new)} => out={len(combined)}")
    print(f"Wrote: {OUT}")


if __name__ == "__main__":
    main()
