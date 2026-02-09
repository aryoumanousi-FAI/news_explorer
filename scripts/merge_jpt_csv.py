# scripts/merge_jpt_csv.py
from __future__ import annotations

from pathlib import Path
import csv
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
MASTER = ROOT / "jpt_scraper" / "data" / "jpt.csv"
NEW = ROOT / "jpt_scraper" / "data" / "jpt_new.csv"
OUT = MASTER

EXPECTED_COLS = ["url", "title", "excerpt", "published_date", "topics", "tags", "scraped_at"]

# Your full history is ~9000 rows; fail fast if master is unexpectedly small.
MIN_MASTER_ROWS = 7000


def _sniff_sep(p: Path) -> str:
    sample = p.read_text(encoding="utf-8", errors="ignore")[:50000].replace("\x00", "")
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", "\t", ";", "|"])
        return dialect.delimiter
    except Exception:
        first_line = sample.splitlines()[0] if sample.splitlines() else ""
        if first_line.count("\t") >= 2:
            return "\t"
        return ","


def _read_any_delim(p: Path) -> pd.DataFrame:
    if not p.exists():
        return pd.DataFrame()
    sep = _sniff_sep(p)
    return pd.read_csv(
        p,
        sep=sep,
        dtype=str,
        keep_default_na=False,
        engine="python",
    )


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df.columns = [c.strip().lstrip("\ufeff") for c in df.columns]

    lower_map = {c.lower(): c for c in df.columns}
    for candidate in ["url", "link", "permalink", "href"]:
        if "url" not in df.columns and candidate in lower_map:
            df = df.rename(columns={lower_map[candidate]: "url"})
            break
    return df


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = _normalize_columns(df)

    for c in EXPECTED_COLS:
        if c not in df.columns:
            df[c] = ""

    for c in EXPECTED_COLS:
        df[c] = df[c].astype(str).str.strip()

    df = df[df["url"] != ""].copy()
    return df


def main() -> None:
    old_raw = _read_any_delim(MASTER)
    new_raw = _read_any_delim(NEW)

    old = _normalize(old_raw)
    new = _normalize(new_raw)

    if old.empty and new.empty:
        print("Both master and new are empty. Nothing to do.")
        return

    # Guard 1: refuse to overwrite if master isn't your full dataset
    if MASTER.exists() and not old_raw.empty and len(old) < MIN_MASTER_ROWS:
        raise RuntimeError(
            f"ABORT: master jpt.csv is too small ({len(old)} rows). "
            f"Expected at least {MIN_MASTER_ROWS}. Not overwriting."
        )

    combined = pd.concat([old, new], ignore_index=True)

    # De-dupe by URL (keep newest scraped_at)
    combined["_scraped"] = pd.to_datetime(combined["scraped_at"], errors="coerce")
    combined = combined.sort_values(["url", "_scraped"], ascending=[True, True])
    combined = combined.drop_duplicates(subset=["url"], keep="last").drop(columns=["_scraped"])

    # Sort newest first by published_date
    combined["_pub"] = pd.to_datetime(combined["published_date"], errors="coerce")
    combined = combined.sort_values("_pub", ascending=False, na_position="last").drop(columns=["_pub"])

    # Guard 2: once master is "big", never allow shrinking
    if len(old) >= MIN_MASTER_ROWS and len(combined) < len(old):
        raise RuntimeError(
            f"ABORT: merged rows ({len(combined)}) < old rows ({len(old)}). Not overwriting."
        )

    OUT.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(OUT, index=False, encoding="utf-8")

    print(f"Merged OK: old={len(old)} new={len(new)} => out={len(combined)}")
    print(f"Wrote: {OUT}")


if __name__ == "__main__":
    main()
