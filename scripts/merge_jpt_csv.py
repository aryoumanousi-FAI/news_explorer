from __future__ import annotations

from pathlib import Path
import csv
import pandas as pd


# ---------- Absolute paths ----------
ROOT = Path(__file__).resolve().parents[1]
MASTER = ROOT / "jpt_scraper" / "data" / "jpt.csv"
NEW = ROOT / "jpt_scraper" / "data" / "jpt_new.csv"
OUT = MASTER

EXPECTED_COLS = ["url", "title", "excerpt", "published_date", "topics", "tags", "scraped_at"]


def _sniff_sep(p: Path) -> str:
    """
    Robust delimiter detection (works for TSV disguised as .csv, and real CSV).
    """
    sample = p.read_text(encoding="utf-8", errors="ignore")[:50000]
    # Remove null bytes if any
    sample = sample.replace("\x00", "")
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", "\t", ";", "|"])
        return dialect.delimiter
    except Exception:
        # Fallback: if tabs appear a lot, treat as TSV
        first_line = sample.splitlines()[0] if sample.splitlines() else ""
        if first_line.count("\t") >= 2:
            return "\t"
        return ","


def _read_any_delim(p: Path) -> pd.DataFrame:
    if not p.exists():
        return pd.DataFrame()

    sep = _sniff_sep(p)
    df = pd.read_csv(
        p,
        sep=sep,
        dtype=str,
        keep_default_na=False,
        engine="python",  # more forgiving for weird delimiters/quotes
    )
    return df


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    # Strip whitespace + BOM
    df.columns = [c.strip().lstrip("\ufeff") for c in df.columns]

    # If URL header came in as a weird variant, fix it
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

    # Ensure expected cols exist
    for c in EXPECTED_COLS:
        if c not in df.columns:
            df[c] = ""

    # Clean strings
    for c in EXPECTED_COLS:
        df[c] = df[c].astype(str).str.strip()

    # Drop rows missing URL (critical key)
    df = df[df["url"] != ""].copy()
    return df


def _date_range(df: pd.DataFrame) -> tuple[str, str]:
    if df.empty or "published_date" not in df.columns:
        return ("", "")
    dt = pd.to_datetime(df["published_date"], errors="coerce")
    if dt.notna().sum() == 0:
        return ("", "")
    return (str(dt.min().date()), str(dt.max().date()))


def main() -> None:
    old_raw = _read_any_delim(MASTER)
    new_raw = _read_any_delim(NEW)

    old = _normalize(old_raw)
    new = _normalize(new_raw)

    # ---- Debug prints (so Actions logs show exactly what's happening) ----
    print("=== DEBUG: FILE READ ===")
    print(f"MASTER exists: {MASTER.exists()}  path={MASTER}")
    print(f"NEW exists:    {NEW.exists()}  path={NEW}")
    print(f"MASTER rows raw: {len(old_raw)}  cols: {list(old_raw.columns)[:12]}")
    print(f"NEW rows raw:    {len(new_raw)}  cols: {list(new_raw.columns)[:12]}")
    print(f"MASTER rows normalized: {len(old)}  date range: {_date_range(old)}")
    print(f"NEW rows normalized:    {len(new)}  date range: {_date_range(new)}")
    if not old.empty:
        print("MASTER sample urls:", old["url"].head(3).tolist())
    if not new.empty:
        print("NEW sample urls:", new["url"].head(3).tolist())
    print("========================")

    if old.empty and new.empty:
        print("Both master and new are empty. Nothing to do.")
        return

    # If master had rows but normalization resulted in 0, abort loudly (prevents wiping history)
    if MASTER.exists() and len(old_raw) > 0 and old.empty:
        raise RuntimeError(
            "MASTER had rows, but after normalization it's empty. "
            "This means the delimiter/header parsing is still wrong. Aborting to prevent data loss."
        )

    combined = pd.concat([old, new], ignore_index=True)

    # Prefer newest scrape per URL
    combined["_scraped"] = pd.to_datetime(combined["scraped_at"], errors="coerce")
    combined = combined.sort_values(["url", "_scraped"], ascending=[True, True])
    combined = combined.drop_duplicates(subset=["url"], keep="last").drop(columns=["_scraped"])

    # Sort newest first
    combined["_pub"] = pd.to_datetime(combined["published_date"], errors="coerce")
    combined = combined.sort_values("_pub", ascending=False, na_position="last").drop(columns=["_pub"])

    # Safety guard: never allow shrinking vs old (when old is big)
    if len(old) >= 1000 and len(combined) < len(old):
        raise RuntimeError(
            f"Safety abort: merged rows ({len(combined)}) < old rows ({len(old)}). "
            "Not overwriting MASTER."
        )

    OUT.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(OUT, index=False, encoding="utf-8")
    print(f"Merged: old={len(old)} new={len(new)} => out={len(combined)}")
    print(f"Wrote: {OUT}")


if __name__ == "__main__":
    main()
