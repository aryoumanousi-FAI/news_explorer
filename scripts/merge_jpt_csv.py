from __future__ import annotations

from pathlib import Path
import pandas as pd

# Anchor everything to the location of THIS FILE:
# repo/.../jpt_scraper/scripts/merge_jpt_csv.py
# parent -> jpt_scraper/
ROOT = Path(__file__).resolve().parents[1]

MASTER = ROOT / "data" / "jpt.csv"      # jpt_scraper/data/jpt.csv (canonical)
NEW = ROOT / "data" / "_new.csv"        # jpt_scraper/data/_new.csv


def main() -> None:
    if not MASTER.exists():
        raise FileNotFoundError(f"Master CSV not found: {MASTER}")
    if not NEW.exists():
        print("No _new.csv found. Nothing to merge.")
        return

    old_df = pd.read_csv(MASTER)
    new_df = pd.read_csv(NEW)

    if "url" not in old_df.columns:
        raise ValueError("Master CSV must include a 'url' column.")
    if "url" not in new_df.columns:
        raise ValueError("New scrape CSV must include a 'url' column.")

    combined = pd.concat([old_df, new_df], ignore_index=True)

    # Keep newest per URL
    if "scraped_at" in combined.columns:
        combined["scraped_at"] = pd.to_datetime(combined["scraped_at"], errors="coerce")
        combined = combined.sort_values("scraped_at")

    combined = combined.drop_duplicates(subset=["url"], keep="last")

    # Optional tidy sort
    if "published_date" in combined.columns:
        combined["published_date"] = pd.to_datetime(combined["published_date"], errors="coerce")
        sort_cols = ["published_date"]
        if "scraped_at" in combined.columns:
            sort_cols.append("scraped_at")
        combined = combined.sort_values(sort_cols, ascending=False)

    # SAFETY
    if len(combined) < len(old_df):
        raise RuntimeError(
            f"Refusing to overwrite: merged rows ({len(combined)}) < old rows ({len(old_df)}). "
            f"MASTER path: {MASTER}"
        )

    tmp = MASTER.with_suffix(".tmp")
    combined.to_csv(tmp, index=False)
    tmp.replace(MASTER)

    print(f"Merged OK. Old={len(old_df)} NewScrape={len(new_df)} Final={len(combined)}")
    print(f"Wrote master to: {MASTER}")


if __name__ == "__main__":
    main()
