# app.py — JPT News Explorer (CSV)
# - Topics: OR logic
# - Countries: OR logic
# - Tags: AND logic

from __future__ import annotations

import ast
import html
import re
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Set

import pandas as pd
import streamlit as st


# -------------------
# Config
# -------------------
DATA_PATH = Path("jpt_scraper/data/jpt.csv")
ALL_TAGS_PATH = Path("all_tags.csv")
PAGE_SIZE = 25


# -------------------
# Normalization
# -------------------
WORD_SPLIT_RE = re.compile(r"(\s+|[-/])")

BASE_ACRONYMS = {
    "AI", "ML", "US", "UK", "UAE", "LNG", "CCS", "CO2", "CO₂", "M&A", "HSE", "OPEC",
    "NGL", "FPSO", "FLNG", "EOR", "IOR", "NPT", "R&D", "API", "ISO", "NACE",
    "IIoT", "OT", "IT", "SCADA", "PLC", "DCS", "ESG", "GHG",
}

COUNTRY_ABBREV = {
    "US": "US",
    "U.S.": "US",
    "USA": "US",
    "United States": "US",
    "United States Of America": "US",
    "UK": "UK",
    "U.K.": "UK",
    "United Kingdom": "UK",
    "Great Britain": "UK",
    "Britain": "UK",
    "UAE": "UAE",
    "U.A.E.": "UAE",
    "United Arab Emirates": "UAE",
}


def _normalize_text(x) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    return " ".join(str(x).split()).strip()


def _parse_listish(value) -> list[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    s = str(value).strip()
    if not s:
        return []
    if s.startswith("[") and s.endswith("]"):
        try:
            parsed = ast.literal_eval(s)
            if isinstance(parsed, list):
                return [str(v).strip() for v in parsed if str(v).strip()]
        except Exception:
            pass
    return [p.strip() for p in s.split(",") if p.strip()]


def _looks_like_acronym(token: str, acronyms: Set[str]) -> bool:
    t = token.strip()
    return (
        t.upper() in acronyms
        or re.fullmatch(r"[A-Z]{2,}", t)
        or re.fullmatch(r"[A-Za-z]{1,4}\d{1,3}", t)
        or (("&" in t or "." in t) and any(c.isalpha() for c in t))
    )


def normalize_phrase(s: str, acronyms: Set[str]) -> str:
    s = _normalize_text(s)
    if not s:
        return ""
    parts = WORD_SPLIT_RE.split(s)
    out = []
    for p in parts:
        raw = p.strip()
        if not raw or raw.isspace() or raw in {"-", "/"}:
            out.append(p)
        elif _looks_like_acronym(raw, acronyms):
            out.append(raw.upper())
        elif any(c.isupper() for c in raw[1:]):
            out.append(raw)
        else:
            out.append(raw[:1].upper() + raw[1:].lower())
    return re.sub(r"\s+", " ", "".join(out)).strip().replace("Co2", "CO2")


# -------------------
# Load + normalize data
# -------------------
@st.cache_data(ttl=60)
def load_data(csv_path: str, master_tags_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    for col in ["url", "title", "excerpt", "published_date", "topics", "tags", "scraped_at"]:
        if col not in df.columns:
            df[col] = ""

    df["url"] = df["url"].map(_normalize_text)
    df["title"] = df["title"].map(_normalize_text)
    df["excerpt"] = df["excerpt"].map(_normalize_text)
    df["published_date"] = pd.to_datetime(df["published_date"], errors="coerce").dt.date

    df["topics_list"] = df["topics"].map(_parse_listish)
    df["tags_list"] = df["tags"].map(_parse_listish)

    acronyms = set(BASE_ACRONYMS)

    df["topics_list"] = df["topics_list"].apply(
        lambda xs: [normalize_phrase(x, acronyms) for x in xs]
    )
    df["tags_list"] = df["tags_list"].apply(
        lambda xs: [normalize_phrase(x, acronyms) for x in xs]
    )

    df = df[df["url"].astype(bool)]
    df = df.drop_duplicates("url", keep="last")
    df = df.sort_values("published_date", ascending=False).reset_index(drop=True)

    # Countries derived from tags
    def extract_countries(tags: List[str]) -> List[str]:
        found = set()
        for t in tags:
            if t in COUNTRY_ABBREV:
                found.add(COUNTRY_ABBREV[t])
        return sorted(found)

    df["countries_list"] = df["tags_list"].apply(extract_countries)

    return df


# -------------------
# Filtering logic
# -------------------
def must_include_all(selected: list[str], row_values: list[str]) -> bool:
    return not selected or all(s in set(row_values) for s in selected)


def must_include_any(selected: list[str], row_values: list[str]) -> bool:
    return not selected or any(s in set(row_values) for s in selected)


def apply_filters(
    df: pd.DataFrame,
    start_d: date,
    end_d: date,
    keywords: list[str],
    any_mode: bool,
    topics: list[str],
    tags: list[str],
    countries: list[str],
) -> pd.Series:
    mask = df["published_date"].between(start_d, end_d, inclusive="both")

    if keywords:
        text = (df["title"] + " " + df["excerpt"]).str.lower()
        if any_mode:
            mask &= text.apply(lambda t: any(k.lower() in t for k in keywords))
        else:
            mask &= text.apply(lambda t: all(k.lower() in t for k in keywords))

    mask &= df["topics_list"].apply(lambda xs: must_include_any(topics, xs))
    mask &= df["countries_list"].apply(lambda xs: must_include_any(countries, xs))
    mask &= df["tags_list"].apply(lambda xs: must_include_all(tags, xs))

    return mask


def make_link(url: str, title: str) -> str:
    return f'<a href="{html.escape(url)}" target="_blank">{html.escape(title)}</a>'


# -------------------
# UI
# -------------------
st.set_page_config("JPT News", layout="wide")
st.title("JPT News Explorer")

df = load_data(str(DATA_PATH), str(ALL_TAGS_PATH))
st.caption(f"Loaded {len(df)} articles")

min_d, max_d = df["published_date"].min(), df["published_date"].max()

with st.sidebar:
    st.header("Filters")

    keyword_input = st.text_input("Keywords (comma-separated)")
    keyword_mode = st.radio("Keyword match", ["Any", "All"])
    keywords = [k.strip() for k in keyword_input.split(",") if k.strip()]

    start_d, end_d = st.date_input("Date range", (min_d, max_d))

    topics = st.multiselect("Topics (match ANY)", sorted({t for xs in df["topics_list"] for t in xs}))
    tags = st.multiselect("Tags (match ALL)", sorted({t for xs in df["tags_list"] for t in xs}))
    countries = st.multiselect("Country (match ANY)", sorted({c for xs in df["countries_list"] for c in xs}))

mask = apply_filters(
    df,
    start_d,
    end_d,
    keywords,
    keyword_mode == "Any",
    topics,
    tags,
    countries,
)

results = df[mask]
st.subheader(f"Results ({len(results)})")

if results.empty:
    st.info("No matches found.")
    st.stop()

page = st.number_input("Page", 1, max(1, (len(results) - 1) // PAGE_SIZE + 1))
page_df = results.iloc[(page - 1) * PAGE_SIZE : page * PAGE_SIZE].copy()

page_df["Article"] = page_df.apply(lambda r: make_link(r["url"], r["title"]), axis=1)

st.write(
    page_df[["published_date", "Article", "countries_list", "topics_list", "tags_list", "excerpt"]]
    .rename(columns={"published_date": "Date", "excerpt": "Excerpt"})
    .to_html(escape=False, index=False),
    unsafe_allow_html=True,
)

st.download_button(
    "Download filtered results",
    results.to_csv(index=False).encode(),
    "jpt_filtered.csv",
    "text/csv",
)
