# app.py — Oil & Gas News Explorer
# Sources:
# - jpt_scraper/data/jpt.csv (JPT merged output)
# - jpt_scraper/data/worldoil_full.csv (WorldOil canonical full)
#
# Features:
# - Card UI (big clickable title, excerpt, chips)
# - Filters: Sources, Topics, Tags, Countries, Search (title + excerpt)
# - AND/OR match mode for Topics/Tags/Countries
# - Pagination
# - “Last updated” banner (file mtime + latest scraped_at)
# - Tag/topic normalization (Title Case + acronym preservation)
# - Canonical tag mapping using all_tags.csv (case-insensitive; column: tag)
# - Country filter derived from tags + manual US/UK/UAE support

from __future__ import annotations

import ast
import html
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set

import pandas as pd
import streamlit as st


# -------------------
# Config
# -------------------
JPT_PATH = Path("jpt_scraper/data/jpt.csv")
WORLDOIL_PATH = Path("jpt_scraper/data/worldoil_full.csv")
ALL_TAGS_PATH = Path("all_tags.csv")  # must contain column: tag


# -------------------
# Normalization
# -------------------
WORD_SPLIT_RE = re.compile(r"(\s+|[-/])")  # keep separators

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
    "UK/UKCS": "UK",
    "U.K./UKCS": "UK",
    "Britain": "UK",
    "UAE": "UAE",
    "U.A.E.": "UAE",
    "United Arab Emirates": "UAE",
}


def _normalize_text(x) -> str:
    if x is None:
        return ""
    if isinstance(x, float) and pd.isna(x):
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
    if not token:
        return False
    t = token.strip()

    if t.upper() in acronyms:
        return True
    if re.fullmatch(r"[A-Za-z]{1,4}\d{1,3}", t):
        return True
    if re.fullmatch(r"[A-Z]{2,}", t):
        return True
    if re.search(r"[&.]", t) and re.search(r"[A-Za-z]", t):
        return True

    return False


def _smart_title_token(token: str, acronyms: Set[str]) -> str:
    raw = token.strip()
    if not raw:
        return token

    if raw.isspace() or raw in {"-", "/"}:
        return raw

    if _looks_like_acronym(raw, acronyms):
        up = raw.upper()
        return "CO2" if up == "CO₂" else up

    # preserve internal casing (e.g., iPhone / eBay / McDermott)
    if any(c.isupper() for c in raw[1:]) and any(c.islower() for c in raw):
        return raw

    return raw[:1].upper() + raw[1:].lower()


def normalize_phrase(s: str, acronyms: Set[str]) -> str:
    s = _normalize_text(s)
    if not s:
        return ""
    parts = WORD_SPLIT_RE.split(s)
    out = "".join(_smart_title_token(p, acronyms) for p in parts)
    out = re.sub(r"\s+", " ", out).strip()
    out = out.replace("Co2", "CO2").replace("Co₂", "CO2")
    return out


def pick_col(df: pd.DataFrame, candidates: List[str]) -> str | None:
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    return None


def load_master_tags(path: Path) -> List[str]:
    if not path.exists():
        return []
    df = pd.read_csv(path)
    if "tag" not in df.columns:
        return []
    return [_normalize_text(x) for x in df["tag"].tolist() if _normalize_text(x)]


def build_acronym_set(master_tags: List[str]) -> Set[str]:
    acronyms = set(BASE_ACRONYMS)
    for t in master_tags:
        t = _normalize_text(t)
        if not t:
            continue
        if re.fullmatch(r"[A-Z0-9&./-]{2,}", t) and re.search(r"[A-Z]", t):
            acronyms.add(t.upper())
        if re.search(r"[&.]", t) and re.search(r"[A-Za-z]", t):
            acronyms.add(t.upper())
    return acronyms


def build_canonical_tag_map(master_tags: List[str], acronyms: Set[str]) -> Dict[str, str]:
    m: Dict[str, str] = {}
    for t in master_tags:
        key = _normalize_text(t).lower()
        if not key:
            continue
        m[key] = normalize_phrase(t, acronyms)
    return m


# -------------------
# Countries
# -------------------
@st.cache_resource
def build_country_set_cached() -> Set[str]:
    out: Set[str] = {"US", "UK", "UAE"}
    try:
        import pycountry  # type: ignore

        for c in pycountry.countries:
            for name in [getattr(c, "name", None), getattr(c, "official_name", None), getattr(c, "common_name", None)]:
                if not name:
                    continue
                out.add(COUNTRY_ABBREV.get(name, name))
    except Exception:
        out |= {
            "Canada", "Mexico", "Brazil", "Argentina", "Norway", "Netherlands", "Germany",
            "France", "Italy", "Spain", "India", "China", "Japan", "Korea", "Australia",
            "Saudi Arabia", "Qatar", "Kuwait", "Iraq", "Iran", "Oman", "Egypt", "Nigeria",
            "Malaysia", "Greece",
        }
    return out


def extract_countries_from_tags(tags: List[str], country_set: Set[str]) -> List[str]:
    out: Set[str] = set()
    for t in tags:
        t_norm = _normalize_text(t)
        if not t_norm:
            continue

        if t_norm in COUNTRY_ABBREV:
            out.add(COUNTRY_ABBREV[t_norm])
            continue

        if t_norm in country_set:
            out.add(t_norm)
            continue

        up = t_norm.upper()
        if up in {"US", "UK", "UAE"}:
            out.add(up)

    return sorted(out)


# -------------------
# Data loading
# -------------------
@st.cache_data(show_spinner=False)
def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def safe_mtime(path: Path) -> datetime | None:
    if not path.exists():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime)


def latest_scraped_at(df: pd.DataFrame) -> datetime | None:
    if df.empty or "scraped_at" not in df.columns:
        return None
    s = pd.to_datetime(df["scraped_at"], errors="coerce").dropna()
    if s.empty:
        return None
    return s.max().to_pydatetime()


def compute_last_updated_banner(jpt_df: pd.DataFrame, wo_df: pd.DataFrame) -> str:
    mtimes = [safe_mtime(JPT_PATH), safe_mtime(WORLDOIL_PATH)]
    mtimes = [m for m in mtimes if m is not None]
    mtime_str = max(mtimes).strftime("%Y-%m-%d %H:%M:%S") if mtimes else "N/A"

    latest = [latest_scraped_at(jpt_df), latest_scraped_at(wo_df)]
    latest = [x for x in latest if x is not None]
    latest_str = max(latest).strftime("%Y-%m-%d %H:%M:%S") if latest else "N/A"

    return f"**Last updated:** file mtime = {mtime_str} | latest scraped_at = {latest_str}"


# -------------------
# Filter helpers
# -------------------
def match_list(values: List[str], selected: List[str], mode: str) -> bool:
    if not selected:
        return True
    s = set(values or [])
    if mode == "AND":
        return all(x in s for x in selected)
    return any(x in s for x in selected)


def truncate(s: str, n: int) -> str:
    s = _normalize_text(s)
    if not s:
        return ""
    return s if len(s) <= n else s[: n - 1].rstrip() + "…"


def chip_row(items: List[str]) -> str:
    if not items:
        return ""
    safe = [html.escape(_normalize_text(x)) for x in items if _normalize_text(x)]
    if not safe:
        return ""
    return "".join([f'<span class="chip">{x}</span>' for x in safe])


def fmt_date(x) -> str:
    try:
        dt = pd.to_datetime(x, errors="coerce")
        if pd.isna(dt):
            return ""
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""


def main() -> None:
    st.set_page_config(page_title="Oil & Gas News Explorer", layout="wide")
    st.title("Oil & Gas News Explorer")

    # Load sources
    jpt = load_csv(JPT_PATH)
    wo = load_csv(WORLDOIL_PATH)

    # Add source labels if missing
    if not jpt.empty and "source" not in jpt.columns:
        jpt["source"] = "JPT"
    if not wo.empty and "source" not in wo.columns:
        wo["source"] = "WorldOil"

    # Combine
    df = pd.concat([jpt, wo], ignore_index=True)

    st.markdown(compute_last_updated_banner(jpt, wo))

    if df.empty:
        st.info("No data found yet. Make sure the CSVs exist and the workflows have run.")
        return

    # Tag canonicalization
    master_tags = load_master_tags(ALL_TAGS_PATH)
    acronyms = build_acronym_set(master_tags)
    canonical_map = build_canonical_tag_map(master_tags, acronyms)

    country_set = build_country_set_cached()

    # Detect columns (schema-tolerant)
    col_title = pick_col(df, ["title", "headline"]) or "title"
    col_url = pick_col(df, ["url", "link", "article_url"]) or "url"
    col_source = pick_col(df, ["source"]) or "source"
    col_published = pick_col(df, ["published_date", "published", "date"])
    col_scraped = pick_col(df, ["scraped_at"])
    col_tags = pick_col(df, ["tags", "tag"])
    col_topics = pick_col(df, ["topics", "topic"])
    col_excerpt = pick_col(df, ["excerpt", "summary", "description", "deck", "teaser", "subtitle"])

    # Ensure required columns exist
    for c in [col_title, col_url, col_source]:
        if c not in df.columns:
            df[c] = ""

    if col_tags is None:
        df["tags"] = [[] for _ in range(len(df))]
        col_tags = "tags"
    if col_topics is None:
        df["topics"] = [[] for _ in range(len(df))]
        col_topics = "topics"

    # Parse list-like columns
    df[col_tags] = df[col_tags].apply(_parse_listish)
    df[col_topics] = df[col_topics].apply(_parse_listish)

    # Normalize tags/topics
    def normalize_tags_list(tags: List[str]) -> List[str]:
        out: List[str] = []
        for t in tags:
            raw = _normalize_text(t)
            if not raw:
                continue
            key = raw.lower()
            if key in canonical_map:
                out.append(canonical_map[key])
            else:
                out.append(normalize_phrase(raw, acronyms))
        seen = set()
        deduped = []
        for x in out:
            if x not in seen:
                seen.add(x)
                deduped.append(x)
        return deduped

    def normalize_topics_list(topics: List[str]) -> List[str]:
        out: List[str] = []
        for t in topics:
            raw = _normalize_text(t)
            if raw:
                out.append(normalize_phrase(raw, acronyms))
        seen = set()
        deduped = []
        for x in out:
            if x not in seen:
                seen.add(x)
                deduped.append(x)
        return deduped

    df["tags_norm"] = df[col_tags].apply(normalize_tags_list)
    df["topics_norm"] = df[col_topics].apply(normalize_topics_list)
    df["countries"] = df["tags_norm"].apply(lambda xs: extract_countries_from_tags(xs, country_set))

    df["source_norm"] = df[col_source].apply(lambda x: normalize_phrase(_normalize_text(x), acronyms) if _normalize_text(x) else "")
    df["title_norm"] = df[col_title].apply(lambda x: _normalize_text(x))

    df["published_dt"] = pd.to_datetime(df[col_published], errors="coerce") if col_published and col_published in df.columns else pd.NaT
    df["scraped_dt"] = pd.to_datetime(df[col_scraped], errors="coerce") if col_scraped and col_scraped in df.columns else pd.NaT

    # -------------------
    # Sidebar (order matters!)
    # -------------------
    sources_all = sorted([s for s in df["source_norm"].dropna().unique().tolist() if _normalize_text(s)])
    sel_sources = st.sidebar.multiselect("Sources", sources_all, default=sources_all)

    filtered = df[df["source_norm"].isin(sel_sources)].copy()

    topics_mode = st.sidebar.radio("Topics match mode", ["OR", "AND"], horizontal=True)
    topics_all = sorted({t for row in filtered["topics_norm"] for t in (row or [])})
    sel_topics = st.sidebar.multiselect("Topics", topics_all, default=[])

    tags_mode = st.sidebar.radio("Tags match mode", ["OR", "AND"], horizontal=True)
    tags_all = sorted({t for row in filtered["tags_norm"] for t in (row or [])})
    sel_tags = st.sidebar.multiselect("Tags", tags_all, default=[])

    countries_mode = st.sidebar.radio("Countries match mode", ["OR", "AND"], horizontal=True)
    countries_all = sorted({c for row in filtered["countries"] for c in (row or [])})
    sel_countries = st.sidebar.multiselect("Countries", countries_all, default=[])

    q = st.sidebar.text_input("Search", value="").strip()

    # Display (bottom)
    st.sidebar.markdown("---")
    st.sidebar.subheader("Display")
    page_size = st.sidebar.selectbox("Page size", [10, 25, 50, 100], index=3)
    excerpt_len = st.sidebar.slider("Excerpt length", 120, 600, 320, 20)
    show_excerpt = st.sidebar.toggle("Show excerpt", value=True)

    # -------------------
    # Apply filters
    # -------------------
    filtered = filtered[filtered["topics_norm"].apply(lambda xs: match_list(xs or [], sel_topics, topics_mode))]
    filtered = filtered[filtered["tags_norm"].apply(lambda xs: match_list(xs or [], sel_tags, tags_mode))]
    filtered = filtered[filtered["countries"].apply(lambda xs: match_list(xs or [], sel_countries, countries_mode))]

    # Search (title + excerpt)
    if q:
        pattern = re.escape(q)
        title_hit = filtered["title_norm"].fillna("").str.contains(pattern, case=False, na=False)

        if col_excerpt and col_excerpt in filtered.columns:
            excerpt_hit = filtered[col_excerpt].fillna("").astype(str).str.contains(pattern, case=False, na=False)
        else:
            excerpt_hit = pd.Series(False, index=filtered.index)

        filtered = filtered[title_hit | excerpt_hit]

    # Sort newest first
    sort_cols = []
    if "published_dt" in filtered.columns:
        sort_cols.append("published_dt")
    if "scraped_dt" in filtered.columns:
        sort_cols.append("scraped_dt")
    if sort_cols:
        filtered = filtered.sort_values(by=sort_cols, ascending=[False] * len(sort_cols), kind="mergesort")

    total = len(filtered)
    st.caption(f"Showing {total:,} results")

    max_page = max(1, (total + page_size - 1) // page_size)
    page = st.number_input("Page", min_value=1, max_value=max_page, value=1, step=1)

    start = (page - 1) * page_size
    end = start + page_size
    page_df = filtered.iloc[start:end].copy()

    # -------------------
    # Card UI styling
    # -------------------
    st.markdown(
        """
        <style>
          .news-wrap { max-width: 1150px; margin: 0 auto; }
          .news-card {
            border: 1px solid rgba(49, 51, 63, 0.2);
            border-radius: 14px;
            padding: 14px 16px;
            margin: 12px 0;
            background: rgba(255, 255, 255, 0.02);
          }
          .news-title {
            font-size: 22px;
            line-height: 1.25;
            font-weight: 750;
            margin: 0 0 6px 0;
          }
          .news-title a { text-decoration: none; }
          .news-title a:hover { text-decoration: underline; }
          .news-meta {
            font-size: 13px;
            opacity: 0.85;
            margin: 0 0 10px 0;
          }
          .news-excerpt {
            font-size: 15px;
            line-height: 1.55;
            margin: 0 0 10px 0;
            opacity: 0.95;
          }
          .chip {
            display: inline-block;
            font-size: 12px;
            padding: 3px 9px;
            border-radius: 999px;
            border: 1px solid rgba(49, 51, 63, 0.22);
            margin: 0 6px 6px 0;
            white-space: nowrap;
          }
          .chip-label {
            font-size: 12px;
            font-weight: 650;
            opacity: 0.8;
            margin: 4px 10px 6px 0;
            display: inline-block;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # -------------------
    # Render cards
    # -------------------
    st.markdown('<div class="news-wrap">', unsafe_allow_html=True)

    for _, r in page_df.iterrows():
        url = _normalize_text(r.get(col_url, ""))
        title = _normalize_text(r.get("title_norm", "")) or "Untitled"
        source = _normalize_text(r.get("source_norm", ""))

        published = fmt_date(r.get("published_dt", None))
        meta = " • ".join([p for p in [published, source] if p])

        excerpt = ""
        if show_excerpt and col_excerpt and col_excerpt in page_df.columns:
            excerpt = truncate(r.get(col_excerpt, ""), excerpt_len)

        topics = r.get("topics_norm", []) or []
        tags = r.get("tags_norm", []) or []
        countries = r.get("countries", []) or []

        title_html = html.escape(title)
        if url:
            url_html = html.escape(url)
            title_block = f'<div class="news-title"><a href="{url_html}" target="_blank" rel="noopener noreferrer">{title_html}</a></div>'
        else:
            title_block = f'<div class="news-title">{title_html}</div>'

        excerpt_html = f'<div class="news-excerpt">{html.escape(excerpt)}</div>' if excerpt else ""

        chips_html = ""
        tchips = chip_row(topics)
        if tchips:
            chips_html += f'<div><span class="chip-label">Topics</span>{tchips}</div>'
        xchips = chip_row(tags)
        if xchips:
            chips_html += f'<div><span class="chip-label">Tags</span>{xchips}</div>'
        cchips = chip_row(countries)
        if cchips:
            chips_html += f'<div><span class="chip-label">Countries</span>{cchips}</div>'

        st.markdown(
            f"""
            <div class="news-card">
              {title_block}
              <div class="news-meta">{html.escape(meta)}</div>
              {excerpt_html}
              {chips_html}
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("</div>", unsafe_allow_html=True)


if __name__ == "__main__":
    main()
