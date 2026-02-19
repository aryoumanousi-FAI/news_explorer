# app.py — JPT + WorldOil News Explorer (CSV) with:
# - Last updated banner (file mtime + latest scraped_at)
# - Tag + Topic normalization (Title Case + acronym preservation)
# - Canonical tag mapping using all_tags.csv (case-insensitive; column: tag)
# - Country filter (derived from tags) + manual additions: US, UK, UAE
# - Cascading filters across Sources, Topics, Tags, Countries
# - Toggle OR/AND matching for Topics, Tags, Countries via UI
# - Clickable links IN the main table (HTML)
# - Pagination (25 rows/page)
# - Header bold + centered; Date single-line (no wrap)

from __future__ import annotations

import ast
import html
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Tuple

import pandas as pd
import streamlit as st


# -------------------
# Config
# -------------------
JPT_PATH = Path("jpt_scraper/data/jpt.csv")
WORLDOIL_PATH = Path("jpt_scraper/data/worldoil_full.csv")
ALL_TAGS_PATH = Path("all_tags.csv")  # must contain column: tag
PAGE_SIZE = 25


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

    # preserve intentional internal casing (e.g., "iPhone", "eBay", "McDermott")
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
        }
    return out


def extract_countries_from_tags(tags: List[str], country_set: Set[str]) -> Set[str]:
    out: Set[str] = set()
    for t in tags:
        t_norm = _normalize_text(t)
        if not t_norm:
            continue
        # map common abbreviations
        if t_norm in COUNTRY_ABBREV:
            out.add(COUNTRY_ABBREV[t_norm])
            continue
        # direct match to known country names
        if t_norm in country_set:
            out.add(t_norm)
            continue
        # sometimes tags include country names as part of longer phrases
        # do a conservative containment check
        for c in ("US", "UK", "UAE"):
            if t_norm.upper() == c:
                out.add(c)
    return out


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
    if df.empty:
        return None
    if "scraped_at" not in df.columns:
        return None
    s = pd.to_datetime(df["scraped_at"], errors="coerce")
    if s.dropna().empty:
        return None
    return s.max().to_pydatetime()


def pick_col(df: pd.DataFrame, candidates: List[str]) -> str | None:
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    return None


def make_link_html(url: str, text: str) -> str:
    u = html.escape(_normalize_text(url))
    t = html.escape(_normalize_text(text)) or u
    if not u:
        return html.escape(_normalize_text(text))
    return f'<a href="{u}" target="_blank" rel="noopener noreferrer">{t}</a>'


def normalize_tags_list(tags: List[str], canonical_map: Dict[str, str], acronyms: Set[str]) -> List[str]:
    out: List[str] = []
    for t in tags:
        raw = _normalize_text(t)
        if not raw:
            continue
        key = raw.lower()
        if key in canonical_map:
