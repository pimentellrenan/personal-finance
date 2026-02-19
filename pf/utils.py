from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any


def normalize_str(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"\s+", " ", text)
    return text


def parse_brl_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("R$", "").replace(" ", "")
    # If it contains comma as decimal separator, remove thousand separators.
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    elif "," in text and "." not in text:
        text = text.replace(",", ".")
    # Keep only numeric/sign/dot.
    text = re.sub(r"[^0-9+\-\.]", "", text)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    # Fallback: try ISO-like with time
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        return None


def month_add(d: date, months: int) -> date:
    year = d.year + (d.month - 1 + months) // 12
    month = (d.month - 1 + months) % 12 + 1
    day = min(d.day, _last_day_of_month(year, month))
    return date(year, month, day)


def _last_day_of_month(year: int, month: int) -> int:
    next_month = date(year, month, 28) + timedelta(days=4)
    last = next_month.replace(day=1) - timedelta(days=1)
    return last.day


def clamp_day(year: int, month: int, day: int) -> date:
    return date(year, month, min(day, _last_day_of_month(year, month)))


def compute_card_closing_date(txn_date: date, *, closing_day: int) -> date:
    if txn_date.day <= closing_day:
        return clamp_day(txn_date.year, txn_date.month, closing_day)
    return clamp_day(month_add(txn_date.replace(day=1), 1).year, month_add(txn_date.replace(day=1), 1).month, closing_day)


def compute_card_due_date(closing_date: date, *, closing_day: int, due_day: int) -> date:
    if due_day <= closing_day:
        nxt = month_add(closing_date.replace(day=1), 1)
        return clamp_day(nxt.year, nxt.month, due_day)
    return clamp_day(closing_date.year, closing_date.month, due_day)


def last_business_day(year: int, month: int) -> date:
    d = clamp_day(year, month, _last_day_of_month(year, month))
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ColumnMatch:
    name: str
    candidates: tuple[str, ...]


def find_column(columns: list[str], match: ColumnMatch) -> str | None:
    normalized = {normalize_str(c): c for c in columns}
    for cand in match.candidates:
        key = normalize_str(cand)
        if key in normalized:
            return normalized[key]
    # partial match
    for cand in match.candidates:
        key = normalize_str(cand)
        for nkey, original in normalized.items():
            if key and key in nkey:
                return original
    return None
