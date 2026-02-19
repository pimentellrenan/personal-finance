from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from pf.utils import normalize_str


@dataclass(frozen=True)
class ReadResult:
    df: pd.DataFrame
    metadata: dict[str, Any]


def read_csv_flexible(path: Path) -> ReadResult:
    encodings = ["utf-8-sig", "utf-8", "latin-1"]
    seps: Iterable[str | None] = [";", ",", "\t", None]
    last_err: Exception | None = None
    for encoding in encodings:
        for sep in seps:
            try:
                df = pd.read_csv(path, encoding=encoding, sep=sep, engine="python")
                if df.shape[1] <= 1:
                    continue
                return ReadResult(df=df, metadata={"encoding": encoding, "sep": sep})
            except Exception as e:  # noqa: BLE001
                last_err = e
                continue
    raise RuntimeError(f"Falha ao ler CSV: {path}") from last_err


def _require_openpyxl() -> None:
    try:
        import openpyxl  # noqa: F401
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(
            "Dependência ausente: openpyxl. Instale com `pip install -r requirements.txt`."
        ) from e


def read_excel_first_sheet(path: Path) -> ReadResult:
    _require_openpyxl()
    df = pd.read_excel(path, sheet_name=0)
    return ReadResult(df=df, metadata={"sheet": 0})


def coerce_str_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == object:
            continue
        # Keep numeric/date as-is, but ensure missing values are NaN.
        # (We avoid blindly stringifying everything.)
    return out


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [normalize_str(c) for c in out.columns]
    return out
