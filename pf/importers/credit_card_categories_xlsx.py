from __future__ import annotations

import hashlib
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from pf.importers.common import read_excel_first_sheet
from pf.utils import ColumnMatch, find_column, normalize_str


def _parse_sim_nao(v) -> int | None:
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    s = normalize_str(v)
    if s in ("sim", "s", "yes", "y", "true", "1"):
        return 1
    if s in ("nao", "não", "n", "no", "false", "0"):
        return 0
    try:
        return 1 if int(v) == 1 else 0
    except Exception:  # noqa: BLE001
        return None


def _clean_hash(v) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    s = str(v).strip()
    return "" if not s or s.lower() in ("nan", "none") else s


def _parse_date(v) -> str | None:
    """Convert various date formats to YYYY-MM-DD string."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    if isinstance(v, (date, datetime)):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, pd.Timestamp):
        return v.strftime("%Y-%m-%d")
    s = str(v).strip()
    if not s or s.lower() in ("nan", "none", "nat"):
        return None
    # Try common formats
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _parse_amount(v) -> float | None:
    """Parse amount, handling Brazilian number formats."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s or s.lower() in ("nan", "none"):
        return None
    # Handle Brazilian format: 1.234,56 -> 1234.56
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def _generate_row_hash(txn_date: str, amount: float, description: str, account: str) -> str:
    """Generate a unique row_hash for manual entries."""
    key = f"credit_card|manual|{txn_date}|{amount:.2f}|{description}|{account}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


def read_credit_card_categories_xlsx(path: Path) -> list[dict[str, Any]]:
    """
    Reads the consolidated `templates/cartao_credito.xlsx` and extracts only
    user-editable fields to apply back into the DB (category/subcategory/etc).
    """
    rr = read_excel_first_sheet(path)
    df = rr.df.copy()
    if df.empty:
        return []

    original_columns = list(df.columns)
    normalized_to_original = {normalize_str(c): c for c in original_columns}
    normalized_columns = list(normalized_to_original.keys())

    hash_col = find_column(
        normalized_columns,
        ColumnMatch(name="hash", candidates=("hash", "row_hash", "hash (oculto)", "__hash", "__row_hash")),
    )
    cat_col = find_column(normalized_columns, ColumnMatch(name="categoria", candidates=("categoria", "category")))
    sub_col = find_column(
        normalized_columns, ColumnMatch(name="subcategoria", candidates=("subcategoria", "sub-category", "sub_category"))
    )
    reimb_col = find_column(
        normalized_columns,
        ColumnMatch(name="reembolsavel", candidates=("reembolsavel", "reembolsável", "reimbursable")),
    )
    person_col = find_column(normalized_columns, ColumnMatch(name="portador", candidates=("portador", "pessoa", "person")))

    if hash_col is None:
        raise ValueError(
            "XLSX de cartão não reconhecido. Coluna esperada: `Hash (oculto)` (ou `row_hash`). "
            f"Encontradas: {original_columns}"
        )

    def _col(col: str | None) -> str | None:
        return normalized_to_original.get(col) if col else None

    hash_o = _col(hash_col)
    cat_o = _col(cat_col)
    sub_o = _col(sub_col)
    reimb_o = _col(reimb_col)
    person_o = _col(person_col)

    out: list[dict[str, Any]] = []
    for _, r in df.iterrows():
        rh = _clean_hash(r.get(hash_o))
        if not rh:
            continue
        category = str(r.get(cat_o) or "").strip() if cat_o else None
        subcategory = str(r.get(sub_o) or "").strip() if sub_o else None
        person = str(r.get(person_o) or "").strip() if person_o else None
        reimb = _parse_sim_nao(r.get(reimb_o)) if reimb_o else None

        out.append(
            {
                "row_hash": rh,
                "category": category if category != "" else None,
                "subcategory": subcategory if subcategory != "" else None,
                "person": person if person != "" else None,
                "reimbursable": reimb,
            }
        )

    return out


def read_credit_card_full_xlsx(path: Path) -> list[dict[str, Any]]:
    """
    Reads the consolidated `templates/cartao_credito.xlsx` and extracts ALL fields,
    including data needed to create new transactions (date, amount, description, etc.).
    
    This allows the Excel to be the "master" - both existing rows (with row_hash)
    and new manual entries (without row_hash, which will be generated).
    """
    rr = read_excel_first_sheet(path)
    df = rr.df.copy()
    if df.empty:
        return []

    original_columns = list(df.columns)
    normalized_to_original = {normalize_str(c): c for c in original_columns}
    normalized_columns = list(normalized_to_original.keys())

    # Find all columns
    hash_col = find_column(
        normalized_columns,
        ColumnMatch(name="hash", candidates=("hash", "row_hash", "hash (oculto)", "__hash", "__row_hash")),
    )
    date_col = find_column(
        normalized_columns,
        ColumnMatch(name="data", candidates=("data da compra", "data", "txn_date", "date")),
    )
    due_date_col = find_column(
        normalized_columns,
        ColumnMatch(name="vencimento", candidates=("data do vencimento", "vencimento", "statement_due_date", "due_date")),
    )
    cat_col = find_column(
        normalized_columns,
        ColumnMatch(name="categoria", candidates=("categoria", "category")),
    )
    sub_col = find_column(
        normalized_columns,
        ColumnMatch(name="subcategoria", candidates=("subcategoria", "sub-category", "sub_category")),
    )
    account_col = find_column(
        normalized_columns,
        ColumnMatch(name="cartao", candidates=("cartao de credito", "cartão de crédito", "account", "card")),
    )
    desc_col = find_column(
        normalized_columns,
        ColumnMatch(name="descricao", candidates=("descricao", "descrição", "description")),
    )
    amount_col = find_column(
        normalized_columns,
        ColumnMatch(name="valor", candidates=("valor (r$)", "valor", "amount")),
    )
    reimb_col = find_column(
        normalized_columns,
        ColumnMatch(name="reembolsavel", candidates=("reembolsavel", "reembolsável", "reimbursable")),
    )
    person_col = find_column(
        normalized_columns,
        ColumnMatch(name="portador", candidates=("portador", "pessoa", "person")),
    )

    def _col(col: str | None) -> str | None:
        return normalized_to_original.get(col) if col else None

    hash_o = _col(hash_col)
    date_o = _col(date_col)
    due_date_o = _col(due_date_col)
    cat_o = _col(cat_col)
    sub_o = _col(sub_col)
    account_o = _col(account_col)
    desc_o = _col(desc_col)
    amount_o = _col(amount_col)
    reimb_o = _col(reimb_col)
    person_o = _col(person_col)

    out: list[dict[str, Any]] = []
    for _, r in df.iterrows():
        # Get hash - may be empty for new manual entries
        rh = _clean_hash(r.get(hash_o)) if hash_o else ""
        
        # Get all fields
        txn_date = _parse_date(r.get(date_o)) if date_o else None
        due_date = _parse_date(r.get(due_date_o)) if due_date_o else None
        category = str(r.get(cat_o) or "").strip() if cat_o else None
        subcategory = str(r.get(sub_o) or "").strip() if sub_o else None
        account = str(r.get(account_o) or "").strip() if account_o else None
        description = str(r.get(desc_o) or "").strip() if desc_o else None
        amount = _parse_amount(r.get(amount_o)) if amount_o else None
        person = str(r.get(person_o) or "").strip() if person_o else None
        reimb = _parse_sim_nao(r.get(reimb_o)) if reimb_o else None
        
        # Skip completely empty rows
        if not txn_date and not description and amount is None:
            continue
        
        # Generate hash for new manual entries
        if not rh and txn_date and amount is not None and description:
            rh = _generate_row_hash(txn_date, amount, description, account or "")
        
        # If still no hash, skip (can't track this row)
        if not rh:
            continue
        
        # Normalize amount to negative for expenses
        if amount is not None and amount > 0:
            amount = -amount

        out.append(
            {
                "row_hash": rh,
                "txn_date": txn_date,
                "statement_due_date": due_date,
                "category": category if category else None,
                "subcategory": subcategory if subcategory else None,
                "account": account if account else None,
                "description": description if description else None,
                "amount": amount,
                "person": person if person else None,
                "reimbursable": reimb,
                "is_new": not bool(_clean_hash(r.get(hash_o))) if hash_o else True,
            }
        )

    return out
