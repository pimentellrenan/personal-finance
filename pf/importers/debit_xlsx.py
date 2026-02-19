from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from pf.db import now_iso
from pf.importers.common import read_excel_first_sheet
from pf.utils import (
    ColumnMatch,
    find_column,
    normalize_str,
    month_add,
    parse_brl_number,
    parse_date,
    sha256_text,
)


def import_debit_xlsx(
    path: Path,
    *,
    source_hash: str,
    source_file: str | None = None,
) -> list[dict[str, Any]]:
    rr = read_excel_first_sheet(path)
    df = rr.df.copy()

    original_columns = list(df.columns)
    normalized_to_original = {normalize_str(c): c for c in original_columns}
    normalized_columns = list(normalized_to_original.keys())

    date_col = find_column(
        normalized_columns,
        ColumnMatch(name="data", candidates=("data da compra", "data do vencimento", "data", "date")),
    )
    desc_col = find_column(
        normalized_columns, ColumnMatch(name="descricao", candidates=("descricao", "descrição", "description", "historico", "histórico"))
    )
    amount_col = find_column(
        normalized_columns,
        ColumnMatch(name="valor", candidates=("valor", "valor (r$)", "valor (em r$)", "amount")),
    )
    category_col = find_column(normalized_columns, ColumnMatch(name="categoria", candidates=("categoria", "category")))
    subcategory_col = find_column(normalized_columns, ColumnMatch(name="subcategoria", candidates=("subcategoria", "subcategoria", "sub-category", "sub_category")))
    reimb_col = find_column(normalized_columns, ColumnMatch(name="reembolsavel", candidates=("reembolsavel", "reembolsável", "reimbursable")))
    ref_col = find_column(normalized_columns, ColumnMatch(name="referencia", candidates=("referencia", "referência", "reference")))
    notes_col = find_column(normalized_columns, ColumnMatch(name="observacoes", candidates=("observacoes", "observações", "notes")))
    person_col = find_column(normalized_columns, ColumnMatch(name="pessoa", candidates=("pessoa", "person", "quem pagou", "pago por")))

    if date_col is None or desc_col is None or amount_col is None:
        raise ValueError(
            "XLSX não reconhecido. Colunas esperadas: Data, Descrição, Valor. "
            f"Encontradas: {original_columns}"
        )

    def get_col(col: str | None) -> str | None:
        return normalized_to_original.get(col) if col else None

    date_col_o = get_col(date_col)
    desc_col_o = get_col(desc_col)
    amount_col_o = get_col(amount_col)
    category_col_o = get_col(category_col)
    subcategory_col_o = get_col(subcategory_col)
    reimb_col_o = get_col(reimb_col)
    ref_col_o = get_col(ref_col)
    notes_col_o = get_col(notes_col)
    person_col_o = get_col(person_col)

    rows: list[dict[str, Any]] = []
    source_file = source_file or str(path)
    created = now_iso()

    for _, r in df.iterrows():
        txn_dt = parse_date(r.get(date_col_o))
        if txn_dt is None:
            continue
        description = str(r.get(desc_col_o) or "").strip()
        if not description:
            description = "(sem descrição)"
        amount_raw = parse_brl_number(r.get(amount_col_o))
        if amount_raw is None:
            continue
        # In templates, users usually write positive amounts for expenses.
        amount = -abs(float(amount_raw))

        payment_method = "debit"

        category = str(r.get(category_col_o) or "").strip() or None if category_col_o else None
        subcategory = str(r.get(subcategory_col_o) or "").strip() or None if subcategory_col_o else None
        reference = str(r.get(ref_col_o) or "").strip() or None if ref_col_o else None
        notes = str(r.get(notes_col_o) or "").strip() or None if notes_col_o else None
        person = str(r.get(person_col_o) or "").strip() or None if person_col_o else None

        reimbursable = 0
        if reimb_col_o:
            rv = normalize_str(r.get(reimb_col_o))
            if rv in ("sim", "s", "yes", "y", "1", "true"):
                reimbursable = 1

        row_hash = sha256_text(
            "|".join(
                [
                    "debit",
                    txn_dt.isoformat(),
                    f"{amount:.2f}",
                    description,
                    payment_method,
                    reference or "",
                ]
            )
        )

        rows.append(
            {
                "row_hash": row_hash,
                "txn_date": txn_dt.isoformat(),
                "cash_date": month_add(txn_dt, 1).isoformat(),
                "amount": float(amount),
                "description": description,
                "group_name": None,
                "category": category,
                "subcategory": subcategory,
                "payment_method": payment_method,
                "account": None,
                "source": "manual_debit_xlsx",
                "statement_closing_date": None,
                "statement_due_date": None,
                "person": person,
                "reimbursable": reimbursable,
                "reference": reference,
                "notes": notes,
                "source_file": source_file,
                "source_hash": source_hash,
                "external_id": None,
                "created_at": created,
                "updated_at": created,
            }
        )

    return rows
