from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from pf.config import CardConfig
from pf.db import now_iso
from pf.utils import compute_card_closing_date, compute_card_due_date, sha256_text


@dataclass(frozen=True)
class ManualEntry:
    txn_date: date
    amount: float
    description: str
    payment_method: str
    group_name: str | None = None
    category: str | None = None
    subcategory: str | None = None
    person: str | None = None
    reimbursable: bool = False
    reference: str | None = None
    notes: str | None = None


def build_manual_transaction_row(
    entry: ManualEntry,
    *,
    card: CardConfig | None = None,
    closing_day: int | None = None,
    due_day: int | None = None,
) -> dict[str, Any]:
    created = now_iso()
    description = (entry.description or "").strip() or "(sem descrição)"
    amount = float(entry.amount)

    statement_closing_date = None
    statement_due_date = None
    account = None
    source = "manual"
    cash_date = entry.txn_date

    if entry.payment_method == "credit_card":
        if card is None:
            raise ValueError("card é obrigatório quando payment_method='credit_card'")
        closing = int(closing_day) if closing_day is not None else int(card.closing_day)
        due = int(due_day) if due_day is not None else int(card.due_day)
        closing_date = compute_card_closing_date(entry.txn_date, closing_day=closing)
        due_date = compute_card_due_date(closing_date, closing_day=closing, due_day=due)
        statement_closing_date = closing_date.isoformat()
        statement_due_date = due_date.isoformat()
        cash_date = due_date
        account = card.name
        source = card.id

    row_hash = sha256_text(
        "|".join(
            [
                "manual",
                entry.txn_date.isoformat(),
                cash_date.isoformat(),
                f"{amount:.2f}",
                description,
                entry.payment_method,
                account or "",
                entry.reference or "",
            ]
        )
    )

    return {
        "row_hash": row_hash,
        "txn_date": entry.txn_date.isoformat(),
        "cash_date": cash_date.isoformat(),
        "amount": float(amount),
        "description": description,
        "group_name": entry.group_name,
        "category": entry.category,
        "subcategory": entry.subcategory,
        "payment_method": entry.payment_method,
        "account": account,
        "source": source,
        "statement_closing_date": statement_closing_date,
        "statement_due_date": statement_due_date,
        "person": entry.person,
        "reimbursable": 1 if entry.reimbursable else 0,
        "reference": entry.reference,
        "notes": entry.notes,
        "source_file": "manual_entry",
        "source_hash": "manual_entry",
        "external_id": None,
        "created_at": created,
        "updated_at": created,
    }

