from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from pf.config import CardConfig
from pf.db import now_iso
from pf.importers.common import read_csv_flexible
from pf.utils import (
    ColumnMatch,
    clamp_day,
    compute_card_closing_date,
    compute_card_due_date,
    find_column,
    month_add,
    normalize_str,
    parse_brl_number,
    parse_date,
    sha256_text,
)


# ============================================================================
# Patterns para identificar transações que devem ser IGNORADAS
# São pagamentos da fatura (dinheiro que o usuário pagou para quitar a fatura)
# ============================================================================
PAYMENT_PATTERNS = (
    "pagamento fatura",
    "pagamento recebido",
    "pagamentos validos",
    "pagamentos válidos",
    "pag fatura",
    "pag. fatura",
    "pagto fatura",
    "pgto fatura",
    "pagamento via pix",
    "pagamento parcial",
)


def _is_payment_transaction(description: str) -> bool:
    """
    Detecta se uma transação é um pagamento da fatura (deve ser ignorada).
    
    Pagamentos são quando o usuário paga a fatura do cartão - não são 
    compras nem estornos, são apenas movimentações de dinheiro do usuário
    para o banco/operadora.
    """
    dnorm = normalize_str(description)
    return any(pattern in dnorm for pattern in PAYMENT_PATTERNS)


def _is_refund_or_credit(description: str) -> bool:
    """
    Detecta se uma transação é um estorno/crédito/reembolso.
    
    Estornos são devoluções de compras que reduzem o valor da fatura.
    Devem ser mantidos com valor NEGATIVO (reduzem o gasto).
    """
    dnorm = normalize_str(description)
    refund_keywords = (
        "estorno",
        "credito de",
        "crédito de",
        "ajuste a credito",
        "ajuste a crédito",
        "reembolso",
        "devolucao",
        "devolução",
        "refund",
        "cancelamento",
    )
    return any(keyword in dnorm for keyword in refund_keywords)


def _guess_card_id_from_path(path: Path) -> str | None:
    n = normalize_str(path.name)
    if "nubank" in n or "nu" in n:
        # Tenta distinguir Nubank Aline vs Nubank Renan pelo nome do arquivo
        if "aline" in n:
            return "nubank_aline"
        return "nubank"
    if "xp" in n:
        return "xp"
    if "c6" in n:
        return "c6"
    if "mercado" in n or "mp" in n:
        return "mercadopago"
    if "porto" in n:
        return "portobank"
    return None


def _extract_statement_due_date_from_path(path: Path) -> date | None:
    """
    Heuristic: if the filename contains an ISO date (YYYY-MM-DD), treat it as statement due date.

    Examples:
    - XP_Fatura2026-01-05.csv -> 2026-01-05
    - Nubank_2025-12-19.csv  -> 2025-12-19
    """
    m = re.search(r"(20\d{2}-\d{2}-\d{2})", path.name)
    if not m:
        return None
    return parse_date(m.group(1))


def extract_statement_due_date_from_path(path: Path) -> date | None:
    """Extract statement due date from filename when it contains an ISO date (YYYY-MM-DD)."""
    return _extract_statement_due_date_from_path(path)


def _compute_statement_closing_date_from_due(due: date, *, closing_day: int) -> date:
    """
    Calcula a data de fechamento a partir do vencimento + dia de fechamento do cartão.

    Regra prática: se o vencimento cai antes/do mesmo dia do fechamento, o fechamento é no mês anterior.
    """
    closing_day = int(closing_day)
    if due.day <= closing_day:
        prev = month_add(due.replace(day=1), -1)
        return clamp_day(prev.year, prev.month, closing_day)
    return clamp_day(due.year, due.month, closing_day)


def import_credit_card_csv(
    path: Path,
    *,
    card: CardConfig,
    source_hash: str,
    source_file: str | None = None,
    person: str | None = None,
) -> list[dict[str, Any]]:
    rr = read_csv_flexible(path)
    df = rr.df.copy()

    # Normalize columns for matching (we keep original names in a mapping).
    original_columns = list(df.columns)
    normalized_to_original = {normalize_str(c): c for c in original_columns}
    normalized_columns = list(normalized_to_original.keys())

    date_col = find_column(
        normalized_columns,
        ColumnMatch(
            name="data",
            candidates=(
                "data",
                "date",
                "data da compra",
                "data_compra",
                "data transacao",
                "data_transacao",
            ),
        ),
    )
    desc_col = find_column(
        normalized_columns,
        ColumnMatch(
            name="descricao",
            candidates=(
                "descricao",
                "descrição",
                "description",
                "title",
                "titulo",
                "título",
                "estabelecimento",
                "merchant",
            ),
        ),
    )
    amount_col = find_column(
        normalized_columns,
        ColumnMatch(name="valor", candidates=("valor", "amount", "preco", "preço", "valor (r$)", "valor_rs")),
    )
    # Prefer BRL amount column when multiple "valor" columns exist (ex.: C6 exports include US$ and R$).
    brl_amount_col = next(
        (col for col in normalized_columns if "valor" in col and "r$" in col),
        None,
    )
    if brl_amount_col:
        amount_col = brl_amount_col
    tipo_col = find_column(
        normalized_columns,
        ColumnMatch(name="tipo", candidates=("tipo", "type", "natureza", "entrada/saida", "entrada_saida")),
    )
    external_id_col = find_column(
        normalized_columns,
        ColumnMatch(name="id", candidates=("id", "identificador", "identifier", "transacao_id", "transaction_id")),
    )
    installment_col = find_column(
        normalized_columns,
        ColumnMatch(name="parcela", candidates=("parcela", "parcel", "installment", "parcelamento")),
    )
    holder_col = find_column(
        normalized_columns,
        ColumnMatch(name="portador", candidates=("portador", "titular", "holder", "pessoa", "pessoa/cartao")),
    )

    if date_col is None or desc_col is None or amount_col is None:
        raise ValueError(
            "CSV não reconhecido. Colunas esperadas: data, descrição, valor. "
            f"Encontradas: {original_columns}"
        )

    date_col = normalized_to_original[date_col]
    desc_col = normalized_to_original[desc_col]
    amount_col = normalized_to_original[amount_col]
    tipo_col = normalized_to_original.get(tipo_col) if tipo_col else None
    external_id_col = normalized_to_original.get(external_id_col) if external_id_col else None
    installment_col = normalized_to_original.get(installment_col) if installment_col else None
    holder_col = normalized_to_original.get(holder_col) if holder_col else None

    # Parse rows
    rows: list[dict[str, Any]] = []
    source_file = source_file or str(path)
    created = now_iso()

    # Card statement exports frequently encode purchases as positive and credits/payments as negative.
    # We normalize to: expenses < 0, credits/refunds > 0.
    sample_amounts = [parse_brl_number(x) for x in df[amount_col].head(200).tolist()]
    positives = sum(1 for x in sample_amounts if x is not None and x > 0)
    negatives = sum(1 for x in sample_amounts if x is not None and x < 0)
    file_expenses_are_positive = positives >= negatives

    statement_due_date = _extract_statement_due_date_from_path(path)
    statement_closing_date = (
        _compute_statement_closing_date_from_due(statement_due_date, closing_day=int(card.closing_day))
        if statement_due_date is not None
        else None
    )
    # Keep deterministic occurrence index per logical row key.
    # This avoids collapsing legitimate repeated purchases that share the same fields
    # (same merchant/date/amount) in the same statement file.
    hash_occurrence_counts: dict[str, int] = {}

    for _, r in df.iterrows():
        txn_dt: date | None = parse_date(r.get(date_col))
        if txn_dt is None:
            continue
        description = str(r.get(desc_col) or "").strip()
        if not description:
            description = "(sem descrição)"

        # =================================================================
        # FILTRO 1: Ignorar pagamentos de fatura
        # São pagamentos que o usuário fez para quitar a fatura anterior
        # Não são despesas nem receitas - são transferências internas
        # =================================================================
        if _is_payment_transaction(description):
            continue

        amount_file = parse_brl_number(r.get(amount_col))
        if amount_file is None:
            continue

        # =================================================================
        # LÓGICA DE SINAIS:
        # - Despesas (compras): valor NEGATIVO (dinheiro que sai)
        # - Estornos/Créditos: valor POSITIVO (dinheiro que volta)
        #
        # Arquivos de fatura geralmente têm:
        # - Compras como valores POSITIVOS
        # - Estornos/Créditos como valores NEGATIVOS
        #
        # Nossa convenção interna é o inverso, então invertemos.
        #
        # Estornos são tratados como "receitas" na fatura - reduzem o total.
        # Quando categorizados como "Gastos Renan", é estorno pessoal dele.
        # Quando categorizados como despesa compartilhada, é crédito dividido.
        # =================================================================
        
        is_refund = _is_refund_or_credit(description)
        
        # Detectar se o valor original do arquivo é negativo (indica estorno/crédito)
        file_value_is_negative = amount_file < 0

        # Try to detect credits/refunds when a "tipo" column exists.
        if tipo_col:
            tipo = normalize_str(r.get(tipo_col))
            if any(k in tipo for k in ("credito", "crédito", "estorno", "reembolso", "refund", "credit")):
                amount = abs(amount_file)  # Estorno = valor positivo
                is_refund = True
            elif any(k in tipo for k in ("debito", "débito", "compra", "purchase", "debit", "saida", "saída")):
                amount = -abs(amount_file)  # Compra = valor negativo
            else:
                # Sem tipo explícito: usar convenção do arquivo
                if file_expenses_are_positive:
                    amount = -amount_file  # Inverte: positivo->negativo (despesa)
                else:
                    amount = amount_file
        else:
            # Sem coluna tipo: usar convenção do arquivo
            if file_expenses_are_positive:
                amount = -amount_file  # Inverte: positivo->negativo (despesa)
            else:
                amount = amount_file

        # Detecção por descrição: se é um estorno/crédito, garantir valor positivo
        # Isso também considera valores que já vieram negativos do arquivo
        if is_refund or file_value_is_negative:
            # Se a descrição indica estorno OU o valor veio negativo do arquivo
            # -> é um crédito/estorno -> deve ser positivo
            amount = abs(amount_file)
            is_refund = True

        # Usar sempre as datas extraídas do nome do arquivo
        if statement_due_date is not None and statement_closing_date is not None:
            closing_date = statement_closing_date
            due_date = statement_due_date
        else:
            # Fallback: usar a data da transação + config do cartão
            closing_date = compute_card_closing_date(txn_dt, closing_day=int(card.closing_day))
            due_date = compute_card_due_date(closing_date, closing_day=int(card.closing_day), due_day=int(card.due_day))

        external_id = None
        if external_id_col:
            external_id = str(r.get(external_id_col) or "").strip() or None

        holder = None
        if person is not None:
            holder = person
        elif holder_col:
            holder = str(r.get(holder_col) or "").strip() or None

        installment = None
        if installment_col:
            raw_installment = str(r.get(installment_col) or "").strip()
            if raw_installment and raw_installment != "-":
                installment = raw_installment

        # Build notes: combine installment info and refund indicator
        notes_parts = []
        if is_refund:
            notes_parts.append("💰 ESTORNO/CRÉDITO")
        if installment:
            notes_parts.append(f"Parcela: {installment}")
        notes = " | ".join(notes_parts) if notes_parts else None

        row_identity = "|".join(
            [
                card.id,
                txn_dt.isoformat(),
                f"{amount:.2f}",
                description,
                holder or "",
                installment or "",
                external_id or "",
            ]
        )
        occurrence = hash_occurrence_counts.get(row_identity, 0) + 1
        hash_occurrence_counts[row_identity] = occurrence

        row_hash = sha256_text(
            "|".join(
                [
                    "credit_card",
                    row_identity,
                    f"occurrence:{occurrence}",
                ]
            )
        )

        stable_key = sha256_text(
            "|".join(
                [
                    "credit_card_stable",
                    card.id,
                    txn_dt.isoformat(),
                    f"{amount:.2f}",
                    holder or "",
                    external_id or "",
                ]
            )
        )

        rows.append(
            {
                "row_hash": row_hash,
                "_stable_key": stable_key,
                "_legacy_amount_file": float(amount_file),
                "txn_date": txn_dt.isoformat(),
                "cash_date": due_date.isoformat(),
                "amount": float(amount),
                "description": description,
                "group_name": None,
                "category": None,
                "subcategory": None,
                "payment_method": "credit_card",
                "account": card.name,
                "source": card.id,
                "statement_closing_date": closing_date.isoformat(),
                "statement_due_date": due_date.isoformat(),
                "person": holder,
                "reimbursable": 0,
                "reference": None,
                "notes": notes,
                "source_file": source_file,
                "source_hash": source_hash,
                "external_id": external_id,
                "created_at": created,
                "updated_at": created,
            }
        )

    # Mark which stable keys are unique within the file.
    # This helps dedupe statement updates where only the description changes (e.g., "PENDING" -> final),
    # while avoiding accidental merges when a file legitimately has multiple rows with the same (date, amount, person, etc.).
    stable_counts: dict[str, int] = {}
    for row in rows:
        k = str(row.get("_stable_key") or "")
        if not k:
            continue
        stable_counts[k] = stable_counts.get(k, 0) + 1
    for row in rows:
        k = str(row.get("_stable_key") or "")
        row["_stable_key_unique_in_file"] = bool(k) and stable_counts.get(k, 0) == 1

    return rows


def guess_card_id(path: Path) -> str | None:
    return _guess_card_id_from_path(path)
