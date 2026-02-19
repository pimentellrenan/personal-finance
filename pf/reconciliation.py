"""
Módulo de Reconciliação Mensal (Acerto de Contas)

Regras:
1. Categorias "Gastos Renan" e "Gastos Aline" NÃO são divididas - são 100% do responsável
2. Contas da Casa (household) são divididas no mês CORRENTE (não do mês anterior)
3. Demais gastos são divididos 50/50
4. O campo "person" indica quem pagou
5. Reembolsáveis são excluídos do acerto

Obs:
- `amount` no banco segue a convenção do app (despesa < 0, crédito/estorno > 0).
  Para o acerto, usamos `valor = -amount`, então:
  - valor > 0 => gasto
  - valor < 0 => estorno/crédito (reduz o gasto)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

import pandas as pd

from pf.utils import normalize_str


@dataclass
class ReconciliationResult:
    """Resultado do cálculo de reconciliação"""

    # Período de referência
    reference_month: int
    reference_year: int

    # Totais gerais (net, já considera estornos/créditos)
    total_despesas: float = 0.0
    total_dividir: float = 0.0
    total_renan_individual: float = 0.0
    total_aline_individual: float = 0.0
    total_contas_casa: float = 0.0

    # Quem pagou o quê (net, já considera estornos/créditos)
    renan_pagou_dividir: float = 0.0
    aline_pagou_dividir: float = 0.0
    familia_pagou_dividir: float = 0.0

    renan_pagou_casa: float = 0.0
    aline_pagou_casa: float = 0.0

    # Sem categoria (apenas gastos, não estornos)
    sem_categoria: float = 0.0
    qtd_sem_categoria: int = 0

    # Por cartão (net, já considera estornos/créditos)
    por_cartao: dict[str, float] = field(default_factory=dict)

    # Totais (pago vs. deveria) - base para o saldo final
    renan_deveria_pagar: float = 0.0
    aline_deveria_pagar: float = 0.0
    renan_pagou_total: float = 0.0
    aline_pagou_total: float = 0.0

    # Saldo final
    aline_deve_renan: float = 0.0

    # Detalhes por transação
    detalhes: list[dict[str, Any]] = field(default_factory=list)

    def calcular_saldo_final(self) -> None:
        """
        Calcula quanto Aline deve a Renan (ou vice-versa).

        Base:
        - Cada transação gera quanto cada um *deveria pagar* (pela regra) e quanto *pagou*.
        - O saldo final é: (Renan pagou - Renan deveria pagar).
          * Positivo  -> Aline → Renan
          * Negativo  -> Renan → Aline
        """
        self.aline_deve_renan = float(self.renan_pagou_total - self.renan_deveria_pagar)


def calculate_reconciliation(
    df: pd.DataFrame,
    *,
    reference_month: int,
    reference_year: int,
    include_household: bool = True,
    df_household: pd.DataFrame | None = None,
) -> ReconciliationResult:
    """
    Calcula a reconciliação para o mês de referência.

    Args:
        df: DataFrame com transações (exceto household)
        reference_month: Mês de referência (1-12)
        reference_year: Ano de referência
        include_household: Se True, inclui contas da casa do mês corrente
        df_household: DataFrame com contas da casa (opcional, se não for filtrar de df)

    Returns:
        ReconciliationResult com todos os cálculos
    """
    result = ReconciliationResult(
        reference_month=int(reference_month),
        reference_year=int(reference_year),
    )

    gastos_renan_cat = normalize_str("Gastos Renan")
    gastos_aline_cat = normalize_str("Gastos Aline")

    def _to_float(v) -> float | None:
        try:
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
        except Exception:
            pass
        try:
            return float(v)
        except Exception:
            return None

    def _canon_person(v: Any) -> str:
        s = normalize_str(v)
        if not s:
            return "Renan"
        if s == "renan":
            return "Renan"
        if s == "aline":
            return "Aline"
        if s in ("familia", "família"):
            return "Família"
        return str(v).strip() or "Renan"

    def _iter_items(df_in: pd.DataFrame | None) -> list[dict[str, Any]]:
        if df_in is None or df_in.empty or "amount" not in df_in.columns:
            return []
        d = df_in.copy()

        # Excluir reembolsáveis (independente do sinal do amount)
        if "reimbursable" in d.columns:
            d = d[d["reimbursable"] != 1].copy()
        if d.empty:
            return []

        out: list[dict[str, Any]] = []
        for _, r in d.iterrows():
            amount = _to_float(r.get("amount"))
            if amount is None:
                continue

            # `valor`: positivo = gasto, negativo = estorno/crédito.
            valor = -float(amount)
            if abs(valor) < 1e-9:
                continue

            payment_method = str(r.get("payment_method") or "").strip() or "unknown"
            if payment_method == "income":
                continue

            category_raw = str(r.get("category") or "").strip()
            category_norm = normalize_str(category_raw)
            subcategory_raw = str(r.get("subcategory") or "").strip() or None
            person = _canon_person(r.get("person"))

            account = str(r.get("account") or "").strip() or None
            description = str(r.get("description") or "").strip() or "(sem descrição)"

            out.append(
                {
                    "row_hash": r.get("row_hash"),
                    "txn_date": r.get("txn_date"),
                    "cash_date": r.get("cash_date"),
                    "payment_method": payment_method,
                    "account": account,
                    "description": description,
                    "category": category_raw or None,
                    "category_norm": category_norm,
                    "subcategory": subcategory_raw,
                    "person": person,
                    "valor": float(valor),
                    "source_file": r.get("source_file"),
                }
            )
        return out

    items: list[dict[str, Any]] = []
    items.extend(_iter_items(df))
    if include_household:
        items.extend(_iter_items(df_household))

    if not items:
        return result

    for it in items:
        valor = float(it["valor"])
        payment_method = str(it.get("payment_method") or "unknown")
        category_norm = str(it.get("category_norm") or "")
        person = str(it.get("person") or "Renan")

        is_household = payment_method == "household"
        if is_household:
            regra = "Contas Casa (50/50)"
            renan_share = valor / 2
            aline_share = valor / 2
        elif category_norm == gastos_renan_cat:
            regra = "Gastos Renan (100%)"
            renan_share = valor
            aline_share = 0.0
        elif category_norm == gastos_aline_cat:
            regra = "Gastos Aline (100%)"
            renan_share = 0.0
            aline_share = valor
        else:
            regra = "Dividir (50/50)"
            renan_share = valor / 2
            aline_share = valor / 2

        # Quem pagou (para saldo): se "Família", considera metade para cada um.
        person_norm = normalize_str(person)
        paid_total = valor
        if person_norm in ("familia", "família"):
            renan_paid = paid_total / 2
            aline_paid = paid_total / 2
            familia_paid = paid_total
        elif person_norm == "aline":
            renan_paid = 0.0
            aline_paid = paid_total
            familia_paid = 0.0
        else:
            renan_paid = paid_total
            aline_paid = 0.0
            familia_paid = 0.0

        # Totais gerais (net, considera estornos)
        result.total_despesas += valor

        # Por tipo
        if is_household:
            result.total_contas_casa += valor
            if person_norm == "aline":
                result.aline_pagou_casa += paid_total
            elif person_norm == "renan":
                result.renan_pagou_casa += paid_total
        elif category_norm == gastos_renan_cat:
            result.total_renan_individual += valor
        elif category_norm == gastos_aline_cat:
            result.total_aline_individual += valor
        else:
            result.total_dividir += valor
            if person_norm == "aline":
                result.aline_pagou_dividir += paid_total
            elif person_norm in ("familia", "família"):
                result.familia_pagou_dividir += paid_total
            else:
                result.renan_pagou_dividir += paid_total

        # Sem categoria (só faz sentido para gasto, não para estorno)
        cat_txt = str(it.get("category") or "").strip()
        if valor > 0 and not cat_txt:
            result.sem_categoria += valor
            result.qtd_sem_categoria += 1

        # Por cartão (net, considera estornos)
        if payment_method == "credit_card":
            account = str(it.get("account") or "").strip() or "(sem cartão)"
            result.por_cartao[account] = float(result.por_cartao.get(account, 0.0) + valor)

        # Totais de "pago" vs. "deveria"
        result.renan_deveria_pagar += float(renan_share)
        result.aline_deveria_pagar += float(aline_share)
        result.renan_pagou_total += float(renan_paid)
        result.aline_pagou_total += float(aline_paid)

        result.detalhes.append(
            {
                "txn_date": it.get("txn_date"),
                "cash_date": it.get("cash_date"),
                "payment_method": payment_method,
                "account": it.get("account"),
                "description": it.get("description"),
                "category": it.get("category"),
                "subcategory": it.get("subcategory"),
                "person": person,
                "valor": float(valor),
                "regra": regra,
                "renan_deveria": float(renan_share),
                "aline_deveria": float(aline_share),
                "renan_pagou": float(renan_paid),
                "aline_pagou": float(aline_paid),
                "familia_pagou": float(familia_paid),
                "renan_delta": float(renan_paid - renan_share),
                "aline_delta": float(aline_paid - aline_share),
                "source_file": it.get("source_file"),
                "row_hash": it.get("row_hash"),
            }
        )

    result.calcular_saldo_final()
    return result


def get_household_transactions_for_month(
    conn,
    month: int,
    year: int,
) -> pd.DataFrame:
    """
    Busca transações de Contas da Casa para o mês corrente.
    """
    from calendar import monthrange

    from pf import queries as pf_queries

    start = date(year, month, 1)
    end = date(year, month, monthrange(year, month)[1])

    df = pf_queries.load_transactions_df(conn, start=start, end=end)

    if df.empty:
        return df

    # Filtrar apenas household
    return df[df["payment_method"] == "household"].copy()


def format_reconciliation_summary(result: ReconciliationResult) -> str:
    """Formata um resumo textual da reconciliação"""
    lines = [
        f"## Acerto de Contas - {result.reference_month:02d}/{result.reference_year}",
        "",
        f"**Total de Despesas (net):** R$ {result.total_despesas:,.2f}",
        "",
        "### Divisão (net)",
        f"- Gastos Dividíveis (50/50): R$ {result.total_dividir:,.2f}",
        f"- Contas da Casa (50/50): R$ {result.total_contas_casa:,.2f}",
        f"- Gastos Renan (100%): R$ {result.total_renan_individual:,.2f}",
        f"- Gastos Aline (100%): R$ {result.total_aline_individual:,.2f}",
        "",
        "### Quem Pagou (net)",
        f"- Renan pagou (dividíveis): R$ {result.renan_pagou_dividir:,.2f}",
        f"- Aline pagou (dividíveis): R$ {result.aline_pagou_dividir:,.2f}",
        f"- Conta Família (dividíveis): R$ {result.familia_pagou_dividir:,.2f}",
        "",
        f"- Renan pagou (casa): R$ {result.renan_pagou_casa:,.2f}",
        f"- Aline pagou (casa): R$ {result.aline_pagou_casa:,.2f}",
        "",
        "### Saldo Final",
    ]

    if result.aline_deve_renan > 0:
        lines.append(f"**Aline → Renan: R$ {result.aline_deve_renan:,.2f}**")
    elif result.aline_deve_renan < 0:
        lines.append(f"**Renan → Aline: R$ {abs(result.aline_deve_renan):,.2f}**")
    else:
        lines.append("**Saldo zerado!**")

    return "\n".join(lines)

