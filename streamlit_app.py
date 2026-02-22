from __future__ import annotations

import html
import os
import re
import shutil
import time
from calendar import monthrange
from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st
import altair as alt
import subprocess
import platform

from pf import config as pf_config
from pf import db as pf_db
from pf import excel_master as pf_excel_master
from pf import excel_sync as pf_excel_sync
from pf import excel_unified as pf_excel_unified
from pf import ingest as pf_ingest
from pf import manual as pf_manual
from pf import queries as pf_queries
from pf import rules_engine as pf_rules_engine
from pf import templates as pf_templates
from pf import reconciliation as pf_recon
from pf.importers.credit_card_csv import guess_card_id as guess_card_id_csv
from pf.utils import clamp_day, last_business_day, month_add, normalize_str, sha256_text

import zipfile
from datetime import datetime
from typing import Any


APP_TITLE = "Finanças Pessoais"
# Oculta toda a UI de "Acerto Mensal" (mantém backend).
SHOW_ACERTO_UI = os.getenv("PF_SHOW_ACERTO_UI", "0").strip() == "1"


def _backup_database(db_path: Path, backup_dir: Path) -> None:
    """Cria backup zipado do banco de dados na pasta raw_data, substituindo o anterior."""
    if not db_path.exists():
        return
    
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_file = backup_dir / "backup_db.zip"
    
    try:
        with zipfile.ZipFile(backup_file, "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(db_path, arcname=db_path.name)
    except Exception as e:
        print(f"Erro ao criar backup: {e}")


def _backup_unified_excel(path: Path, backup_dir: Path, *, keep_last: int = 30) -> Path | None:
    """Cria backup versionado do financas.xlsx e mantém só os mais recentes."""
    if not path.exists():
        return None
    backup_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    dest = backup_dir / f"{path.stem}_{ts}{path.suffix}"
    try:
        shutil.copy2(path, dest)
    except Exception:
        return None

    try:
        backups = sorted(
            [p for p in backup_dir.glob(f"{path.stem}_*{path.suffix}") if p.is_file()],
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        for old in backups[keep_last:]:
            try:
                old.unlink()
            except Exception:
                pass
    except Exception:
        pass
    return dest


def _open_file(path: Path) -> None:
    """Open a file with the default system application."""
    try:
        if platform.system() == "Darwin":  # macOS
            subprocess.run(["open", str(path)], check=False)
        elif platform.system() == "Windows":
            subprocess.run(["start", "", str(path)], shell=True, check=False)
        else:  # Linux
            subprocess.run(["xdg-open", str(path)], check=False)
    except Exception:
        pass  # Silently fail if can't open


def _is_running_in_docker() -> bool:
    if os.getenv("PF_RUNNING_IN_DOCKER", "").strip() == "1":
        return True
    return Path("/.dockerenv").exists()


def _to_date(v) -> date | None:
    if v is None:
        return None
    if isinstance(v, str):
        try:
            return date.fromisoformat(v[:10])
        except Exception:  # noqa: BLE001
            return None
    try:
        if pd.isna(v):
            return None
    except Exception:  # noqa: BLE001
        pass
    try:
        if isinstance(v, datetime):
            return v.date()
    except Exception:  # noqa: BLE001
        return None
    if isinstance(v, date):
        return v
    return None


def _default_statement_closing_date(due_dt: date, *, closing_day: int) -> date:
    """
    Default de fechamento a partir do vencimento + dia de fechamento do cartão.
    Regra prática: se o vencimento cai antes/do mesmo dia do fechamento, o fechamento é no mês anterior.
    """
    closing_day = int(closing_day)
    if due_dt.day <= closing_day:
        prev = month_add(due_dt.replace(day=1), -1)
        return clamp_day(prev.year, prev.month, closing_day)
    return clamp_day(due_dt.year, due_dt.month, closing_day)


# CSS customizado para melhorar o visual
CUSTOM_CSS = """
<style>
    /* Layout base para notebook 15" */
    [data-testid="stMainBlockContainer"] {
        max-width: 1440px !important;
        padding-top: 0.8rem !important;
        padding-left: 1rem !important;
        padding-right: 1rem !important;
        padding-bottom: 1rem !important;
    }
    [data-testid="stSidebar"] {
        min-width: 300px !important;
        max-width: 300px !important;
    }
    html, body, [data-testid="stAppViewContainer"] {
        font-size: 14px !important;
    }
    h1 { font-size: 1.45rem !important; }
    h2 { font-size: 1.2rem !important; }
    h3 { font-size: 1.05rem !important; }
    .stMetric label, .stCaption, .stMarkdown p, .stTextInput label, .stSelectbox label {
        font-size: 0.9rem !important;
    }

    /* ============================================
       BOTÕES INVISÍVEIS PARA CARDS CLICÁVEIS
       ============================================ */
    /* Container de card clicável - posição relativa para overlay */
    div[data-testid="stVerticalBlockBorderWrapper"]:has(.cc-card-wrapper),
    div[data-testid="stVerticalBlockBorderWrapper"]:has(.budget-card-wrapper) {
        position: relative !important;
    }
    
    /* Esconder o botão visualmente mas manter clicável - versão ultra-agressiva */
    div[data-testid="stVerticalBlockBorderWrapper"]:has(.cc-card-wrapper) > div > div > div:last-child,
    div[data-testid="stVerticalBlockBorderWrapper"]:has(.budget-card-wrapper) > div > div > div:last-child {
        position: absolute !important;
        top: 0 !important;
        left: 0 !important;
        right: 0 !important;
        bottom: 0 !important;
        z-index: 50 !important;
        margin: 0 !important;
        padding: 0 !important;
    }
    
    div[data-testid="stVerticalBlockBorderWrapper"]:has(.cc-card-wrapper) button,
    div[data-testid="stVerticalBlockBorderWrapper"]:has(.budget-card-wrapper) button {
        opacity: 0 !important;
        width: 100% !important;
        height: 100% !important;
        position: absolute !important;
        top: 0 !important;
        left: 0 !important;
        cursor: pointer !important;
        border: none !important;
        background: transparent !important;
        padding: 0 !important;
        margin: 0 !important;
    }

    /* ============================================
       STATUS DAS FATURAS: CARDS
       ============================================ */
    .stmt-card-wrapper {
        min-height: 190px;
        padding: 0.25rem 0.1rem;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        gap: 0.75rem;
    }
    .stmt-card-title {
        font-size: 1.1rem;
        font-weight: 700;
        margin: 0;
        line-height: 1.15;
    }
    .stmt-card-sub {
        font-size: 0.92rem;
        opacity: 0.85;
        margin: 0;
        line-height: 1.15;
    }
    .stmt-card-amount {
        font-size: 1.85rem;
        font-weight: 800;
        margin: 0;
        line-height: 1.15;
    }
    .stmt-card-badges {
        display: flex;
        gap: 0.4rem;
        flex-wrap: wrap;
        align-items: center;
    }
    .stmt-card-badges span {
        font-size: 0.82rem !important;
        padding: 0.2rem 0.55rem !important;
    }

    /* Cards de métricas */
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    div[data-testid="stMetric"] label {
        color: rgba(255, 255, 255, 0.9) !important;
    }
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: white !important;
    }
    
    /* Cartões de status */
    .status-card {
        padding: 1.5rem;
        border-radius: 12px;
        margin-bottom: 1rem;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
    }
    .status-ok { background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); color: white; }
    .status-warning { background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); color: white; }
    .status-neutral { background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); color: white; }
    
    /* Progress bars customizados */
    .budget-progress {
        height: 8px;
        border-radius: 4px;
        background: rgba(255, 255, 255, 0.3);
        margin-top: 0.5rem;
    }
    .budget-progress-fill {
        height: 100%;
        border-radius: 4px;
        transition: width 0.3s ease;
    }
    .progress-ok { background: #38ef7d; }
    .progress-warning { background: #f5a623; }
    .progress-danger { background: #f5576c; }

    /* Card de orçamento clicável */
    .budget-card-wrapper {
        position: relative;
        margin-bottom: 0.5rem;
    }
    .budget-card {
        border: 1px solid rgba(255, 255, 255, 0.14);
        border-radius: 16px;
        padding: 1rem 1.1rem;
        background: linear-gradient(135deg, rgba(255, 255, 255, 0.08) 0%, rgba(255, 255, 255, 0.03) 100%);
        cursor: pointer;
        transition: all 0.25s ease;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
    }
    .budget-card:hover {
        transform: translateY(-3px);
        box-shadow: 0 6px 20px rgba(0, 0, 0, 0.15);
        border-color: rgba(255, 255, 255, 0.25);
        background: linear-gradient(135deg, rgba(255, 255, 255, 0.12) 0%, rgba(255, 255, 255, 0.06) 100%);
    }
    .budget-card-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 0.5rem;
    }
    .budget-card-title {
        display: flex;
        align-items: center;
        gap: 0.6rem;
        font-weight: 600;
        font-size: 0.95rem;
    }
    .budget-dot {
        width: 14px;
        height: 14px;
        border-radius: 50%;
        flex: 0 0 auto;
        box-shadow: 0 0 0 3px rgba(0, 0, 0, 0.18);
    }
    .budget-card-arrow {
        opacity: 0.5;
        transition: all 0.2s ease;
        font-size: 1.1rem;
    }
    .budget-card:hover .budget-card-arrow {
        opacity: 1;
        transform: translateX(3px);
    }
    .budget-meta {
        display: flex;
        justify-content: space-between;
        font-size: 0.9rem;
        font-weight: 500;
        margin-top: 0.2rem;
    }
    .budget-submeta {
        display: flex;
        justify-content: space-between;
        font-size: 0.8rem;
        opacity: 0.75;
        margin-top: 0.4rem;
    }
    
    /* Wrapper para budget card clicável */
    .budget-card-wrapper {
        position: relative;
        cursor: pointer;
        margin-bottom: 0.5rem;
    }

    /* Dialogs mais largos (modais) */
    div[data-testid="stDialog"] div[role="dialog"],
    div[role="dialog"] {
        width: min(1200px, 96vw) !important;
        max-width: 96vw !important;
    }

    /* Cards clicáveis de cartão - Design Moderno */
    .cc-grid {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 1rem;
        margin: 0.5rem 0 1rem 0;
    }
    @media (max-width: 900px) {
        .cc-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
    @media (max-width: 600px) {
        .cc-grid { grid-template-columns: 1fr; }
    }
    
    /* Wrapper para card clicável - contém card + botão invisível */
    .cc-card-wrapper {
        position: relative;
        cursor: pointer;
        margin-bottom: 0.5rem;
    }
    
    .cc-card {
        display: block;
        text-decoration: none;
        color: white;
        border: none;
        border-radius: 20px;
        padding: 1.25rem 1.4rem;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.15);
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        cursor: pointer;
        position: relative;
        overflow: hidden;
    }
    .cc-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background: linear-gradient(135deg, rgba(255,255,255,0.1) 0%, transparent 50%);
        pointer-events: none;
    }
    .cc-card:hover {
        transform: translateY(-6px) scale(1.02);
        box-shadow: 0 12px 30px rgba(0, 0, 0, 0.25);
    }
    .cc-card:active { 
        transform: translateY(-2px) scale(1.01);
    }
    
    /* Ícone de seta no card */
    .cc-card-arrow {
        position: absolute;
        top: 1rem;
        right: 1rem;
        opacity: 0.6;
        transition: all 0.3s ease;
        font-size: 1.2rem;
    }
    .cc-card:hover .cc-card-arrow {
        opacity: 1;
        transform: translateX(4px);
    }
    
    /* Cores específicas por tipo de cartão */
    .cc-card-debit {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
    }
    .cc-card-household {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
    }
    .cc-card-0 {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    }
    .cc-card-1 {
        background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
    }
    .cc-card-2 {
        background: linear-gradient(135deg, #fa709a 0%, #fee140 100%);
    }
    .cc-card-3 {
        background: linear-gradient(135deg, #30cfd0 0%, #330867 100%);
    }
    .cc-card-4 {
        background: linear-gradient(135deg, #a8edea 0%, #fed6e3 100%);
    }
    .cc-card-5 {
        background: linear-gradient(135deg, #ff9a9e 0%, #fecfef 100%);
    }
    .cc-card-6 {
        background: linear-gradient(135deg, #43e97b 0%, #38f9d7 100%);
    }
    
    .cc-card-icon {
        font-size: 2rem;
        margin-bottom: 0.5rem;
        display: block;
    }
    .cc-card-title {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 0.75rem;
        font-size: 0.95rem;
        margin-bottom: 0.6rem;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .cc-card-title span:first-child {
        font-weight: 700;
        font-size: 1.05rem;
        letter-spacing: 0.3px;
    }
    .cc-card-title span:last-child {
        opacity: 0.85;
        font-size: 0.8rem;
    }
    .cc-card-value {
        font-size: 1.7rem;
        font-weight: 800;
        letter-spacing: 0.5px;
        margin: 0.5rem 0;
        text-shadow: 0 2px 4px rgba(0, 0, 0, 0.15);
    }
    .cc-card-sub {
        margin-top: 0.6rem;
        font-size: 0.82rem;
        opacity: 0.85;
        display: flex;
        align-items: center;
        gap: 0.35rem;
    }
    .cc-card-sub::before {
        content: "📅";
        font-size: 0.85rem;
    }

    /* Seções */
    .section-header {
        font-size: 1.25rem;
        font-weight: 600;
        margin: 1.5rem 0 1rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #667eea;
    }
    
    /* ============================================
       CARDS DE FATURA - LAYOUT COMPACTO UNIFORMIZADO
       ============================================ */
    .fatura-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
        gap: 0.75rem;
        margin: 1rem 0;
    }
    
    .fatura-card {
        background: linear-gradient(135deg, rgba(255, 255, 255, 0.08) 0%, rgba(255, 255, 255, 0.03) 100%);
        border: 1px solid rgba(255, 255, 255, 0.12);
        border-radius: 12px;
        padding: 0.9rem;
        cursor: pointer;
        transition: all 0.2s ease;
        min-height: 120px;
        display: flex;
        flex-direction: column;
    }
    .fatura-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
        border-color: rgba(255, 255, 255, 0.25);
    }
    .fatura-card-name {
        font-size: 1rem;
        font-weight: 700;
        margin-bottom: 0.3rem;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .fatura-card-due {
        font-size: 0.75rem;
        opacity: 0.7;
        margin-bottom: 0.5rem;
    }
    .fatura-card-value {
        font-size: 1.2rem;
        font-weight: 800;
        margin-bottom: 0.5rem;
        flex-grow: 1;
    }
    .fatura-badges {
        display: flex;
        gap: 0.4rem;
        flex-wrap: wrap;
    }
    .fatura-badge {
        padding: 0.15rem 0.5rem;
        border-radius: 999px;
        font-size: 0.65rem;
        font-weight: 700;
        display: inline-block;
    }
    .fatura-badge-paga { background: #DCFCE7; color: #166534; }
    .fatura-badge-pendente { background: #FEF3C7; color: #92400E; }
    .fatura-badge-fechada { background: #DBEAFE; color: #1E40AF; }
    .fatura-badge-aberta { background: #F3F4F6; color: #6B7280; }
    
    /* ============================================
       CARD DE REEMBOLSÁVEIS
       ============================================ */
    .reimb-card {
        background: linear-gradient(135deg, rgba(255, 255, 255, 0.10) 0%, rgba(255, 255, 255, 0.05) 100%);
        border: 1px solid rgba(255, 255, 255, 0.14);
        width: min(520px, 100%);
        border-radius: 16px;
        padding: 0.9rem 1.0rem;
        color: white;
        box-shadow: 0 2px 10px rgba(0, 0, 0, 0.12);
        margin: 0.75rem auto;
    }
    .reimb-card-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 0.6rem;
    }
    .reimb-card-title {
        font-size: 1.0rem;
        font-weight: 700;
    }
    .reimb-metrics {
        display: grid;
        grid-template-columns: repeat(3, 1fr);
        gap: 0.75rem;
    }
    .reimb-metric {
        text-align: center;
    }
    .reimb-metric-label {
        font-size: 0.72rem;
        opacity: 0.85;
        margin-bottom: 0.15rem;
    }
    .reimb-metric-value {
        font-size: 1.15rem;
        font-weight: 800;
    }
    .reimb-positive { color: #38ef7d; }
    .reimb-negative { color: #f5576c; }
    
</style>
"""


def _get_query_param(name: str) -> str | None:
    val = st.query_params.get(name)
    if val is None:
        return None
    if isinstance(val, list):
        return str(val[0]) if val else None
    return str(val)


def _pop_query_param(name: str) -> None:
    try:
        st.query_params.pop(name, None)
        return
    except Exception:
        pass
    try:
        if name in st.query_params:
            del st.query_params[name]
    except Exception:
        pass

_COLS_PTBR = {
    "id": "ID",
    "txn_date": "Data da compra",
    "cash_date": "Data (impacto)",
    "amount": "Valor (R$)",
    "description": "Descrição",
    "payment_method": "Forma de pagamento",
    "account": "Conta/Cartão",
    "category": "Categoria",
    "subcategory": "Subcategoria",
    "reimbursable": "Reembolsável",
    "reference": "Referência",
    "notes": "Observações",
    "hash": "Hash",
    "file_path": "Arquivo",
    "importer": "Importador",
    "imported_at": "Importado em",
    "rows": "Linhas",
    "data": "Data",
    "evento": "Evento",
    "pessoa": "Pessoa",
    "valor": "Valor (R$)",
    "acumulado": "Acumulado (R$)",
    "vencimento": "Vencimento",
    "total_fatura": "Total da fatura",
    "data_recebimento": "Data de recebimento",
    "recebimento": "Recebimento",
    "valor_registrado": "Valor registrado",
    "saldo": "Saldo",
}

_PAYMENT_METHOD_PTBR = {
    "income": "Receita",
    "credit_card": "Cartão de crédito",
    "debit": "Débito",
    "pix": "PIX",
    "transfer": "Transferência",
    "cash": "Dinheiro",
}


@st.cache_resource
def _get_conn(db_path_str: str):
    """
    Cria e retorna conexão com o banco de dados.
    O check_same_thread=False permite usar a conexão em diferentes threads do Streamlit.
    """
    conn = pf_db.connect(Path(db_path_str), check_same_thread=False)
    pf_db.migrate(conn)
    pf_db.backfill_debit_cash_dates(conn)
    pf_db.backfill_income_cash_dates(conn)
    return conn


def _compute_pay_events(year: int, month: int, schedule: dict) -> pd.DataFrame:
    rows = []
    last_day = monthrange(year, month)[1]
    for ev in schedule.get("events", []):
        rule = ev.get("rule")
        name = ev.get("name", "Evento")
        person = ev.get("person")
        if rule == "fixed_day":
            d = date(year, month, min(int(ev.get("day", 1)), last_day))
        elif rule == "last_business_day":
            d = last_business_day(year, month)
        else:
            continue
        rows.append({"data": d, "evento": name, "pessoa": person})
    return pd.DataFrame(rows).sort_values("data") if rows else pd.DataFrame(columns=["data", "evento", "pessoa"])


def _fmt_brl(value: float) -> str:
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _display_df_ptbr(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out = out.rename(columns={k: v for k, v in _COLS_PTBR.items() if k in out.columns})

    if "Forma de pagamento" in out.columns:
        out["Forma de pagamento"] = out["Forma de pagamento"].apply(
            lambda v: _PAYMENT_METHOD_PTBR.get(str(v), v)
        )

    if "Reembolsável" in out.columns:
        def _to_sim_nao(v) -> str:
            if v is None:
                return ""
            try:
                if pd.isna(v):
                    return ""
            except Exception:
                pass
            try:
                return "Sim" if int(v) == 1 else "Não"
            except Exception:
                s = str(v).strip().lower()
                if s in ("sim", "s", "yes", "y", "true", "1"):
                    return "Sim"
                if s in ("nao", "não", "n", "no", "false", "0"):
                    return "Não"
                return str(v)

        out["Reembolsável"] = out["Reembolsável"].apply(_to_sim_nao)

    return out


def _expense_categories(expense_tree: dict) -> tuple[list[str], dict[str, list[str]]]:
    categories: list[str] = []
    sub_map: dict[str, list[str]] = {}
    for category, subs in expense_tree.items():
        if not isinstance(subs, list):
            continue
        cat = str(category).strip()
        if not cat:
            continue
        categories.append(cat)
        sub_map[cat] = [str(x).strip() for x in subs if str(x).strip()]
    return categories, sub_map


def _income_categories(income_tree: dict) -> list[str]:
    # Expected: {"Receitas": [..]}
    node = income_tree.get("Receitas")
    if isinstance(node, list):
        return [str(x).strip() for x in node if str(x).strip()]
    cats: list[str] = []
    for _, v in income_tree.items():
        if isinstance(v, list):
            cats.extend([str(x).strip() for x in v if str(x).strip()])
    return cats


def _derive_category_subcategory(row: dict | pd.Series) -> tuple[str, str]:
    grp = str(row.get("group_name") or "").strip()
    cat = str(row.get("category") or "").strip()
    sub = str(row.get("subcategory") or "").strip()
    if sub:
        return (cat or grp, sub)
    if grp and cat:
        return (grp, cat)
    return (cat or grp, "")


def _save_upload(base_dir: Path, upload) -> Path:
    uploads_dir = base_dir / "raw_data" / "_uploads"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    safe_name = Path(upload.name).name
    dest = uploads_dir / f"{time.time_ns()}_{safe_name}"
    dest.write_bytes(upload.getbuffer())
    return dest


def _normalize_legacy_expense_categories(conn) -> None:
    """
    Legacy versions stored (group_name, category, subcategory) with 2 or 3 levels.
    Current MVP uses ONLY (category, subcategory). This normalizes expense rows by:
    - Dropping group_name when subcategory is already present.
    - Promoting (group_name -> category, category -> subcategory) when subcategory is missing.
    """
    now = pf_db.now_iso()
    conn.execute(
        """
        UPDATE transactions
        SET group_name = NULL, updated_at = ?
        WHERE amount < 0
          AND group_name IS NOT NULL AND TRIM(group_name) <> ''
          AND subcategory IS NOT NULL AND TRIM(subcategory) <> ''
        """,
        (now,),
    )
    conn.execute(
        """
        UPDATE transactions
        SET subcategory = category, category = group_name, group_name = NULL, updated_at = ?
        WHERE amount < 0
          AND group_name IS NOT NULL AND TRIM(group_name) <> ''
          AND (subcategory IS NULL OR TRIM(subcategory) = '')
          AND category IS NOT NULL AND TRIM(category) <> ''
        """,
        (now,),
    )
    conn.commit()


def _migrate_credit_card_statement_meta_keys(conn, cards: dict) -> None:
    """
    Migração leve (retrocompatibilidade):
    versões anteriores salvaram o `card_source` como "excel_credit_card", que não é único por cartão.
    Tentamos reatribuir para o id do cartão (ou fallback por nome) quando o vencimento mapeia para um único cartão.
    """
    try:
        rows = conn.execute(
            """
            SELECT card_source, statement_due_date
            FROM credit_card_statements
            WHERE card_source = 'excel_credit_card'
            """
        ).fetchall()
        if not rows:
            return

        card_id_by_name = {c.name: c.id for c in cards.values()}

        for r in rows:
            due = str(r["statement_due_date"] or "").strip()
            if not due:
                continue

            acc_rows = conn.execute(
                """
                SELECT DISTINCT account
                FROM transactions
                WHERE payment_method = 'credit_card'
                  AND statement_due_date = ?
                """,
                (due,),
            ).fetchall()
            accounts = sorted({str(a["account"] or "").strip() for a in acc_rows if str(a["account"] or "").strip()})
            if len(accounts) != 1:
                continue
            account = accounts[0]
            new_key = card_id_by_name.get(account) or normalize_str(account) or account

            existing = conn.execute(
                """
                SELECT 1
                FROM credit_card_statements
                WHERE card_source = ?
                  AND statement_due_date = ?
                LIMIT 1
                """,
                (new_key, due),
            ).fetchone()
            if existing is None:
                conn.execute(
                    """
                    UPDATE credit_card_statements
                    SET card_source = ?, updated_at = ?
                    WHERE card_source = 'excel_credit_card'
                      AND statement_due_date = ?
                    """,
                    (new_key, pf_db.now_iso(), due),
                )
            else:
                conn.execute(
                    """
                    DELETE FROM credit_card_statements
                    WHERE card_source = 'excel_credit_card'
                      AND statement_due_date = ?
                    """,
                    (due,),
                )
        conn.commit()
    except Exception:  # noqa: BLE001
        return


def main() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)
    st.title(f"💰 {APP_TITLE}")

    base_dir = pf_config.repo_root()
    paths = pf_db.default_paths(base_dir)
    
    # Backup automático do banco de dados
    _backup_database(paths.db_path, base_dir / "raw_data")

    cards = pf_config.load_cards_config(base_dir)
    expense_categories_tree = pf_config.load_expense_categories(base_dir)
    income_categories_tree = pf_config.load_income_categories(base_dir)
    pay_schedule = pf_config.load_pay_schedule(base_dir)
    rules_cfg = pf_config.load_rules(base_dir)
    rules = rules_cfg.get("rules", []) if isinstance(rules_cfg, dict) else []
    budgets_cfg = pf_config.load_budgets(base_dir)
    budgets = budgets_cfg.get("budgets", {}) if isinstance(budgets_cfg, dict) else {}
    # Calcular total_monthly_budget automaticamente a partir da soma dos budgets individuais
    total_budget = sum(float(v) for v in budgets.values() if isinstance(v, (int, float)))

    conn = _get_conn(str(paths.db_path))
    _normalize_legacy_expense_categories(conn)
    _migrate_credit_card_statement_meta_keys(conn, cards)

    pending_review_count = pf_db.count_pending_reviews(conn)

    main_pages = ["Dashboard", "Gerenciamento de Cartões", "Investimentos"]
    review_label = f"⚠️ Revisão ({pending_review_count})" if pending_review_count > 0 else "Revisão de Importação"
    advanced_pages = ["Transações", review_label, "Config"]
    nav_options = main_pages + advanced_pages
    if "nav" not in st.session_state:
        st.session_state["nav"] = main_pages[0]
    current_nav = st.session_state.get("nav", main_pages[0])
    legacy_map = {
        "Visão Geral": "Dashboard",
        "Importar": "Dashboard",
        "Rotina": "Dashboard",
        "Acerto Mensal": "Transações",
        "Revisão de Importação": review_label,
    }
    current_nav = legacy_map.get(str(current_nav), current_nav)
    if current_nav not in nav_options:
        current_nav = main_pages[0]

    def _go(page: str) -> None:
        st.session_state["nav"] = page
        st.rerun()

    nav = st.sidebar.radio(
        "Página",
        nav_options,
        index=nav_options.index(current_nav),
        key="nav",
        label_visibility="collapsed",
    )
    # Seção de Atualização (CSV/Excel) na sidebar
    raw_dir = base_dir / "raw_data"
    templates_dir = base_dir / "templates"
    unified_xlsx = templates_dir / "financas.xlsx"
    files = pf_ingest.scan_raw_data(base_dir)
    csv_files = [p for p in files if p.suffix.lower() == ".csv"]
    card_ids = list(cards.keys())

    with st.sidebar.expander("🔄 Atualização", expanded=True):
        st.caption("CSV/Excel")
        is_docker = _is_running_in_docker()

        # Garantir que o Excel exista para download/upload/sync.
        pf_excel_unified.ensure_unified_excel(
            unified_xlsx,
            expense_categories_tree=expense_categories_tree,
            income_categories_tree=income_categories_tree,
            cards=[c.name for c in cards.values()],
        )

        excel_bytes = unified_xlsx.read_bytes() if unified_xlsx.exists() else None
        if excel_bytes:
            st.download_button(
                "📊 Abrir Excel (baixar)",
                data=excel_bytes,
                file_name="financas.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                width="stretch",
            )
        else:
            st.info("O Excel será criado automaticamente na primeira sincronização.")

        if not is_docker:
            if st.button("🖥️ Abrir Excel (no servidor)", key="sidebar_open_unified", width="stretch"):
                with st.spinner("Abrindo..."):
                    _open_file(unified_xlsx)
                st.rerun()
        else:
            st.caption("Rodando em Docker: não dá para abrir Excel no servidor. Use baixar/enviar.")

        uploaded_xlsx = st.file_uploader(
            "📤 Enviar Excel atualizado (.xlsx)",
            type=["xlsx"],
            key="sidebar_upload_unified_xlsx",
        )
        if uploaded_xlsx is not None:
            upload_hash = sha256_text(uploaded_xlsx.getvalue().hex())
            if st.session_state.get("last_uploaded_xlsx_hash") != upload_hash:
                templates_dir.mkdir(parents=True, exist_ok=True)
                bk = _backup_unified_excel(unified_xlsx, base_dir / "raw_data" / "backups" / "financas")
                unified_xlsx.write_bytes(uploaded_xlsx.getvalue())
                card_owner_by_name = {c.name: c.owner for c in cards.values()}
                res = pf_ingest.sync_unified_from_excel(
                    conn,
                    path=unified_xlsx,
                    card_owner_by_name=card_owner_by_name,
                )
                def _fmt(sr: pf_db.SyncResult) -> str:
                    return f"{sr.inserted + sr.updated + sr.deleted}"
                if bk is not None:
                    st.caption(f"Backup criado: `{bk.name}`")
                st.success(
                    "✅ Upload salvo e sincronizado. "
                    f"CC:{_fmt(res.credit_card)} D:{_fmt(res.debit)} R:{_fmt(res.income)} C:{_fmt(res.household)}"
                )
                st.session_state["last_uploaded_xlsx_hash"] = upload_hash
                st.rerun()

        uploaded_statement_csvs = st.file_uploader(
            "📤 Enviar faturas CSV (.csv)",
            type=["csv"],
            accept_multiple_files=True,
            key="sidebar_upload_statement_csvs",
            help="Arquivos enviados aqui são processados no botão de importação e removidos em seguida.",
        )
        if uploaded_statement_csvs:
            st.caption(f"{len(uploaded_statement_csvs)} arquivo(s) pronto(s) para importar")

        force = bool(st.session_state.get("dashboard_force", False))
        
        has_uploaded_csvs = bool(uploaded_statement_csvs)
        if st.button(
            "📥 Importar CSVs",
            key="sidebar_import_all",
            disabled=not (csv_files or has_uploaded_csvs),
            width="stretch",
        ):
            imported_msgs: list[str] = []
            skipped_msgs: list[str] = []
            error_msgs: list[str] = []
            imported_paths: list[str] = []
            uploaded_temp_paths: list[Path] = []
            try:
                if uploaded_statement_csvs:
                    for upload in uploaded_statement_csvs:
                        try:
                            uploaded_temp_paths.append(_save_upload(base_dir, upload))
                        except Exception as e:
                            error_msgs.append(f"{upload.name}: falha ao salvar upload ({e})")

                to_import = [*csv_files, *uploaded_temp_paths]
                for p in to_import:
                    try:
                        card_id = guess_card_id_csv(p)
                        if not card_id or card_id not in cards:
                            error_msgs.append(
                                f"{p.name}: cartão não identificado pelo nome do arquivo. "
                                "Use prefixo como XP_, Nubank_, C6_, MercadoPago_ ou PortoBank_."
                            )
                            continue
                        res = pf_ingest.ingest_credit_card_csv(
                            conn,
                            path=p,
                            card=cards[str(card_id)],
                            rules=rules,
                            force=force,
                        )
                        if res.imported:
                            imported_msgs.append(f"{p.name}: {res.rows_inserted}")
                            imported_paths.append(str(p))
                        else:
                            skipped_msgs.append(f"{p.name}")
                    except Exception as e:
                        error_msgs.append(f"{p.name}: {e}")
            finally:
                for p in uploaded_temp_paths:
                    try:
                        if p.exists():
                            p.unlink()
                    except Exception:
                        pass

            if imported_msgs:
                st.success("✅ " + ", ".join(imported_msgs))
            if skipped_msgs:
                st.info("⏭️ Já importados")
            if error_msgs:
                st.error("❌ " + ", ".join(error_msgs))

            if imported_paths:
                placeholders = ", ".join(["?"] * len(imported_paths))
                rows_db = conn.execute(
                    f"""
                    SELECT origin_id, row_hash, txn_date, cash_date, statement_due_date, amount,
                           description, account, category, subcategory, person,
                           reimbursable, notes
                    FROM transactions
                    WHERE payment_method = 'credit_card'
                      AND source_file IN ({placeholders})
                    """,
                    imported_paths,
                ).fetchall()
                pf_excel_unified.append_credit_card_rows(
                    unified_xlsx,
                    rows=[dict(r) for r in rows_db],
                    expense_categories_tree=expense_categories_tree,
                    income_categories_tree=income_categories_tree,
                    cards=[c.name for c in cards.values()],
                )
            st.rerun()

        # Sincronização do Excel ocorre automaticamente ao enviar o arquivo atualizado.

    today = date.today()
    period_expanded = nav in ("Dashboard", "Transações")
    
    with st.sidebar.expander("Período (Dashboard/Transações)", expanded=period_expanded):
        def _add_months(d: date, delta: int) -> date:
            m0 = (d.month - 1) + int(delta)
            y = d.year + (m0 // 12)
            m = (m0 % 12) + 1
            return date(y, m, 1)

        mes_nomes = [
            "Janeiro",
            "Fevereiro",
            "Março",
            "Abril",
            "Maio",
            "Junho",
            "Julho",
            "Agosto",
            "Setembro",
            "Outubro",
            "Novembro",
            "Dezembro",
        ]

        # Gerar lista de meses (inclui futuros)
        meses_dropdown: list[str] = []
        start_m = date(2026, 1, 1)
        max_cash_raw = conn.execute("SELECT MAX(cash_date) FROM transactions").fetchone()[0]
        end_m = start_m
        if max_cash_raw:
            try:
                max_cash_dt = date.fromisoformat(str(max_cash_raw)[:10])
                end_m = date(max_cash_dt.year, max_cash_dt.month, 1)
            except Exception:  # noqa: BLE001
                end_m = start_m
        if end_m < start_m:
            end_m = start_m
        cursor = start_m
        while cursor <= end_m:
            meses_dropdown.append(f"{mes_nomes[cursor.month-1]}/{cursor.year}")
            cursor = _add_months(cursor, 1)

        default_month = date(today.year, today.month, 1)
        if default_month < start_m:
            default_month = start_m
        if default_month > end_m:
            default_month = end_m
        default_label = f"{mes_nomes[default_month.month-1]}/{default_month.year}"
        default_idx = meses_dropdown.index(default_label) if default_label in meses_dropdown else len(meses_dropdown) - 1

        # Dropdown de mês/ano
        mes_selecionado = st.selectbox(
            "📅 Mês",
            meses_dropdown,
            index=default_idx,
            key="period_month"
        )
        
        # Parsear mês selecionado
        partes_mes = mes_selecionado.split("/")
        selected_month = mes_nomes.index(partes_mes[0]) + 1
        selected_year = int(partes_mes[1])
        
        # Calcular datas do mês
        start = date(selected_year, selected_month, 1)
        last_day = monthrange(selected_year, selected_month)[1]
        end = date(selected_year, selected_month, last_day)
        
        st.caption(f"Período: {start.strftime('%d/%m/%Y')} a {end.strftime('%d/%m/%Y')}")

    df = pf_queries.load_transactions_df(conn, start=start, end=end)

    def sync_excels_ui(
        *,
        include_credit_card: bool = False,
        open_credit_card: bool = False,
        open_debit: bool = False,
        open_income: bool = False,
    ) -> None:
        try:
            include_credit_card = include_credit_card or open_credit_card
            out_paths = pf_excel_sync.sync_excel_files(
                base_dir,
                conn=conn,
                expense_categories_tree=expense_categories_tree,
                income_categories_tree=income_categories_tree,
                include_credit_card=bool(include_credit_card),
            )
            ordered = [out_paths.get("debit"), out_paths.get("income")]
            if include_credit_card:
                ordered.append(out_paths.get("credit_card"))
            existing = [p for p in ordered if p is not None and p.exists()]
            if existing:
                st.success("Excel atualizado: " + " | ".join(f"`{p}`" for p in existing))
            else:
                st.info("Nenhuma planilha de saída gerada ainda.")
            
            # Open credit card Excel if requested
            if open_credit_card and include_credit_card:
                cc_path = out_paths.get("credit_card")
                if cc_path and cc_path.exists():
                    _open_file(cc_path)
            if open_debit:
                debit_path = out_paths.get("debit")
                if debit_path and debit_path.exists():
                    _open_file(debit_path)
            if open_income:
                income_path = out_paths.get("income")
                if income_path and income_path.exists():
                    _open_file(income_path)
        except Exception as e:  # noqa: BLE001
            st.warning(f"Falha ao sincronizar Excel: {e}")

    if nav == "Dashboard":
        # Cabeçalho do período
        if start.month == end.month and start.year == end.year:
            month_names = ["", "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
                           "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]
            st.markdown(f"### 📊 {month_names[start.month]} de {start.year}")
        else:
            st.markdown(f"### 📊 {start.strftime('%d/%m/%Y')} a {end.strftime('%d/%m/%Y')}")
        
        # Debug: mostrar quantas transações foram carregadas
        st.caption(f"📝 {len(df)} transações carregadas do banco | Período: {start.strftime('%d/%m/%Y')} a {end.strftime('%d/%m/%Y')}")

        # Métricas principais
        income = float(df.loc[df["payment_method"] == "income", "amount"].sum()) if not df.empty else 0.0
        expense = float((-df.loc[df["amount"] < 0, "amount"].sum())) if not df.empty else 0.0
        net = income - expense
        cc = df[df["payment_method"] == "credit_card"].copy() if not df.empty else df
        cc_total = float((-cc["amount"].sum())) if not cc.empty else 0.0

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            delta_income = None
            st.metric("💵 Receitas", _fmt_brl(income), delta=delta_income)
        with col2:
            budget_pct = (expense / total_budget * 100) if total_budget > 0 else 0
            delta_expense = f"{budget_pct:.0f}% do orçamento" if total_budget > 0 else None
            st.metric("💸 Despesas", _fmt_brl(expense), delta=delta_expense, delta_color="inverse")
        with col3:
            delta_color = "normal" if net >= 0 else "inverse"
            st.metric("📈 Resultado", _fmt_brl(net), delta="Positivo" if net >= 0 else "Negativo", delta_color=delta_color)
        with col4:
            st.metric("💳 Cartões", _fmt_brl(cc_total))

        # ========================================
        # ACERTO (sempre visível no Dashboard)
        # ========================================
        df_despesas_acerto = pd.DataFrame()
        meses_nomes = ["", "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
                       "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]

        acerto_month = selected_month
        acerto_year = selected_year
        if acerto_month == 1:
            prev_acerto_month = 12
            prev_acerto_year = acerto_year - 1
        else:
            prev_acerto_month = acerto_month - 1
            prev_acerto_year = acerto_year

        # Cartões de crédito (regra do README):
        # - XP e Nubank Aline → entram no mês do acerto
        # - Demais cartões → entram com a fatura do mês anterior
        cartao_prev_start = date(prev_acerto_year, prev_acerto_month, 1)
        cartao_curr_end = date(acerto_year, acerto_month, monthrange(acerto_year, acerto_month)[1])
        acerto_period = (acerto_year * 12) + acerto_month

        df_acerto_cc_all = pf_queries.load_transactions_df(conn, start=cartao_prev_start, end=cartao_curr_end)
        df_acerto_cc_all = (
            df_acerto_cc_all[df_acerto_cc_all["payment_method"] == "credit_card"].copy()
            if not df_acerto_cc_all.empty
            else df_acerto_cc_all
        )
        if not df_acerto_cc_all.empty:
            offset_by_account = {c.name: (0 if c.id in ("xp", "nubank_aline") else -1) for c in cards.values()}
            due_col = "statement_due_date" if "statement_due_date" in df_acerto_cc_all.columns else "cash_date"
            due_dt = pd.to_datetime(df_acerto_cc_all[due_col], errors="coerce")
            due_period = (due_dt.dt.year * 12) + due_dt.dt.month
            offset = df_acerto_cc_all["account"].map(offset_by_account)
            fallback_offset = (due_dt.dt.day > 10).astype(int) * -1
            offset = offset.fillna(fallback_offset)
            expected_period = acerto_period + offset.astype(int)
            df_acerto_cc = df_acerto_cc_all[due_period == expected_period].copy()
        else:
            df_acerto_cc = df_acerto_cc_all

        # Débitos/PIX do mês anterior entram no acerto do mês corrente (por txn_date).
        acerto_debito_start = date(prev_acerto_year, prev_acerto_month, 1)
        acerto_debito_end = date(prev_acerto_year, prev_acerto_month, monthrange(prev_acerto_year, prev_acerto_month)[1])
        df_acerto_deb = pf_queries.load_transactions_df_by_txn_date(conn, start=acerto_debito_start, end=acerto_debito_end)
        df_acerto_deb = (
            df_acerto_deb[~df_acerto_deb["payment_method"].isin(["credit_card", "household", "income"])].copy()
            if not df_acerto_deb.empty
            else df_acerto_deb
        )

        # Contas da Casa entram no mês em que foram pagas (cash_date no mês do acerto).
        acerto_casa_start = date(acerto_year, acerto_month, 1)
        acerto_casa_end = date(acerto_year, acerto_month, monthrange(acerto_year, acerto_month)[1])
        df_acerto_casa = pf_queries.load_transactions_df(conn, start=acerto_casa_start, end=acerto_casa_end)
        df_acerto_casa = (
            df_acerto_casa[df_acerto_casa["payment_method"] == "household"].copy()
            if not df_acerto_casa.empty
            else df_acerto_casa
        )

        df_acerto_all = pd.concat([df_acerto_cc, df_acerto_deb], ignore_index=True)
        acerto_result = pf_recon.calculate_reconciliation(
            df_acerto_all,
            reference_month=acerto_month,
            reference_year=acerto_year,
            include_household=True,
            df_household=df_acerto_casa,
        )

        @st.dialog("🤝 Detalhes do Acerto")
        def _show_acerto_details_dialog() -> None:
            st.caption(
                f"Mês do acerto: {meses_nomes[acerto_month]}/{acerto_year} • "
                f"Débitos/PIX: {meses_nomes[prev_acerto_month]}/{prev_acerto_year} • "
                "Contas da Casa: pagas no mês do acerto."
            )
            st.caption(
                'Regras: "Gastos Renan" = 100% Renan; "Gastos Aline" = 100% Aline; demais = 50/50. '
                'Créditos/estornos reduzem o gasto.'
            )

            saldo = float(acerto_result.aline_deve_renan)
            if saldo >= 0:
                direction = "Aline → Renan"
            else:
                direction = "Renan → Aline"

            c1, c2, c3 = st.columns(3)
            c1.metric("Renan pagou (net)", _fmt_brl(float(acerto_result.renan_pagou_total)))
            c2.metric("Renan deveria pagar", _fmt_brl(float(acerto_result.renan_deveria_pagar)))
            c3.metric("Saldo", _fmt_brl(abs(saldo)), delta=direction, delta_color="off")

            st.markdown("---")
            details_df = pd.DataFrame(acerto_result.detalhes)
            if details_df.empty:
                st.info("Sem transações consideradas no acerto.")
                return

            tabs = st.tabs(["Transações", "Por categoria"])
            with tabs[0]:
                show_cols = [
                    c
                    for c in [
                        "txn_date",
                        "payment_method",
                        "account",
                        "description",
                        "category",
                        "subcategory",
                        "person",
                        "valor",
                        "regra",
                        "renan_deveria",
                        "aline_deveria",
                        "renan_delta",
                    ]
                    if c in details_df.columns
                ]
                show = details_df[show_cols].copy()
                for col in ("valor", "renan_deveria", "aline_deveria", "renan_delta"):
                    if col in show.columns:
                        show[col] = show[col].apply(lambda v: _fmt_brl(float(v)))
                st.dataframe(show, width="stretch", hide_index=True)

            with tabs[1]:
                if "regra" not in details_df.columns or "valor" not in details_df.columns:
                    st.info("Sem dados para agrupar.")
                    return
                div = details_df[(details_df["regra"] == "Dividir (50/50)") & details_df["category"].notna()].copy()
                div = div[div["category"].astype(str).str.strip() != ""]
                if div.empty:
                    st.info("Sem itens para dividir por categoria.")
                    return
                by_cat = div.groupby("category")["valor"].sum().sort_values(ascending=False).reset_index()
                by_cat.columns = ["Categoria", "Total (net)"]
                by_cat["Cada um (÷2)"] = by_cat["Total (net)"] / 2
                by_cat["Total (net)"] = by_cat["Total (net)"].apply(lambda v: _fmt_brl(float(v)))
                by_cat["Cada um (÷2)"] = by_cat["Cada um (÷2)"].apply(lambda v: _fmt_brl(float(v)))
                st.dataframe(by_cat, width="stretch", hide_index=True)

        st.markdown("---")
        col_a, col_b = st.columns([3, 1])
        with col_a:
            saldo = float(acerto_result.aline_deve_renan)
            direction = "Aline → Renan" if saldo >= 0 else "Renan → Aline"
            st.metric("🤝 Acerto do mês", _fmt_brl(abs(saldo)), delta=direction, delta_color="off")
        with col_b:
            if st.button("Ver detalhes", key=f"acerto_details_{acerto_year}_{acerto_month}", width="stretch"):
                _show_acerto_details_dialog()

        if SHOW_ACERTO_UI:
            # ========================================
            # ANÁLISE DO ACERTO DO MÊS SELECIONADO
            # ========================================
            st.markdown("---")

            meses_nomes = ["", "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
                           "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]

            st.markdown(f"### 💰 Acerto de {meses_nomes[selected_month]}/{selected_year}")

            # O mês do acerto é o mês selecionado pelo usuário
            acerto_month = selected_month
            acerto_year = selected_year

            # Mês anterior ao acerto (para débitos)
            if acerto_month == 1:
                prev_acerto_month = 12
                prev_acerto_year = acerto_year - 1
            else:
                prev_acerto_month = acerto_month - 1
                prev_acerto_year = acerto_year

            # Cartões de crédito (regra do README):
            # - XP e Nubank Aline → entram no mês do acerto
            # - Demais cartões → entram com a fatura do mês anterior
            cartao_curr_start = date(acerto_year, acerto_month, 1)
            cartao_curr_end = date(acerto_year, acerto_month, monthrange(acerto_year, acerto_month)[1])
            cartao_prev_start = date(prev_acerto_year, prev_acerto_month, 1)
            cartao_prev_end = date(prev_acerto_year, prev_acerto_month, monthrange(prev_acerto_year, prev_acerto_month)[1])
            acerto_period = (acerto_year * 12) + acerto_month

            # Débitos/PIX: usamos txn_date do mês anterior (padrão do acerto)
            acerto_debito_start = date(prev_acerto_year, prev_acerto_month, 1)
            acerto_debito_end = date(prev_acerto_year, prev_acerto_month, monthrange(prev_acerto_year, prev_acerto_month)[1])

            # Buscar faturas, débitos e contas da casa
            df_acerto_cc_all = pf_queries.load_transactions_df(conn, start=cartao_prev_start, end=cartao_curr_end)
            df_acerto_cc_all = (
                df_acerto_cc_all[df_acerto_cc_all["payment_method"] == "credit_card"].copy()
                if not df_acerto_cc_all.empty
                else df_acerto_cc_all
            )
            if not df_acerto_cc_all.empty:
                offset_by_account = {
                    c.name: (0 if c.id in ("xp", "nubank_aline") else -1)
                    for c in cards.values()
                }
                due_col = "statement_due_date" if "statement_due_date" in df_acerto_cc_all.columns else "cash_date"
                due_dt = pd.to_datetime(df_acerto_cc_all[due_col], errors="coerce")
                due_period = (due_dt.dt.year * 12) + due_dt.dt.month
                offset = df_acerto_cc_all["account"].map(offset_by_account)
                # Fallback para cartões desconhecidos: se vencimento for até dia 10, entra no mês; senão, mês anterior.
                fallback_offset = (due_dt.dt.day > 10).astype(int) * -1
                offset = offset.fillna(fallback_offset)
                expected_period = acerto_period + offset.astype(int)
                df_acerto_cc = df_acerto_cc_all[due_period == expected_period].copy()
            else:
                df_acerto_cc = df_acerto_cc_all

            # Débitos/PIX do mês anterior entram no acerto do mês corrente.
            df_acerto_deb = pf_queries.load_transactions_df_by_txn_date(conn, start=acerto_debito_start, end=acerto_debito_end)
            df_acerto_deb = (
                df_acerto_deb[~df_acerto_deb["payment_method"].isin(["credit_card", "household", "income"])].copy()
                if not df_acerto_deb.empty
                else df_acerto_deb
            )

            # Contas da Casa entram no mês em que foram pagas (cash_date = Data Pagamento).
            acerto_casa_start = date(acerto_year, acerto_month, 1)
            acerto_casa_end = date(acerto_year, acerto_month, monthrange(acerto_year, acerto_month)[1])
            df_acerto_casa = pf_queries.load_transactions_df(conn, start=acerto_casa_start, end=acerto_casa_end)
            df_acerto_casa = (
                df_acerto_casa[df_acerto_casa["payment_method"] == "household"].copy() if not df_acerto_casa.empty else df_acerto_casa
            )

            df_acerto_all = pd.concat([df_acerto_cc, df_acerto_deb], ignore_index=True)
            df_acerto_all_with_household = pd.concat([df_acerto_cc, df_acerto_deb, df_acerto_casa], ignore_index=True)

            df_despesas_acerto = pd.DataFrame()
            if not df_acerto_all.empty or not df_acerto_casa.empty:
                df_despesas_acerto = df_acerto_all_with_household[df_acerto_all_with_household["amount"] < 0].copy()
                df_despesas_acerto["valor"] = df_despesas_acerto["amount"].abs()

                result_preview = pf_recon.calculate_reconciliation(
                    df_acerto_all,
                    reference_month=acerto_month,
                    reference_year=acerto_year,
                    include_household=True,
                    df_household=df_acerto_casa,
                )

                total_dividir = float(result_preview.total_dividir + result_preview.total_contas_casa)
                metade_dividir = total_dividir / 2
                total_acerto = float(result_preview.total_despesas)
                aline_paga = float(result_preview.aline_deve_renan)
                sem_categoria = float(result_preview.sem_categoria)
                qtd_sem_cat = int(result_preview.qtd_sem_categoria)

                # Cards de resumo do acerto
                col_a1, col_a2, col_a3, col_a4 = st.columns(4)
                with col_a1:
                    st.metric("📊 Total do Acerto", _fmt_brl(total_acerto),
                             delta=f"{meses_nomes[acerto_month]}/{acerto_year}")
                with col_a2:
                    st.metric("🔄 Dividir ÷2", _fmt_brl(total_dividir),
                             delta=f"Cada um: {_fmt_brl(metade_dividir)}")
                with col_a3:
                    if qtd_sem_cat > 0:
                        st.metric("⚠️ Sem Categoria", _fmt_brl(sem_categoria),
                                 delta=f"{qtd_sem_cat} itens pendentes", delta_color="inverse")
                    else:
                        st.metric("✅ Categorizado", "100%", delta="Tudo OK!")
                with col_a4:
                    direction = "Aline → Renan" if aline_paga >= 0 else "Renan → Aline"
                    st.metric(
                        "💵 Acerto",
                        _fmt_brl(abs(aline_paga)),
                        delta=direction,
                        delta_color="off",
                    )

            else:
                st.info(f"Sem transações para o acerto de {meses_nomes[acerto_month]}/{acerto_year}")

        # Faturas por Cartão (Status) + Débitos
        st.markdown("---")
        st.markdown("### 💳 Faturas por Cartão")
        st.caption(
            "Configure a data de **fechamento** em **Gerenciamento de Cartões** e marque a fatura como **paga** quando houver data de pagamento. "
            "Clique em uma fatura (ou Débitos) para ver detalhes."
        )

        # Regra do README (Acerto Mensal) aplicada nos cards de fatura:
        # - XP e Nubank Aline → fatura do mês selecionado
        # - Demais cartões (ex.: Nubank Renan, C6, Mercado Pago) → fatura do mês anterior
        ref_year = start.year
        ref_month = start.month
        prev_month = ref_month - 1 if ref_month > 1 else 12
        prev_year = ref_year if ref_month > 1 else ref_year - 1

        prev_start = date(prev_year, prev_month, 1)
        curr_end = date(ref_year, ref_month, monthrange(ref_year, ref_month)[1])
        acerto_period = (ref_year * 12) + ref_month

        cc_all = pf_queries.load_transactions_df(conn, start=prev_start, end=curr_end)
        cc_df = (
            cc_all[cc_all["payment_method"] == "credit_card"].copy()
            if not cc_all.empty
            else cc_all
        )

        if cc_df.empty or "account" not in cc_df.columns:
            st.info("Sem lançamentos de cartão para as faturas (regra do acerto).")
        else:
            offset_by_account = {c.name: (0 if c.id in ("xp", "nubank_aline") else -1) for c in cards.values()}
            due_col = "statement_due_date" if "statement_due_date" in cc_df.columns else "cash_date"
            due_dt = pd.to_datetime(cc_df[due_col], errors="coerce")
            due_period = (due_dt.dt.year * 12) + due_dt.dt.month
            offset = cc_df["account"].map(offset_by_account)
            fallback_offset = (due_dt.dt.day > 10).astype(int) * -1
            offset = offset.fillna(fallback_offset)
            expected_period = acerto_period + offset.astype(int)
            cc_df = cc_df[due_period == expected_period].copy()

            if cc_df.empty:
                st.info("Sem faturas no mês (pela regra do acerto).")
            else:
                # Dados para detalhes (faturas) e para o quadro de status.
                cc_df = cc_df.copy()

                # Débitos do mês selecionado (cash impact no mês)
                if df.empty:
                    debit_exp = pd.DataFrame()
                else:
                    debit_df = df.copy()
                    if "payment_method" in debit_df.columns:
                        debit_df = debit_df[~debit_df["payment_method"].isin(["credit_card", "income", "household"])].copy()
                    debit_exp = debit_df[debit_df["amount"] < 0].copy() if "amount" in debit_df.columns else pd.DataFrame()
                debit_total = abs(float(debit_exp["amount"].sum())) if not debit_exp.empty and "amount" in debit_exp.columns else 0.0

                # Contas da Casa do mês selecionado
                if df.empty:
                    house_exp = pd.DataFrame()
                else:
                    house_df = df.copy()
                    if "payment_method" in house_df.columns:
                        house_df = house_df[house_df["payment_method"] == "household"].copy()
                    house_exp = house_df[house_df["amount"] < 0].copy() if "amount" in house_df.columns else pd.DataFrame()
                house_total = abs(float(house_exp["amount"].sum())) if not house_exp.empty and "amount" in house_exp.columns else 0.0

                @st.dialog("🏦 Débitos/PIX")
                def show_debit_detail_dialog() -> None:
                    if debit_exp.empty:
                        st.info("Sem débitos/PIX no período.")
                        return
                    d = debit_exp.copy()
                    d["valor"] = d["amount"].abs()
                    cols_show = [c for c in ["txn_date", "description", "category", "subcategory", "payment_method", "valor"] if c in d.columns]
                    out = d[cols_show].copy().rename(
                        columns={
                            "txn_date": "Data",
                            "description": "Descrição",
                            "category": "Categoria",
                            "subcategory": "Subcategoria",
                            "payment_method": "Forma",
                            "valor": "Valor",
                        }
                    )
                    if "Valor" in out.columns:
                        out["Valor"] = out["Valor"].apply(_fmt_brl)
                    if "Data" in out.columns:
                        out = out.sort_values("Data", ascending=False)
                    st.metric("Total", _fmt_brl(float(d["valor"].sum())))
                    st.dataframe(out, hide_index=True, width="stretch")

                @st.dialog("🏠 Contas da Casa")
                def show_house_detail_dialog() -> None:
                    if house_exp.empty:
                        st.info("Sem contas da casa no período.")
                        return
                    h = house_exp.copy()
                    h["valor"] = h["amount"].abs()
                    cols_show = [c for c in ["cash_date", "description", "category", "subcategory", "valor"] if c in h.columns]
                    out = h[cols_show].copy().rename(
                        columns={
                            "cash_date": "Data",
                            "description": "Descrição",
                            "category": "Categoria",
                            "subcategory": "Subcategoria",
                            "valor": "Valor",
                        }
                    )
                    if "Valor" in out.columns:
                        out["Valor"] = out["Valor"].apply(_fmt_brl)
                    if "Data" in out.columns:
                        out = out.sort_values("Data", ascending=False)
                    st.metric("Total", _fmt_brl(float(h["valor"].sum())))
                    st.dataframe(out, hide_index=True, width="stretch")

                @st.dialog("🧾 Detalhes da Fatura")
                def show_statement_detail_dialog(account_name: str, due_date: date) -> None:
                    if cc_df.empty:
                        st.info("Sem dados disponíveis.")
                        return
                    due_col = "statement_due_date" if "statement_due_date" in cc_df.columns else "cash_date"
                    data = cc_df[
                        (cc_df.get("account") == account_name)
                        & (cc_df.get(due_col) == due_date)
                    ].copy()
                    if data.empty:
                        st.info("Nenhuma transação para essa fatura.")
                        return

                    # "amount" no banco segue a convenção do app:
                    # - Despesas (compras) < 0
                    # - Créditos/estornos > 0
                    # Para o usuário, exibimos compras como positivas e estornos como negativos.
                    total = abs(float(data["amount"].sum()))
                    st.markdown(f"### 💳 {html.escape(str(account_name))}")
                    st.caption(f"Vencimento: {due_date.strftime('%d/%m/%Y')}")
                    if "txn_date" in data.columns and not data["txn_date"].isna().all():
                        min_d = data["txn_date"].min()
                        max_d = data["txn_date"].max()
                        if pd.notna(min_d) and pd.notna(max_d):
                            st.caption(f"Período: {min_d.strftime('%d/%m/%Y')} → {max_d.strftime('%d/%m/%Y')}")
                    st.metric("Total da Fatura", _fmt_brl(total))
                    st.markdown("---")

                    def _extract_installments(text: str | None) -> str | None:
                        if not text:
                            return None
                        m = re.search(r"(?:parcela\s*:?\s*)?(\d+)\s*de\s*(\d+)", str(text), flags=re.IGNORECASE)
                        if not m:
                            return None
                        return f"{int(m.group(1))}/{int(m.group(2))}"

                    show = data.copy()
                    show["valor"] = -show["amount"]
                    if "notes" in show.columns:
                        show["parcelas"] = show["notes"].apply(_extract_installments)

                    cols_show = [c for c in ["txn_date", "description", "category", "subcategory", "parcelas", "valor"] if c in show.columns]
                    out = show[cols_show].copy().rename(
                        columns={
                            "txn_date": "Data",
                            "description": "Descrição",
                            "category": "Categoria",
                            "subcategory": "Subcategoria",
                            "parcelas": "Parcela",
                            "valor": "Valor",
                        }
                    )
                    if "Valor" in out.columns:
                        out["Valor"] = out["Valor"].apply(_fmt_brl)
                    if "Data" in out.columns:
                        out = out.sort_values("Data", ascending=False)
                    st.dataframe(out, hide_index=True, width="stretch")

                def _render_statement_details(account_name: str, due_date: date) -> None:
                    if cc_df.empty:
                        st.info("Sem dados disponíveis.")
                        return
                    due_col = "statement_due_date" if "statement_due_date" in cc_df.columns else "cash_date"
                    data = cc_df[
                        (cc_df.get("account") == account_name)
                        & (cc_df.get(due_col) == due_date)
                    ].copy()
                    if data.empty:
                        st.info("Nenhuma transação para essa fatura.")
                        return

                    total = abs(float(data["amount"].sum()))
                    st.markdown(f"### 💳 {html.escape(str(account_name))}")
                    st.caption(f"Vencimento: {due_date.strftime('%d/%m/%Y')}")
                    if "txn_date" in data.columns and not data["txn_date"].isna().all():
                        min_d = data["txn_date"].min()
                        max_d = data["txn_date"].max()
                        if pd.notna(min_d) and pd.notna(max_d):
                            st.caption(f"Período: {min_d.strftime('%d/%m/%Y')} → {max_d.strftime('%d/%m/%Y')}")
                    st.metric("Total da Fatura", _fmt_brl(total))
                    st.markdown("---")

                    def _extract_installments(text: str | None) -> str | None:
                        if not text:
                            return None
                        m = re.search(r"(?:parcela\s*:?\s*)?(\d+)\s*de\s*(\d+)", str(text), flags=re.IGNORECASE)
                        if not m:
                            return None
                        return f"{int(m.group(1))}/{int(m.group(2))}"

                    show = data.copy()
                    show["valor"] = -show["amount"]
                    if "notes" in show.columns:
                        show["parcelas"] = show["notes"].apply(_extract_installments)

                    cols_show = [c for c in ["txn_date", "description", "category", "subcategory", "parcelas", "valor"] if c in show.columns]
                    out = show[cols_show].copy().rename(
                        columns={
                            "txn_date": "Data",
                            "description": "Descrição",
                            "category": "Categoria",
                            "subcategory": "Subcategoria",
                            "parcelas": "Parcela",
                            "valor": "Valor",
                        }
                    )
                    if "Valor" in out.columns:
                        out["Valor"] = out["Valor"].apply(_fmt_brl)
                    if "Data" in out.columns:
                        out = out.sort_values("Data", ascending=False)
                    st.dataframe(out, hide_index=True, width="stretch")

                stmt_cols = [c for c in ["account", "statement_due_date", "statement_closing_date", "amount"] if c in cc_df.columns]
                stmt_df = cc_df[stmt_cols].copy()

                if "statement_due_date" not in stmt_df.columns:
                    st.info("Sem datas de vencimento para montar o quadro de status.")
                else:
                    if "account" not in stmt_df.columns:
                        stmt_df["account"] = "(sem cartão)"
                    if "statement_closing_date" not in stmt_df.columns:
                        stmt_df["statement_closing_date"] = None

                    grouped = (
                        stmt_df.groupby(["account", "statement_due_date"], dropna=False)
                        .agg(
                            total_amount=("amount", "sum"),
                            closing_date=("statement_closing_date", "max"),
                        )
                        .reset_index()
                    )
                    grouped["total_fatura"] = grouped["total_amount"].abs()
                    grouped = grouped.sort_values(["statement_due_date", "account"])

                    card_id_by_name = {c.name: c.id for c in cards.values()}
                    card_id_by_norm_name = {normalize_str(c.name): c.id for c in cards.values()}
                    
                    # Renderizar cards SEMPRE em 1 linha (cards quadrados via CSS)
                    total_cards = 2 + int(len(grouped))  # Débitos + Contas Casa + faturas
                    cols = st.columns(total_cards, gap="small")
                    col_idx = 0

                    def _next_col():
                        nonlocal col_idx
                        col = cols[min(col_idx, len(cols) - 1)]
                        col_idx += 1
                        return col

                    # Card de Débitos primeiro
                    with _next_col():
                        with st.container(border=True):
                            st.markdown(
                                f"""
                                <div class="stmt-card-wrapper stmt-card-wrapper">
                                  <div>
                                    <div class="stmt-card-title">🏦 Débitos/PIX</div>
                                    <div class="stmt-card-sub">Mês: {start.strftime('%m/%Y')}</div>
                                  </div>
                                  <div class="stmt-card-amount">{html.escape(_fmt_brl(debit_total))}</div>
                                  <div class="stmt-card-badges">
                                    <span style="background:#DCFCE7;color:#166534;padding:0.15rem 0.5rem;border-radius:999px;font-size:0.7rem;font-weight:700;">✓ OK</span>
                                  </div>
                                </div>
                                """,
                                unsafe_allow_html=True,
                            )
                            if st.button("Ver", key=f"debit_details_{ref_year}_{ref_month}", width="stretch"):
                                show_debit_detail_dialog()

                    # Card de Contas da Casa
                    with _next_col():
                        with st.container(border=True):
                            st.markdown(
                                f"""
                                <div class="stmt-card-wrapper stmt-card-wrapper">
                                  <div>
                                    <div class="stmt-card-title">🏠 Contas Casa</div>
                                    <div class="stmt-card-sub">Mês: {start.strftime('%m/%Y')}</div>
                                  </div>
                                  <div class="stmt-card-amount">{html.escape(_fmt_brl(house_total))}</div>
                                  <div class="stmt-card-badges">
                                    <span style="background:#DBEAFE;color:#1E40AF;padding:0.15rem 0.5rem;border-radius:999px;font-size:0.7rem;font-weight:700;">Casa</span>
                                  </div>
                                </div>
                                """,
                                unsafe_allow_html=True,
                            )
                            if st.button("Ver", key=f"house_details_{ref_year}_{ref_month}", width="stretch"):
                                show_house_detail_dialog()
                    
                    # Cards de faturas com formulário inline
                    for i, r in grouped.iterrows():
                        account = str(r.get("account") or "").strip() or "(sem cartão)"
                        due_dt = _to_date(r.get("statement_due_date"))
                        if due_dt is None:
                            continue

                        meta_card_source = (
                            card_id_by_name.get(account)
                            or card_id_by_norm_name.get(normalize_str(account))
                            or normalize_str(account)
                            or "unknown"
                        )
                        key_base = f"{normalize_str(meta_card_source)}_{due_dt.isoformat()}"
                        meta = pf_db.get_credit_card_statement_meta(
                            conn, card_source=meta_card_source, statement_due_date=due_dt
                        )

                        # Fechamento:
                        # 1) Se existir no meta (calendário), respeitar.
                        # 2) Senão, calcular pelo config do cartão (dia fixo).
                        # 3) Fallback: usar o que veio do banco / heurística antiga.
                        closing_dt = None
                        if meta:
                            meta_close = _to_date(meta.get("statement_closing_date"))
                            if meta_close is not None:
                                closing_dt = meta_close
                        if closing_dt is None:
                            card_cfg = cards.get(meta_card_source)
                            if card_cfg is not None:
                                try:
                                    closing_dt = _default_statement_closing_date(due_dt, closing_day=int(card_cfg.closing_day))
                                except Exception:  # noqa: BLE001
                                    closing_dt = None
                        if closing_dt is None:
                            closing_dt = _to_date(r.get("closing_date"))
                        if closing_dt is None:
                            try:
                                from datetime import timedelta

                                closing_dt = due_dt - timedelta(days=7)
                            except Exception:  # noqa: BLE001
                                closing_dt = None

                        paid_dt = _to_date(meta.get("paid_date")) if meta else None
                        paid_flag = bool(int(meta.get("is_paid") or 0)) if meta else False
                        is_closed_now = bool(closing_dt is not None and today >= closing_dt)
                        is_paid_now = bool(paid_flag and paid_dt is not None)
                        total_fatura = float(r.get("total_fatura") or 0.0)

                        @st.dialog("💳 Gerenciar Fatura")
                        def _manage_statement_dialog(
                            account_name: str,
                            due_date: date,
                            meta_source: str,
                            closing_date: date | None,
                        ) -> None:
                            st.caption(f"Cartão: {account_name}")
                            st.caption(
                                f"Venc: {due_date.strftime('%d/%m/%Y')} • "
                                f"Fecha: {(closing_date.strftime('%d/%m/%Y') if closing_date else '—')}"
                            )

                            meta_now = pf_db.get_credit_card_statement_meta(
                                conn, card_source=meta_source, statement_due_date=due_date
                            )
                            paid_dt_now = _to_date(meta_now.get("paid_date")) if meta_now else None
                            paid_flag_now = bool(int(meta_now.get("is_paid") or 0)) if meta_now else False

                            # Não usamos `st.form` aqui porque widgets dentro de form não atualizam
                            # o estado (disabled/enabled) até o submit.
                            base_key = f"{normalize_str(meta_source)}_{due_date.isoformat()}"
                            paid_key = f"stmt_paid_{base_key}"
                            paid_date_key = f"stmt_paid_date_{base_key}"
                            save_key = f"stmt_paid_save_{base_key}"

                            default_paid = paid_dt_now or due_date
                            if paid_key not in st.session_state:
                                st.session_state[paid_key] = bool(paid_flag_now)
                            if paid_date_key not in st.session_state:
                                st.session_state[paid_date_key] = default_paid

                            paid_flag_in = st.toggle("Paga", key=paid_key)
                            if paid_date_key in st.session_state:
                                paid_in = st.date_input(
                                    "Data pagamento",
                                    key=paid_date_key,
                                    disabled=not bool(paid_flag_in),
                                )
                            else:
                                paid_in = st.date_input(
                                    "Data pagamento",
                                    key=paid_date_key,
                                    value=default_paid,
                                    disabled=not bool(paid_flag_in),
                                )

                            if st.button("Salvar", key=save_key, width="stretch"):
                                pf_db.upsert_credit_card_statement_meta(
                                    conn,
                                    card_source=meta_source,
                                    statement_due_date=due_date,
                                    statement_closing_date=closing_date,
                                    is_closed=bool(closing_date is not None and today >= closing_date),
                                    is_paid=bool(paid_flag_in),
                                    paid_date=(paid_in if bool(paid_flag_in) else None),
                                )
                                try:
                                    pf_excel_unified.update_credit_card_status(
                                        unified_xlsx,
                                        account=account_name,
                                        due_date=due_date,
                                        status=("Pago" if bool(paid_flag_in) else "Em aberto"),
                                    )
                                except Exception:
                                    pass
                                st.rerun()

                            st.markdown("---")
                            _render_statement_details(account_name, due_date)

                        with _next_col():
                            with st.container(border=True):
                                fech_label = closing_dt.strftime('%d/%m/%Y') if closing_dt else "—"
                                pay_badge = (
                                    '<span style="background:#DCFCE7;color:#166534;padding:0.15rem 0.5rem;border-radius:999px;font-size:0.7rem;font-weight:700;">Paga</span>'
                                    if is_paid_now
                                    else '<span style="background:#FEF3C7;color:#92400E;padding:0.15rem 0.5rem;border-radius:999px;font-size:0.7rem;font-weight:700;">Pendente</span>'
                                )
                                close_badge = (
                                    '<span style="background:#DBEAFE;color:#1E40AF;padding:0.15rem 0.5rem;border-radius:999px;font-size:0.7rem;font-weight:700;">Fechada</span>'
                                    if is_closed_now
                                    else '<span style="background:#F3F4F6;color:#6B7280;padding:0.15rem 0.5rem;border-radius:999px;font-size:0.7rem;font-weight:700;">Aberta</span>'
                                )
                                st.markdown(
                                    f"""
                                    <div class="stmt-card-wrapper stmt-card-wrapper">
                                      <div>
                                        <div class="stmt-card-title">{html.escape(account)}</div>
                                        <div class="stmt-card-sub">Venc: {due_dt.strftime('%d/%m/%Y')}</div>
                                        <div class="stmt-card-sub">Fecha: {fech_label}</div>
                                      </div>
                                      <div class="stmt-card-amount">{html.escape(_fmt_brl(total_fatura))}</div>
                                      <div class="stmt-card-badges">{pay_badge} {close_badge}</div>
                                    </div>
                                    """,
                                    unsafe_allow_html=True,
                                )

                                if st.button(
                                    "Gerenciar",
                                    key=f"btn_{key_base}",
                                    help="Marcar como paga e informar a data de pagamento",
                                    width="stretch",
                                ):
                                    _manage_statement_dialog(account, due_dt, meta_card_source, closing_dt)

        

        # Resumo por categoria (do mês do acerto) — importante para ver o que está sendo considerado.
        if not df_despesas_acerto.empty:
            st.markdown("#### Por Categoria (acerto)")
            cat_view = df_despesas_acerto.copy()
            if "reimbursable" in cat_view.columns:
                cat_view = cat_view[cat_view["reimbursable"] != 1].copy()
            if "category" in cat_view.columns:
                cat_view["categoria"] = (
                    cat_view["category"].fillna("").astype(str).replace("", pd.NA).fillna("(sem categoria)")
                )
            else:
                cat_view["categoria"] = "(sem categoria)"
            if "valor" in cat_view.columns:
                by_cat = cat_view.groupby("categoria")["valor"].sum().sort_values(ascending=False).reset_index()
            else:
                by_cat = pd.DataFrame(columns=["categoria", "valor"])
            if not by_cat.empty:
                by_cat.columns = ["Categoria", "Valor"]
                by_cat["Valor"] = by_cat["Valor"].apply(_fmt_brl)
                st.dataframe(by_cat, hide_index=True, width="stretch")

        # Orçamento vs Realizado por Categoria (clicável)
        if budgets and not df.empty:
            st.markdown("---")
            st.markdown("### 🎯 Orçamento por Categoria")
            st.caption("Clique em uma categoria para ver os gastos detalhados")
            exp = df[df["amount"] < 0].copy()
            if not exp.empty:
                exp["valor"] = -exp["amount"]
                derived = exp.apply(
                    lambda r: pd.Series(_derive_category_subcategory(r), index=["cat", "sub"]),
                    axis=1,
                )
                exp = exp.join(derived)
                exp["categoria"] = exp["cat"].replace("", pd.NA).fillna("(sem categoria)")
                by_cat = exp.groupby("categoria")["valor"].sum().to_dict()
                
                st.session_state["budget_expenses_data"] = exp
                
                # Mostrar em grid de 3 colunas com cards clicáveis
                budget_items = list(budgets.items())
                cols = st.columns(3)

                for idx, (cat, limit) in enumerate(budget_items):
                    spent = float(by_cat.get(cat, 0.0))
                    limit_f = float(limit) if limit else 0.0
                    pct_raw = (spent / limit_f * 100) if limit_f > 0 else 0.0
                    pct = min(pct_raw, 100) if limit_f > 0 else 0.0
                    remaining = limit_f - spent

                    with cols[idx % 3]:
                        # Determinar cor baseado no percentual (farol)
                        if pct_raw >= 90:
                            dot_color = "#f5576c"
                            status_text = "⚠️ Atenção"
                        elif pct_raw >= 70:
                            dot_color = "#f5a623"
                            status_text = "🔔 Alerta"
                        else:
                            dot_color = "#38ef7d"
                            status_text = "✅ OK"

                        rest_label = (
                            f"Restam {_fmt_brl(max(remaining, 0))}"
                            if remaining >= 0
                            else f"Estourou {_fmt_brl(abs(remaining))}"
                        )
                        
                        safe_cat = str(cat).replace(" ", "_").replace("/", "_").replace("(", "").replace(")", "")

                        budget_card_html = f"""
                        <div class="budget-card-wrapper" id="budget-wrap-{safe_cat}">
                            <div class="budget-card">
                                <div class="budget-card-header">
                                    <div class="budget-card-title">
                                        <span class="budget-dot" style="background: {dot_color};"></span>
                                        <span>{html.escape(str(cat))}</span>
                                    </div>
                                    <span class="budget-card-arrow">→</span>
                                </div>
                                <div class="budget-meta">
                                    <span style="font-size: 1.1rem; font-weight: 700;">{html.escape(_fmt_brl(spent))}</span>
                                    <span style="opacity: 0.7;">de {html.escape(_fmt_brl(limit_f))}</span>
                                </div>
                                <div class="budget-progress" style="background: rgba(255,255,255,0.18); height: 8px; border-radius: 4px; margin-top: 0.6rem;">
                                    <div class="budget-progress-fill" style="width: {pct:.1f}%; background: {dot_color}; height: 100%; border-radius: 4px;"></div>
                                </div>
                                <div class="budget-submeta">
                                    <span>{min(pct_raw, 100):.0f}% usado</span>
                                    <span>{html.escape(rest_label)}</span>
                                </div>
                            </div>
                        </div>
                        """
                        st.markdown(budget_card_html, unsafe_allow_html=True)
                        
                        # Botão invisível sobreposto ao card
                        if st.button("​", key=f"budget_btn_{safe_cat}", width="stretch"):
                            st.session_state["show_category_detail"] = cat

                # Dialog para mostrar detalhes da categoria selecionada
                @st.dialog("📊 Detalhes da Categoria")
                def show_category_detail_dialog(category_name: str):
                    exp_data = st.session_state.get("budget_expenses_data", pd.DataFrame())
                    if exp_data.empty:
                        st.info("Sem dados disponíveis")
                        return

                    cat_data = exp_data[exp_data["categoria"] == category_name].copy()
                    if cat_data.empty:
                        st.info(f"Nenhum gasto em '{category_name}'")
                        return

                    total = float(cat_data["valor"].sum())
                    limit_val = float(budgets.get(category_name, 0) or 0)

                    st.markdown(f"### {html.escape(str(category_name))}")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Total Gasto", _fmt_brl(total))
                    with col2:
                        if limit_val > 0:
                            st.metric("Orçamento", _fmt_brl(limit_val), delta=f"Restam {_fmt_brl(max(limit_val - total, 0))}")

                    st.markdown("---")
                    st.markdown("#### Transações")

                    df_show_cols = [c for c in ["txn_date", "description", "subcategory", "account", "valor"] if c in cat_data.columns]
                    df_show = cat_data[df_show_cols].copy()
                    df_show = df_show.rename(
                        columns={
                            "txn_date": "Data",
                            "description": "Descrição",
                            "subcategory": "Subcategoria",
                            "account": "Cartão/Conta",
                            "valor": "Valor",
                        }
                    )
                    if "Valor" in df_show.columns:
                        df_show["Valor"] = df_show["Valor"].apply(_fmt_brl)
                    if "Data" in df_show.columns:
                        df_show = df_show.sort_values("Data", ascending=False)
                    st.dataframe(df_show, hide_index=True, width="stretch")

                    if "subcategory" in cat_data.columns:
                        st.markdown("#### Por Subcategoria")
                        by_sub = cat_data.groupby("subcategory")["valor"].sum().sort_values(ascending=False).reset_index()
                        by_sub.columns = ["Subcategoria", "Valor"]
                        by_sub["Valor"] = by_sub["Valor"].apply(_fmt_brl)
                        st.dataframe(by_sub, hide_index=True, width="stretch")

                # Mostrar dialog se categoria selecionada
                if st.session_state.get("show_category_detail"):
                    show_category_detail_dialog(st.session_state["show_category_detail"])
                    st.session_state["show_category_detail"] = None
        
        # Pendências compactas
        pending_cc = 0
        pending_exp = 0
        if not df.empty:
            cat_s = df["category"].fillna("").astype(str)
            sub_s = df["subcategory"].fillna("").astype(str)
            pending_cc = int(((df["payment_method"] == "credit_card") & ((cat_s == "") | (sub_s == ""))).sum())
            pending_exp = int(((df["amount"] < 0) & ((cat_s == "") | (sub_s == ""))).sum())

        if pending_exp > 0:
            st.warning(f"⚠️ **{pending_exp}** despesa(s) sem categoria — Sincronize na barra lateral")

        # Card de Reembolsáveis
        st.markdown("---")
        if not df.empty:
            reimb_exp = df[(df["reimbursable"] == 1) & (df["amount"] < 0)].copy()
            cat_s = df["category"].fillna("").astype(str) if "category" in df.columns else pd.Series([], dtype=str)
            sub_s = df["subcategory"].fillna("").astype(str) if "subcategory" in df.columns else pd.Series([], dtype=str)
            reimb_inc = df[
                (df["payment_method"] == "income") & ((cat_s == "Reembolso") | (sub_s == "Reembolso"))
            ].copy()
            total_exp = float((-reimb_exp["amount"].sum())) if not reimb_exp.empty else 0.0
            total_inc = float(reimb_inc["amount"].sum()) if not reimb_inc.empty else 0.0
            saldo = total_exp - total_inc
            
            # Determinar cores baseado no saldo
            saldo_class = "reimb-positive" if saldo <= 0 else ""
            
            reimb_html = f"""
            <div class="reimb-card">
                <div class="reimb-card-header">
                    <div class="reimb-card-title">💸 Reembolsáveis</div>
                </div>
                <div class="reimb-metrics">
                    <div class="reimb-metric">
                        <div class="reimb-metric-label">A Receber</div>
                        <div class="reimb-metric-value">{html.escape(_fmt_brl(total_exp))}</div>
                    </div>
                    <div class="reimb-metric">
                        <div class="reimb-metric-label">Recebido</div>
                        <div class="reimb-metric-value reimb-positive">{html.escape(_fmt_brl(total_inc))}</div>
                    </div>
                    <div class="reimb-metric">
                        <div class="reimb-metric-label">Saldo</div>
                        <div class="reimb-metric-value {saldo_class}">{html.escape(_fmt_brl(saldo))}</div>
                    </div>
                </div>
            </div>
            """
            st.markdown(reimb_html, unsafe_allow_html=True)
            
            # Detalhes em expander
            if not reimb_exp.empty:
                with st.expander("📋 Detalhes dos reembolsáveis", expanded=False):
                    cols_show = [c for c in ["txn_date", "description", "category", "subcategory", "amount", "reference"] if c in reimb_exp.columns]
                    show_df = reimb_exp[cols_show].copy()
                    show_df["amount"] = show_df["amount"].abs()
                    show_df = show_df.rename(columns={
                        "txn_date": "Data",
                        "description": "Descrição",
                        "category": "Categoria",
                        "subcategory": "Subcategoria",
                        "amount": "Valor",
                        "reference": "Referência",
                    })
                    if "Valor" in show_df.columns:
                        show_df["Valor"] = show_df["Valor"].apply(_fmt_brl)
                    if "Data" in show_df.columns:
                        show_df = show_df.sort_values("Data", ascending=False)
                    st.dataframe(show_df, hide_index=True, width="stretch")

    elif nav == "Gerenciamento de Cartões":
        st.subheader("💳 Gerenciamento de Cartões")
        st.caption(
            "Defina o calendário de **vencimento** e **fechamento** por mês. "
            "O status **Aberta/Fechada** usa o *fechamento*; **Pendente/Paga** depende apenas do pagamento informado no card."
        )

        month_start = start
        month_end = end

        # Montar uma linha por cartão para o mês selecionado (vencimento no mês).
        rows: list[dict] = []
        for c in cards.values():
            due_dt: date | None = None
            closing_dt: date | None = None

            # 1) Preferir calendário salvo (meta) dentro do mês.
            meta_in_month = conn.execute(
                """
                SELECT statement_due_date, statement_closing_date
                FROM credit_card_statements
                WHERE card_source = ?
                  AND statement_due_date >= ?
                  AND statement_due_date <= ?
                ORDER BY statement_due_date ASC
                LIMIT 1
                """,
                (str(c.id), month_start.isoformat(), month_end.isoformat()),
            ).fetchone()
            if meta_in_month is not None:
                due_dt = _to_date(meta_in_month["statement_due_date"])
                closing_dt = _to_date(meta_in_month["statement_closing_date"])

            # 2) Senão, tentar inferir pelo que já existe no banco (transações do mês).
            if due_dt is None:
                txn_due = conn.execute(
                    """
                    SELECT statement_due_date
                    FROM transactions
                    WHERE payment_method = 'credit_card'
                      AND account = ?
                      AND statement_due_date IS NOT NULL
                      AND statement_due_date >= ?
                      AND statement_due_date <= ?
                    ORDER BY statement_due_date ASC
                    LIMIT 1
                    """,
                    (str(c.name), month_start.isoformat(), month_end.isoformat()),
                ).fetchone()
                if txn_due is not None:
                    due_dt = _to_date(txn_due["statement_due_date"])

            # 3) Fallback: usar dia do vencimento do config.
            if due_dt is None:
                due_dt = clamp_day(month_start.year, month_start.month, int(c.due_day))

            # Fechamento default pelo config do cartão (pode ser sobrescrito no editor)
            if closing_dt is None and due_dt is not None:
                closing_dt = _default_statement_closing_date(due_dt, closing_day=int(c.closing_day))

            rows.append(
                {
                    "id": str(c.id),
                    "Cartão": str(c.name),
                    "Vencimento": due_dt,
                    "Fechamento": closing_dt,
                }
            )

        sched_df = pd.DataFrame(rows).set_index("id")

        edited = st.data_editor(
            sched_df,
            hide_index=True,
            width="stretch",
            disabled=["Cartão"],
            column_config={
                "Vencimento": st.column_config.DateColumn("Vencimento", help="Data de vencimento da fatura (mês selecionado)."),
                "Fechamento": st.column_config.DateColumn("Fechamento", help="Data de fechamento da fatura (pode ser no mês anterior)."),
            },
        )

        if st.button("Salvar calendário do mês", type="primary", width="stretch"):
            updated = 0
            for card_id, r in edited.iterrows():
                due_dt = r.get("Vencimento")
                closing_dt = r.get("Fechamento")
                due_date = due_dt if isinstance(due_dt, date) else _to_date(due_dt)
                closing_date = closing_dt if isinstance(closing_dt, date) else _to_date(closing_dt)

                if due_date is None:
                    continue

                meta = pf_db.get_credit_card_statement_meta(conn, card_source=str(card_id), statement_due_date=due_date)
                paid_dt = _to_date(meta.get("paid_date")) if meta else None
                paid_flag = bool(int(meta.get("is_paid") or 0)) if meta else False

                pf_db.upsert_credit_card_statement_meta(
                    conn,
                    card_source=str(card_id),
                    statement_due_date=due_date,
                    statement_closing_date=closing_date,
                    is_closed=bool(closing_date is not None and today >= closing_date),
                    is_paid=paid_flag,
                    paid_date=paid_dt if paid_flag else None,
                )
                updated += 1

            st.success(f"✅ Calendário atualizado ({updated} cartão(ões)).")
            st.rerun()

    elif nav == "Investimentos":
        st.subheader("📈 Investimentos")

        # Carrega dados dos investimentos
        monthly_all = pf_db.load_investment_monthly_df(
            conn,
            start_year=2020,
            start_month=1,
            end_year=today.year,
            end_month=today.month,
        )
        
        inv_df = pf_db.load_investments_df(conn)
        
        allowed_banks = ["XP", "C6", "Mercado Pago", "Nubank"]

        def _infer_bank(inv_row: pd.Series) -> str:
            part = str(inv_row.get("partition") or "").lower()
            issuer = str(inv_row.get("issuer") or "").lower()
            blob = f"{part} {issuer}"
            if "xp" in blob:
                return "XP"
            if "c6" in blob:
                return "C6"
            if "mercado" in blob:
                return "Mercado Pago"
            if "nubank" in blob:
                return "Nubank"
            return str(inv_row.get("issuer") or "").strip()

        # =========
        # Editor estilo "print": linhas = investimentos; colunas = meses
        # =========
        pt_months = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"]

        def _month_iter(start_ym: tuple[int, int], end_ym: tuple[int, int]) -> list[tuple[int, int]]:
            y, m = int(start_ym[0]), int(start_ym[1])
            y2, m2 = int(end_ym[0]), int(end_ym[1])
            out: list[tuple[int, int]] = []
            cur = y * 12 + m
            endp = y2 * 12 + m2
            while cur <= endp:
                yy = (cur - 1) // 12
                mm = (cur - 1) % 12 + 1
                out.append((yy, mm))
                cur += 1
            return out

        c1, c2 = st.columns([1, 1])
        with c1:
            start_month_dt = st.date_input("Início (mês)", value=date(2025, 2, 1), key="inv_start_month")
        with c2:
            end_month_dt = st.date_input("Fim (mês)", value=date(today.year, today.month, 1), key="inv_end_month")

        start_ym = (int(start_month_dt.year), int(start_month_dt.month))
        end_ym = (int(end_month_dt.year), int(end_month_dt.month))
        if (start_ym[0] * 12 + start_ym[1]) > (end_ym[0] * 12 + end_ym[1]):
            start_ym, end_ym = end_ym, start_ym

        months = _month_iter(start_ym, end_ym)
        month_labels = [f"{pt_months[m-1]}/{y}" for (y, m) in months]
        month_label_by_ym = {ym: lab for ym, lab in zip(months, month_labels, strict=True)}

        # Inventário (investments)
        inv_map: dict[int, dict[str, object]] = {}
        if not inv_df.empty:
            for _, inv in inv_df.iterrows():
                inv_map[int(inv["id"])] = {
                    "banco": _infer_bank(inv),
                    "nome": str(inv.get("product") or ""),
                    "vencimento": inv.get("maturity_date"),
                }

        inv_rows: list[dict[str, object]] = []
        for inv_id, info in sorted(inv_map.items(), key=lambda kv: (str(kv[1].get("banco") or ""), str(kv[1].get("nome") or ""))):
            row: dict[str, object] = {
                "ID": int(inv_id),
                "Banco": str(info.get("banco") or ""),
                "Nome": str(info.get("nome") or ""),
                "Vencimento": info.get("vencimento"),
            }
            for lab in month_labels:
                row[lab] = None
            inv_rows.append(row)

        # Preenche valores mensais (balance) no período selecionado
        if not monthly_all.empty:
            period_min = start_ym[0] * 12 + start_ym[1]
            period_max = end_ym[0] * 12 + end_ym[1]
            mdf = monthly_all.copy()
            mdf["period"] = mdf["year"].astype(int) * 12 + mdf["month"].astype(int)
            mdf = mdf[(mdf["period"] >= period_min) & (mdf["period"] <= period_max)].copy()

            by_id = {int(r["ID"]): r for r in inv_rows}
            for _, r in mdf.iterrows():
                inv_id = int(r.get("investment_id"))
                y = int(r.get("year"))
                m = int(r.get("month"))
                lab = month_label_by_ym.get((y, m))
                if not lab:
                    continue
                tgt = by_id.get(inv_id)
                if tgt is None:
                    continue
                bal = r.get("balance")
                tgt[lab] = None if pd.isna(bal) else float(bal)

        if not inv_rows:
            st.info("Nenhum investimento cadastrado ainda. Adicione linhas na tabela abaixo.")

        matrix_df = pd.DataFrame(inv_rows) if inv_rows else pd.DataFrame(columns=["ID", "Banco", "Nome", "Vencimento", *month_labels])

        # Totais por mês (para visualizar rápido, como no print)
        totals_row = {"Banco": "", "Nome": "Saldo Total", "Vencimento": None}
        for lab in month_labels:
            try:
                totals_row[lab] = float(pd.to_numeric(matrix_df.get(lab), errors="coerce").fillna(0).sum())
            except Exception:  # noqa: BLE001
                totals_row[lab] = 0.0

        # rendimento mensal (delta do total mês a mês)
        perf_row = {"Banco": "", "Nome": "Rendimento (Δ)", "Vencimento": None}
        prev_total = None
        for (y, m), lab in zip(months, month_labels, strict=True):
            total = float(totals_row.get(lab) or 0.0)
            if prev_total is None:
                perf_row[lab] = None
            else:
                perf_row[lab] = total - prev_total
            prev_total = total

        totals_view = pd.DataFrame([totals_row, perf_row])
        st.dataframe(totals_view, hide_index=True, width="stretch")

        col_cfg: dict[str, object] = {
            "ID": st.column_config.NumberColumn("ID", disabled=True),
            "Banco": st.column_config.SelectboxColumn("Banco", options=allowed_banks, required=True),
            "Nome": st.column_config.TextColumn("Nome", required=True, help="Ex.: CDB Liq Diária, LCI, Tesouro..."),
            "Vencimento": st.column_config.DateColumn("Vencimento", format="DD/MM/YYYY"),
        }
        for lab in month_labels:
            col_cfg[lab] = st.column_config.NumberColumn(lab, format="R$ %.2f")

        edited_matrix = st.data_editor(
            matrix_df,
            num_rows="dynamic",
            hide_index=True,
            width="stretch",
            column_config=col_cfg,
            key="inv_matrix",
        )

        if st.button("💾 Salvar tabela", type="primary"):
            updated_cells = 0
            for _, row in edited_matrix.iterrows():
                inv_id_raw = row.get("ID")
                inv_id = None if pd.isna(inv_id_raw) else int(inv_id_raw)
                banco = str(row.get("Banco") or "").strip()
                nome = str(row.get("Nome") or "").strip()
                if not banco or not nome:
                    continue
                if banco not in allowed_banks:
                    continue

                venc = row.get("Vencimento")
                venc_dt = venc if isinstance(venc, date) else _to_date(venc)

                inv_id = pf_db.upsert_investment(
                    conn,
                    investment_id=inv_id,
                    partition=banco,
                    issuer=banco,
                    product=nome,
                    maturity_date=venc_dt,
                )

                for (y, m) in months:
                    lab = month_label_by_ym[(y, m)]
                    v = row.get(lab)
                    # vazio -> remove (mantém a matriz consistente)
                    if v is None or (isinstance(v, float) and pd.isna(v)):
                        conn.execute(
                            "DELETE FROM investment_monthly WHERE investment_id = ? AND year = ? AND month = ?",
                            (int(inv_id), int(y), int(m)),
                        )
                        continue
                    try:
                        bal = float(v)
                    except Exception:  # noqa: BLE001
                        continue
                    checked = date(int(y), int(m), monthrange(int(y), int(m))[1])
                    pf_db.upsert_investment_monthly(
                        conn,
                        investment_id=int(inv_id),
                        year=int(y),
                        month=int(m),
                        applied=None,
                        balance=bal,
                        checked_at=checked,
                    )
                    updated_cells += 1

            conn.commit()
            st.success(f"✅ Salvo ({updated_cells} célula(s) atualizada(s)).")
            st.rerun()
        
        # Gráfico de evolução
        st.markdown("---")
        st.markdown("### 📊 Evolução Mensal")

        # usa a própria matriz para montar o chart (no período selecionado)
        if edited_matrix.empty if "edited_matrix" in locals() else matrix_df.empty:
            st.info("Adicione investimentos para visualizar o gráfico de evolução.")
        else:
            chart_data = []
            base = edited_matrix if "edited_matrix" in locals() else matrix_df
            for _, r in base.iterrows():
                nome_ativo = str(r.get("Nome") or "").strip() or "(sem nome)"
                for (y, m) in months:
                    lab = month_label_by_ym[(y, m)]
                    v = r.get(lab)
                    if v is None or (isinstance(v, float) and pd.isna(v)):
                        continue
                    try:
                        valor_f = float(v)
                    except Exception:  # noqa: BLE001
                        continue
                    chart_data.append({"Data": date(int(y), int(m), 1), "Nome": nome_ativo, "Valor": valor_f})

            if chart_data:
                chart_df = pd.DataFrame(chart_data)
                chart_df = chart_df.groupby(["Data", "Nome"], as_index=False)["Valor"].sum()

                chart_df["Mes"] = pd.to_datetime(chart_df["Data"])
                chart_df["MesLabel"] = chart_df["Mes"].dt.strftime("%b/%Y")

                totals_df = chart_df.groupby(["Mes", "MesLabel"], as_index=False)["Valor"].sum()
                totals_df = totals_df.rename(columns={"Valor": "Total"})

                bars = (
                    alt.Chart(chart_df)
                    .mark_bar()
                    .encode(
                        x=alt.X(
                            "MesLabel:O",
                            title="Mês",
                            sort=alt.SortField(field="Mes", order="ascending"),
                            axis=alt.Axis(labelAngle=0, labelOverlap="greedy"),
                        ),
                        y=alt.Y("sum(Valor):Q", title="Total (R$)"),
                        color=alt.Color("Nome:N", title="Ativo"),
                        tooltip=[
                            alt.Tooltip("MesLabel:O", title="Mês"),
                            alt.Tooltip("Nome:N", title="Ativo"),
                            alt.Tooltip("sum(Valor):Q", title="Valor", format=",.2f"),
                        ],
                    )
                )

                total_line = (
                    alt.Chart(totals_df)
                    .mark_line(color="#E5E7EB", strokeWidth=3, point=alt.OverlayMarkDef(size=60))
                    .encode(
                        x=alt.X("MesLabel:O", sort=alt.SortField(field="Mes", order="ascending")),
                        y=alt.Y("Total:Q"),
                        tooltip=[
                            alt.Tooltip("MesLabel:O", title="Mês"),
                            alt.Tooltip("Total:Q", title="Total", format=",.2f"),
                        ],
                    )
                )

                st.altair_chart((bars + total_line).properties(height=420), use_container_width=True)
    elif nav == "Transações":
        st.subheader("💰 Acerto Mensal com Aline")
        
        # Meses para seleção amigável
        meses_nomes = ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", 
                       "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]
        
        # Gerar opções de meses recentes (últimos 6 meses)
        opcoes_acerto = []
        for i in range(6):
            m = today.month - i
            y = today.year
            if m <= 0:
                m += 12
                y -= 1
            opcoes_acerto.append(f"{meses_nomes[m-1]} {y}")
        
        acerto_selecionado = st.selectbox(
            "📅 Selecione o mês do acerto",
            opcoes_acerto,
            index=0,
            help="Mês em que você faz o acerto (início do mês)"
        )
        
        # Parsear seleção
        partes = acerto_selecionado.split()
        ref_month = meses_nomes.index(partes[0]) + 1
        ref_year = int(partes[1])
        
        # Mês anterior (para débitos e faturas 2ª quinzena)
        prev_month = ref_month - 1 if ref_month > 1 else 12
        prev_year = ref_year if ref_month > 1 else ref_year - 1
        
        st.caption(f"""
        **O que entra no acerto de {meses_nomes[ref_month-1]}:**
        - 💳 Cartões com vencimento **até dia 10** entram no mês do acerto (ex: XP 05, Nubank Aline 05)
        - 💳 Cartões com vencimento **após dia 10** entram com a fatura do mês anterior (ex: Nubank 19, C6 20, Mercado Pago 17)
        - 💵 Débitos/PIX de **{meses_nomes[prev_month-1]}**
        - 🏠 **Contas da Casa pagas em {meses_nomes[ref_month-1]}** (podem ter \"Mês Referência\" do mês anterior)
        """)
        
        # Período de faturas definido por cartão (regra fixa):
        # - XP e Nubank Aline → mês do acerto
        # - Demais cartões → mês anterior
        cartao_prev_start = date(prev_year, prev_month, 1)
        cartao_prev_end = date(prev_year, prev_month, monthrange(prev_year, prev_month)[1])
        cartao_curr_start = date(ref_year, ref_month, 1)
        cartao_curr_end = date(ref_year, ref_month, monthrange(ref_year, ref_month)[1])
        acerto_period = (ref_year * 12) + ref_month
        
        # Período para débitos: mês anterior inteiro
        debito_start = date(prev_year, prev_month, 1)
        debito_end = date(prev_year, prev_month, monthrange(prev_year, prev_month)[1])
        
        # Buscar faturas de cartão (cash_date = vencimento)
        df_cartoes_all = pf_queries.load_transactions_df(conn, start=cartao_prev_start, end=cartao_curr_end)
        df_cartoes_all = (
            df_cartoes_all[df_cartoes_all["payment_method"] == "credit_card"].copy()
            if not df_cartoes_all.empty
            else df_cartoes_all
        )
        if not df_cartoes_all.empty:
            offset_by_account = {
                c.name: (0 if c.id in ("xp", "nubank_aline") else -1)
                for c in cards.values()
            }
            due_col = "statement_due_date" if "statement_due_date" in df_cartoes_all.columns else "cash_date"
            due_dt = pd.to_datetime(df_cartoes_all[due_col], errors="coerce")
            due_period = (due_dt.dt.year * 12) + due_dt.dt.month
            offset = df_cartoes_all["account"].map(offset_by_account)
            fallback_offset = (due_dt.dt.day > 10).astype(int) * -1
            offset = offset.fillna(fallback_offset)
            expected_period = acerto_period + offset.astype(int)
            df_cartoes = df_cartoes_all[due_period == expected_period].copy()
        else:
            df_cartoes = df_cartoes_all
        
        # Buscar débitos do mês anterior por data da transação (txn_date)
        df_debitos = pf_queries.load_transactions_df_by_txn_date(conn, start=debito_start, end=debito_end)
        df_debitos = df_debitos[~df_debitos["payment_method"].isin(["credit_card", "household", "income"])].copy() if not df_debitos.empty else df_debitos
        
        # Buscar contas da casa do mês CORRENTE (não do anterior!)
        casa_start = date(ref_year, ref_month, 1)
        casa_end = date(ref_year, ref_month, monthrange(ref_year, ref_month)[1])
        df_casa = pf_queries.load_transactions_df(conn, start=casa_start, end=casa_end)
        df_casa = df_casa[df_casa["payment_method"] == "household"].copy() if not df_casa.empty else df_casa
        
        # Combinar cartões e débitos
        df_acerto = pd.concat([df_cartoes, df_debitos], ignore_index=True)
        
        if df_acerto.empty and df_casa.empty:
            st.info("Sem transações no período selecionado.")
        else:
            # Usar o módulo de reconciliação
            result = pf_recon.calculate_reconciliation(
                df_acerto,
                reference_month=ref_month,
                reference_year=ref_year,
                include_household=True,
                df_household=df_casa,
            )
            details_df = pd.DataFrame(result.detalhes)
            
            # Resumo visual
            st.markdown("---")
            st.markdown(f"### 📊 Resumo do Acerto de {meses_nomes[ref_month-1]}/{ref_year}")
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("💸 Total Despesas", f"R$ {result.total_despesas:,.2f}")
            with col2:
                st.metric("🔄 Dividir (÷2)", f"R$ {result.total_dividir:,.2f}")
            with col3:
                st.metric("🏠 Contas Casa", f"R$ {result.total_contas_casa:,.2f}")
            with col4:
                if result.qtd_sem_categoria > 0:
                    st.metric("⚠️ Sem Categoria", f"R$ {result.sem_categoria:,.2f}", delta=f"{result.qtd_sem_categoria} itens", delta_color="inverse")
                else:
                    st.metric("✅ Categorizado", "OK")
            
            st.markdown("---")
            st.markdown("### 👤 Gastos Individuais (não dividem)")
            col_r, col_a = st.columns(2)
            with col_r:
                st.metric("🧔 Renan", f"R$ {result.total_renan_individual:,.2f}")
            with col_a:
                st.metric("👩 Aline", f"R$ {result.total_aline_individual:,.2f}")
            
            # Quem pagou o quê
            st.markdown("---")
            st.markdown("### 💳 Quem Pagou")
            col_p1, col_p2, col_p3 = st.columns(3)
            with col_p1:
                st.markdown(f"""
                <div style="background: #667eea; padding: 1rem; border-radius: 10px; color: white; text-align: center;">
                    <div style="font-size: 0.9rem;">🧔 Renan pagou</div>
                    <div style="font-size: 1.2rem; font-weight: bold;">R$ {result.renan_pagou_dividir + result.renan_pagou_casa:,.2f}</div>
                    <div style="font-size: 0.8rem; opacity: 0.9;">Dividíveis: R$ {result.renan_pagou_dividir:,.2f}</div>
                    <div style="font-size: 0.8rem; opacity: 0.9;">Casa: R$ {result.renan_pagou_casa:,.2f}</div>
                </div>
                """, unsafe_allow_html=True)
            with col_p2:
                st.markdown(f"""
                <div style="background: #f093fb; padding: 1rem; border-radius: 10px; color: white; text-align: center;">
                    <div style="font-size: 0.9rem;">👩 Aline pagou</div>
                    <div style="font-size: 1.2rem; font-weight: bold;">R$ {result.aline_pagou_dividir + result.aline_pagou_casa:,.2f}</div>
                    <div style="font-size: 0.8rem; opacity: 0.9;">Dividíveis: R$ {result.aline_pagou_dividir:,.2f}</div>
                    <div style="font-size: 0.8rem; opacity: 0.9;">Casa: R$ {result.aline_pagou_casa:,.2f}</div>
                </div>
                """, unsafe_allow_html=True)
            with col_p3:
                st.markdown(f"""
                <div style="background: #38ef7d; padding: 1rem; border-radius: 10px; color: white; text-align: center;">
                    <div style="font-size: 0.9rem;">👨‍👩‍👦 Conta Família</div>
                    <div style="font-size: 1.2rem; font-weight: bold;">R$ {result.familia_pagou_dividir:,.2f}</div>
                    <div style="font-size: 0.8rem; opacity: 0.9;">(considerado 50/50)</div>
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown("---")
            st.markdown("### 💵 Valor do Acerto")
            
            saldo_acerto = float(result.aline_deve_renan)
            if saldo_acerto > 0:
                st.markdown(f"""
                <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 2rem; border-radius: 15px; text-align: center; color: white;">
                    <h2 style="margin: 0;">Aline deve pagar a Renan</h2>
                    <h1 style="margin: 0.5rem 0; font-size: 3rem;">{html.escape(_fmt_brl(abs(saldo_acerto)))}</h1>
                    <p style="margin: 0; opacity: 0.9;">Renan pagou (net): {html.escape(_fmt_brl(float(result.renan_pagou_total)))}</p>
                    <p style="margin: 0; opacity: 0.9;">Renan deveria pagar: {html.escape(_fmt_brl(float(result.renan_deveria_pagar)))}</p>
                </div>
                """, unsafe_allow_html=True)
            elif saldo_acerto < 0:
                st.markdown(f"""
                <div style="background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); padding: 2rem; border-radius: 15px; text-align: center; color: white;">
                    <h2 style="margin: 0;">Renan deve pagar a Aline</h2>
                    <h1 style="margin: 0.5rem 0; font-size: 3rem;">{html.escape(_fmt_brl(abs(saldo_acerto)))}</h1>
                    <p style="margin: 0; opacity: 0.9;">Aline pagou (net): {html.escape(_fmt_brl(float(result.aline_pagou_total)))}</p>
                    <p style="margin: 0; opacity: 0.9;">Aline deveria pagar: {html.escape(_fmt_brl(float(result.aline_deveria_pagar)))}</p>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.success("🎉 Saldo zerado! Ninguém deve nada.")

            st.markdown("---")
            st.markdown("### 🧾 Transações consideradas no acerto")
            with st.expander("Ver lista completa", expanded=False):
                if details_df.empty:
                    st.info("Sem transações consideradas.")
                else:
                    show_cols = [
                        c
                        for c in [
                            "txn_date",
                            "payment_method",
                            "account",
                            "description",
                            "category",
                            "subcategory",
                            "person",
                            "valor",
                            "regra",
                            "renan_deveria",
                            "aline_deveria",
                            "renan_delta",
                            "source_file",
                        ]
                        if c in details_df.columns
                    ]
                    show = details_df[show_cols].copy()
                    for col in ("valor", "renan_deveria", "aline_deveria", "renan_delta"):
                        if col in show.columns:
                            show[col] = show[col].apply(lambda v: _fmt_brl(float(v)))
                    st.dataframe(show, width="stretch", hide_index=True)
            
            # Faturas por cartão
            if result.por_cartao:
                st.markdown("---")
                st.markdown("### 💳 Faturas por Cartão")
                
                # Gradientes para variar os cards
                card_gradients = [
                    "linear-gradient(135deg, #667eea 0%, #764ba2 100%)",
                    "linear-gradient(135deg, #4facfe 0%, #00f2fe 100%)",
                    "linear-gradient(135deg, #fa709a 0%, #fee140 100%)",
                    "linear-gradient(135deg, #30cfd0 0%, #330867 100%)",
                    "linear-gradient(135deg, #a8edea 0%, #fed6e3 100%)",
                    "linear-gradient(135deg, #43e97b 0%, #38f9d7 100%)",
                ]
                
                cards_html = ['<div class="cc-grid">']
                for idx, (card_name, card_total) in enumerate(result.por_cartao.items()):
                    gradient = card_gradients[idx % len(card_gradients)]
                    cards_html.append(f'<div class="cc-card" style="background: {gradient};">')
                    cards_html.append('<div class="cc-card-title">')
                    cards_html.append(f"<span>💳 {html.escape(str(card_name or 'Sem cartão'))}</span>")
                    cards_html.append("</div>")
                    cards_html.append(f'<div class="cc-card-value">{html.escape(_fmt_brl(float(card_total)))}</div>')
                    cards_html.append("</div>")
                cards_html.append("</div>")
                st.markdown("\n".join(cards_html), unsafe_allow_html=True)
            
            # Contas da casa (se houver)
            if not df_casa.empty:
                st.markdown("---")
                st.markdown(f"### 🏠 Contas da Casa de {meses_nomes[ref_month-1]}")
                df_casa_display = df_casa[df_casa["amount"] < 0].copy()
                if not df_casa_display.empty:
                    df_casa_display["valor"] = df_casa_display["amount"].abs()
                    df_casa_display = df_casa_display[["txn_date", "subcategory", "description", "valor", "person"]].copy()
                    df_casa_display.columns = ["Data", "Tipo", "Descrição", "Valor", "Quem Pagou"]
                    df_casa_display["Valor"] = df_casa_display["Valor"].apply(lambda x: f"R$ {x:,.2f}")
                    st.dataframe(df_casa_display, width="stretch", hide_index=True)
            
            # Detalhamento por categoria (para dividir)
            st.markdown("---")
            st.markdown("### 📂 Gastos para Dividir (por Categoria)")
            if details_df.empty:
                st.info("Sem transações no acerto.")
            else:
                df_dividir = details_df[details_df["regra"] == "Dividir (50/50)"].copy() if "regra" in details_df.columns else details_df.iloc[0:0].copy()
                if not df_dividir.empty:
                    df_dividir = df_dividir[df_dividir["category"].notna()].copy() if "category" in df_dividir.columns else df_dividir.iloc[0:0].copy()
                if not df_dividir.empty and "category" in df_dividir.columns:
                    df_dividir = df_dividir[df_dividir["category"].astype(str).str.strip() != ""].copy()
                if df_dividir.empty:
                    st.info("Sem gastos para dividir por categoria.")
                else:
                    by_cat = df_dividir.groupby("category")["valor"].sum().sort_values(ascending=False).reset_index()
                    by_cat.columns = ["Categoria", "Total (net)"]
                    by_cat["Cada um paga (÷2)"] = by_cat["Total (net)"] / 2
                    by_cat["Total (net)"] = by_cat["Total (net)"].apply(lambda v: _fmt_brl(float(v)))
                    by_cat["Cada um paga (÷2)"] = by_cat["Cada um paga (÷2)"].apply(lambda v: _fmt_brl(float(v)))
                    st.dataframe(by_cat, width="stretch", hide_index=True)
            
            # Lista de itens sem categoria
            if result.qtd_sem_categoria > 0:
                st.markdown("---")
                st.warning(f"⚠️ **{result.qtd_sem_categoria} transações sem categoria** - Categorize antes de finalizar o acerto!")
                with st.expander("Ver itens sem categoria"):
                    if details_df.empty:
                        st.info("Sem itens.")
                    else:
                        df_sem = details_df.copy()
                        if "valor" in df_sem.columns:
                            df_sem = df_sem[df_sem["valor"] > 0].copy()
                        df_sem = (
                            df_sem[df_sem["category"].isna() | (df_sem["category"] == "")].copy()
                            if "category" in df_sem.columns
                            else df_sem.iloc[0:0].copy()
                        )
                        if df_sem.empty:
                            st.info("Sem itens.")
                        else:
                            show_cols = [c for c in ["txn_date", "description", "valor", "source_file"] if c in df_sem.columns]
                            show = df_sem[show_cols].copy().rename(
                                columns={
                                    "txn_date": "Data",
                                    "description": "Descrição",
                                    "valor": "Valor",
                                    "source_file": "Origem",
                                }
                            )
                            if "Valor" in show.columns:
                                show["Valor"] = show["Valor"].apply(lambda v: _fmt_brl(float(v)))
                            if "Data" in show.columns:
                                show = show.sort_values("Data", ascending=False)
                            st.dataframe(show, width="stretch", hide_index=True)
            
            # Botão de exportar para Excel
            st.markdown("---")
            st.markdown("### 📥 Exportar Acerto")
            
            # Preparar dados para export
            df_export = details_df.copy() if not details_df.empty else pd.DataFrame()
            if not df_export.empty:
                export_cols = [
                    c
                    for c in [
                        "txn_date",
                        "cash_date",
                        "payment_method",
                        "account",
                        "description",
                        "category",
                        "subcategory",
                        "valor",
                        "person",
                        "regra",
                        "renan_deveria",
                        "aline_deveria",
                        "renan_delta",
                        "source_file",
                    ]
                    if c in df_export.columns
                ]
                df_export = df_export[export_cols].copy()
                df_export = df_export.rename(
                    columns={
                        "txn_date": "Data",
                        "cash_date": "Impacto",
                        "payment_method": "Forma",
                        "account": "Cartão/Conta",
                        "description": "Descrição",
                        "category": "Categoria",
                        "subcategory": "Subcategoria",
                        "valor": "Valor (net)",
                        "person": "Quem Pagou",
                        "regra": "Regra",
                        "renan_deveria": "Renan deveria",
                        "aline_deveria": "Aline deveria",
                        "renan_delta": "Saldo Renan",
                        "source_file": "Origem",
                    }
                )

                def _divide_label(regra: Any) -> str:
                    s = str(regra or "")
                    if s.startswith("Gastos Renan"):
                        return "NÃO (Renan)"
                    if s.startswith("Gastos Aline"):
                        return "NÃO (Aline)"
                    return "SIM"

                if "Regra" in df_export.columns:
                    df_export["Divide?"] = df_export["Regra"].apply(_divide_label)
                df_export = df_export.sort_values([c for c in ["Origem", "Data"] if c in df_export.columns])
            
            # Gerar Excel em memória
            from io import BytesIO
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                # Aba com todas as transações
                if not df_export.empty:
                    df_export.to_excel(writer, sheet_name='Transações', index=False)
                
                # Aba com contas da casa
                if not df_casa.empty:
                    df_casa_exp = df_casa[df_casa["amount"] < 0][["txn_date", "subcategory", "description", "amount", "person"]].copy()
                    df_casa_exp.columns = ["Data", "Tipo", "Descrição", "Valor", "Quem Pagou"]
                    df_casa_exp["Valor"] = df_casa_exp["Valor"].abs()
                    df_casa_exp.to_excel(writer, sheet_name='Contas da Casa', index=False)
                df_export.to_excel(writer, sheet_name='Transações', index=False)
                
                # Aba com resumo
                resumo_data = {
                    "Item": [
                        "Total Despesas",
                        "Gastos para Dividir",
                        "Contas da Casa",
                        "Gastos Individuais Renan",
                        "Gastos Individuais Aline",
                        "",
                        "Renan pagou (dividíveis)",
                        "Aline pagou (dividíveis)",
                        "Família pagou (dividíveis)",
                        "Renan pagou (casa)",
                        "Aline pagou (casa)",
                        "",
                        "Aline deve a Renan" if result.aline_deve_renan >= 0 else "Renan deve a Aline"
                    ],
                    "Valor": [
                        result.total_despesas,
                        result.total_dividir,
                        result.total_contas_casa,
                        result.total_renan_individual,
                        result.total_aline_individual,
                        None,
                        result.renan_pagou_dividir,
                        result.aline_pagou_dividir,
                        result.familia_pagou_dividir,
                        result.renan_pagou_casa,
                        result.aline_pagou_casa,
                        None,
                        abs(result.aline_deve_renan)
                    ]
                }
                df_resumo = pd.DataFrame(resumo_data)
                df_resumo.to_excel(writer, sheet_name='Resumo', index=False)
                
                # Aba por categoria
                if not details_df.empty and "regra" in details_df.columns and "category" in details_df.columns:
                    df_div = details_df[details_df["regra"] == "Dividir (50/50)"].copy()
                    df_div = df_div[df_div["category"].notna()].copy()
                    df_div = df_div[df_div["category"].astype(str).str.strip() != ""].copy()
                    if not df_div.empty:
                        by_cat_export = df_div.groupby("category")["valor"].sum().sort_values(ascending=False).reset_index()
                        by_cat_export.columns = ["Categoria", "Total (net)"]
                        by_cat_export["Cada um paga (÷2)"] = by_cat_export["Total (net)"] / 2
                        by_cat_export.to_excel(writer, sheet_name='Por Categoria', index=False)
            
            excel_data = output.getvalue()
            
            st.download_button(
                "📥 Baixar Excel do Acerto",
                data=excel_data,
                file_name=f"acerto_{meses_nomes[ref_month-1].lower()}_{ref_year}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary",
            )

    elif str(nav).startswith("Trans"):
        st.subheader("Transacoes")
        st.caption("Lance rapido: salva no banco e no templates/financas.xlsx.")

        expense_cats, expense_sub_map = _expense_categories(expense_categories_tree)

        @st.dialog("Novo lancamento")
        def _new_transaction_dialog() -> None:
            with st.form("new_txn_simple_form"):
                txn_kind = st.radio(
                    "Tipo do lancamento",
                    ["Debito", "Cartao de credito", "Conta da casa"],
                    horizontal=True,
                )

                c1, c2 = st.columns(2)
                with c1:
                    txn_date = st.date_input("Data", value=date.today())
                with c2:
                    amount_in = st.number_input("Valor (R$)", min_value=0.01, step=10.0, format="%.2f")

                description = st.text_input("Descricao")
                category = st.selectbox("Categoria", [""] + expense_cats, index=0)
                sub_opts = expense_sub_map.get(category, []) if category else []
                subcategory = st.selectbox("Subcategoria", [""] + sub_opts, index=0)
                notes = st.text_area("Observacoes (opcional)", value="", height=80)

                person = "Renan"
                reimbursable = False
                card_id = None
                reference_month = None

                if txn_kind == "Debito":
                    payer = st.radio("Quem pagou", ["Renan", "Aline"], horizontal=True)
                    person = payer
                    reimbursable = st.checkbox("Reembolsavel", value=False)

                elif txn_kind == "Cartao de credito":
                    card_options = list(cards.keys())
                    card_id = st.selectbox("Cartao", card_options, index=0, format_func=lambda k: cards[k].name)
                    owner_default = cards[card_id].owner or ""
                    person = st.text_input("Portador (opcional)", value=owner_default)
                    reimbursable = st.checkbox("Reembolsavel", value=False)

                else:
                    payer = st.radio("Quem pagou", ["Renan", "Aline"], horizontal=True)
                    person = payer
                    reference_month = st.date_input(
                        "Mes de referencia",
                        value=txn_date.replace(day=1),
                        help="Mes ao qual a conta se refere (YYYY-MM).",
                    )

                submitted = st.form_submit_button("Salvar lancamento", type="primary")

            if not submitted:
                return

            description_v = (description or "").strip()
            if not description_v:
                st.error("Informe a descricao.")
                return
            if not category:
                st.error("Selecione a categoria.")
                return

            try:
                if txn_kind in ("Debito", "Cartao de credito"):
                    payment_method = "credit_card" if txn_kind == "Cartao de credito" else "debit"
                    entry = pf_manual.ManualEntry(
                        txn_date=txn_date,
                        amount=-abs(float(amount_in)),
                        description=description_v,
                        payment_method=payment_method,
                        group_name=None,
                        category=category or None,
                        subcategory=(subcategory or None),
                        person=(person.strip() or None),
                        reimbursable=bool(reimbursable),
                        reference=None,
                        notes=(notes.strip() or None),
                    )
                    row = pf_manual.build_manual_transaction_row(
                        entry,
                        card=cards[card_id] if card_id else None,
                    )
                else:
                    ref_month = (reference_month or txn_date).strftime("%Y-%m")
                    paid_dt = txn_date
                    amount_signed = -abs(float(amount_in))
                    row_hash = sha256_text(
                        "|".join([
                            "manual_household",
                            paid_dt.isoformat(),
                            f"{amount_signed:.2f}",
                            description_v,
                            category,
                            subcategory or "",
                            person,
                            ref_month,
                        ])
                    )
                    now = pf_db.now_iso()
                    row = {
                        "row_hash": row_hash,
                        "txn_date": paid_dt.isoformat(),
                        "cash_date": paid_dt.isoformat(),
                        "amount": amount_signed,
                        "description": description_v,
                        "group_name": None,
                        "category": category or None,
                        "subcategory": (subcategory or None),
                        "payment_method": "household",
                        "account": None,
                        "source": "manual_household",
                        "statement_closing_date": None,
                        "statement_due_date": None,
                        "person": person,
                        "reimbursable": 0,
                        "reference": ref_month,
                        "notes": (notes.strip() or None),
                        "source_file": "manual_entry",
                        "source_hash": "manual_entry",
                        "external_id": None,
                        "created_at": now,
                        "updated_at": now,
                    }

                inserted = pf_db.insert_transactions(conn, [row])
                if inserted == 0:
                    st.info("Lancamento ja existia (dedupe).")
                    return

                app = pf_excel_unified.append_transactions_to_unified(
                    unified_xlsx,
                    rows=[row],
                    expense_categories_tree=expense_categories_tree,
                    income_categories_tree=income_categories_tree,
                    cards=[c.name for c in cards.values()],
                )
                st.success(
                    "Lancamento salvo no banco e no Excel. "
                    f"(Excel: CC={app.get('credit_card',0)} D={app.get('debit',0)} C={app.get('household',0)})"
                )
                st.rerun()
            except Exception as e:
                st.error(f"Falha ao salvar: {e}")

        cta1, cta2 = st.columns([1, 2])
        with cta1:
            if st.button("+ Novo lancamento", type="primary", width="stretch"):
                _new_transaction_dialog()
        with cta2:
            st.caption("Escolha tipo: Debito, Cartao de credito ou Conta da casa.")

        st.divider()
        st.caption("Ultimos 20 lancamentos")
        df_recent = pf_queries.load_transactions_df(conn)
        if df_recent.empty:
            st.info("Sem lancamentos.")
        else:
            cols = [
                "txn_date",
                "cash_date",
                "payment_method",
                "account",
                "category",
                "subcategory",
                "description",
                "amount",
                "person",
            ]
            cols = [c for c in cols if c in df_recent.columns]
            show = df_recent.sort_values(["id"], ascending=[False]).head(20)[cols]
            st.dataframe(_display_df_ptbr(show), width="stretch", hide_index=True)

        st.divider()
        st.caption("Categorizacao de cartao (hierarquica) - salva no Excel e no banco")
        df_edit_src = pf_queries.load_transactions_df(conn, start=start, end=end)
        if df_edit_src.empty:
            st.info("Sem transacoes no periodo.")
        else:
            df_cc = df_edit_src[df_edit_src["payment_method"] == "credit_card"].copy()
            if df_cc.empty:
                st.info("Sem transacoes de cartao no periodo.")
            else:
                if "id" in df_cc.columns:
                    df_cc = df_cc.sort_values("id", ascending=False)
                df_cc = df_cc.head(200).reset_index(drop=True)

                expense_cats, expense_sub_map = _expense_categories(expense_categories_tree)
                preview = df_cc[["txn_date", "account", "description", "amount", "category", "subcategory"]].copy()
                preview["category"] = preview["category"].fillna("").astype(str)
                preview["subcategory"] = preview["subcategory"].fillna("").astype(str)
                st.dataframe(_display_df_ptbr(preview), width="stretch", hide_index=True)

                def _tx_label(i: int) -> str:
                    r = df_cc.iloc[i]
                    dt = _to_date(r.get("txn_date"))
                    dt_s = dt.isoformat() if dt else str(r.get("txn_date") or "")
                    acc = str(r.get("account") or "").strip()
                    desc = str(r.get("description") or "").strip()
                    amt = float(r.get("amount") or 0.0)
                    return f"{dt_s} | {acc} | {desc[:80]} | {_fmt_brl(abs(amt))}"

                idx = st.selectbox(
                    "Transacao para categorizar",
                    options=list(range(len(df_cc))),
                    format_func=_tx_label,
                    key="tx_cc_pick_idx",
                )
                row = df_cc.iloc[int(idx)]
                row_hash = str(row.get("row_hash") or "").strip()
                cur_cat = str(row.get("category") or "").strip()
                cur_sub = str(row.get("subcategory") or "").strip()
                cur_reemb = bool(int(row.get("reimbursable") or 0) == 1)

                cat_options = [""] + expense_cats
                cat_idx = cat_options.index(cur_cat) if cur_cat in cat_options else 0
                selected_cat = st.selectbox("Categoria", cat_options, index=cat_idx, key=f"tx_cat_{row_hash}")

                sub_options = [""] + expense_sub_map.get(selected_cat, [])
                sub_idx = sub_options.index(cur_sub) if cur_sub in sub_options else 0
                selected_sub = st.selectbox("Subcategoria", sub_options, index=sub_idx, key=f"tx_sub_{row_hash}")

                selected_reemb = st.checkbox("Reembolsavel", value=cur_reemb, key=f"tx_reemb_{row_hash}")

                if (selected_cat, selected_sub, selected_reemb) != (cur_cat, cur_sub, cur_reemb):
                    try:
                        updates = [
                            {
                                "row_hash": row_hash,
                                "category": selected_cat or None,
                                "subcategory": selected_sub or None,
                                "reimbursable": selected_reemb,
                            }
                        ]
                        updated_db, missing_db = pf_db.bulk_update_categories_by_row_hash(
                            conn, updates, allow_clear=True
                        )
                        updated_xlsx, missing_xlsx = pf_excel_unified.update_credit_card_categories(
                            unified_xlsx, updates=updates
                        )
                        st.success(
                            f"Categoria salva. DB: {updated_db} (faltantes {missing_db}) | "
                            f"Excel: {updated_xlsx} (faltantes {missing_xlsx})"
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(f"Falha ao salvar categorias: {e}")
    elif nav == review_label:
        st.subheader("🔍 Revisão de Importação")
        st.caption(
            "Transações importadas via CSV que possivelmente duplicam um lançamento manual no Excel. "
            "Escolha a ação para cada item."
        )

        reviews = pf_db.get_pending_reviews(conn)
        if not reviews:
            st.success("✅ Nenhuma transação aguardando revisão.")
        else:
            st.info(f"**{len(reviews)}** transação(ões) aguardando revisão.")
            for rev in reviews:
                inc = rev.get("incoming", {})
                inc_date = inc.get("txn_date", "—")
                inc_desc = inc.get("description", "—")
                inc_amount = inc.get("amount", 0.0)
                inc_account = inc.get("account", "—")

                # Fetch candidate transactions from DB
                cand_ids: list[int] = rev.get("candidate_ids", [])
                candidates: list[dict] = []
                for cid in cand_ids:
                    row = conn.execute(
                        "SELECT * FROM transactions WHERE id = ?", (cid,)
                    ).fetchone()
                    if row:
                        candidates.append(dict(row))

                rev_id = rev["id"]
                with st.expander(
                    f"📅 {inc_date}  |  {inc_desc}  |  R$ {abs(float(inc_amount)):.2f}",
                    expanded=True,
                ):
                    col_csv, col_manual = st.columns(2)
                    with col_csv:
                        st.markdown("**Importado do CSV**")
                        st.write(f"Data: `{inc_date}`")
                        st.write(f"Descrição: `{inc_desc}`")
                        st.write(f"Valor: `R$ {float(inc_amount):.2f}`")
                        st.write(f"Cartão: `{inc_account}`")
                        st.write(f"Arquivo: `{inc.get('source_file', '—')}`")
                    with col_manual:
                        st.markdown("**Lançamento(s) existente(s)**")
                        if candidates:
                            for cand in candidates:
                                st.write(f"Data: `{cand.get('txn_date', '—')}`")
                                st.write(f"Descrição: `{cand.get('description', '—')}`")
                                st.write(f"Valor: `R$ {float(cand.get('amount', 0)):.2f}`")
                                st.write(f"Categoria: `{cand.get('category') or '—'}`")
                                st.write(f"Subcategoria: `{cand.get('subcategory') or '—'}`")
                                st.write(f"Pessoa: `{cand.get('person') or '—'}`")
                                st.write(f"Notas: `{cand.get('notes') or '—'}`")
                                st.divider()
                        else:
                            st.write("_(não encontrado)_")

                    # Select which candidate to merge into (if multiple)
                    merge_target_id: int | None = cand_ids[0] if cand_ids else None
                    if len(cand_ids) > 1:
                        cand_labels = [
                            f"[{c.get('id')}] {c.get('description', '')} {c.get('txn_date', '')}"
                            for c in candidates
                        ]
                        sel_idx = st.selectbox(
                            "Mesclar com qual lançamento?",
                            options=list(range(len(cand_ids))),
                            format_func=lambda i: cand_labels[i],
                            key=f"sel_{rev_id}",
                        )
                        merge_target_id = cand_ids[sel_idx]

                    b1, b2, b3 = st.columns(3)
                    with b1:
                        if st.button("✅ Mesclar (manter edições)", key=f"merge_{rev_id}",
                                     help="Mantém o lançamento manual e atualiza com dados do CSV"):
                            pf_db.resolve_pending_review(
                                conn, review_id=rev_id, resolution="merge",
                                merge_into_id=merge_target_id
                            )
                            st.rerun()
                    with b2:
                        if st.button("➕ Criar como nova", key=f"create_{rev_id}",
                                     help="Insere o lançamento CSV como uma nova transação separada"):
                            pf_db.resolve_pending_review(
                                conn, review_id=rev_id, resolution="create_new"
                            )
                            st.rerun()
                    with b3:
                        if st.button("🗑️ Ignorar CSV", key=f"skip_{rev_id}",
                                     help="Descarta o lançamento do CSV, mantém somente o manual"):
                            pf_db.resolve_pending_review(
                                conn, review_id=rev_id, resolution="skip"
                            )
                            st.rerun()

    elif nav == "Config":
        st.subheader("Cartões")
        st.json({k: vars(v) for k, v in cards.items()})
        st.caption("Editar em `config/cards.json`.")

        st.subheader("Regras (auto-categorização)")
        st.caption("Editar em `config/rules.json`.")
        st.json(rules_cfg)
        with st.expander("Exemplo de regra (copiar/colar)", expanded=False):
            st.code(
                """{
  "rules": [
    {
      "name": "Uber",
      "enabled": true,
      "match": {
        "description_contains": ["uber"],
        "payment_methods": ["credit_card", "debit"],
        "kind": "expense"
      },
      "set": { "category": "Transporte", "subcategory": "Aplicativo" }
    }
  ]
}""",
                language="json",
            )
        if st.button("Aplicar regras aos lançamentos sem categoria/subcategoria", type="secondary"):
            try:
                updated = pf_rules_engine.apply_rules_to_transactions(conn, rules)
                st.success(f"{updated} lançamento(s) atualizado(s).")
                if updated:
                    sync_excels_ui()
            except Exception as e:  # noqa: BLE001
                st.warning(str(e))

        st.subheader("Categorias (despesas)")
        st.caption("Editar em `config/categories_expenses.json`.")
        st.json(expense_categories_tree)

        st.subheader("Categorias (receitas)")
        st.caption("Editar em `config/categories_income.json`.")
        st.json(income_categories_tree)

        st.subheader("Recebimentos (eventos)")
        st.caption("Editar em `config/pay_schedule.json`.")
        st.json(pay_schedule)


if __name__ == "__main__":
    main()
