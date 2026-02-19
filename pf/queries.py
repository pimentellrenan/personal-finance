from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd


def load_transactions_df(conn, *, start: date | None = None, end: date | None = None) -> pd.DataFrame:
    where = []
    params: list[Any] = []
    if start is not None:
        where.append("cash_date >= ?")
        params.append(start.isoformat())
    if end is not None:
        where.append("cash_date <= ?")
        params.append(end.isoformat())
    sql = "SELECT * FROM transactions"
    if where:
        sql += " WHERE " + " AND ".join(where)
    df = pd.read_sql_query(sql, conn, params=params)
    for col in ("txn_date", "cash_date", "statement_closing_date", "statement_due_date", "created_at", "updated_at"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    return df


def load_transactions_df_by_txn_date(conn, *, start: date | None = None, end: date | None = None) -> pd.DataFrame:
    where = []
    params: list[Any] = []
    if start is not None:
        where.append("txn_date >= ?")
        params.append(start.isoformat())
    if end is not None:
        where.append("txn_date <= ?")
        params.append(end.isoformat())
    sql = "SELECT * FROM transactions"
    if where:
        sql += " WHERE " + " AND ".join(where)
    df = pd.read_sql_query(sql, conn, params=params)
    for col in ("txn_date", "cash_date", "statement_closing_date", "statement_due_date", "created_at", "updated_at"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    return df


def load_imports_df(conn) -> pd.DataFrame:
    df = pd.read_sql_query("SELECT * FROM imports ORDER BY imported_at DESC", conn)
    return df
