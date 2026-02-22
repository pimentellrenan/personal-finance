from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from datetime import date as dt_date
from pathlib import Path
from typing import Iterable

from pf.utils import month_add, parse_date, sha256_text


@dataclass(frozen=True)
class DbPaths:
    base_dir: Path
    db_path: Path


def default_paths(base_dir: Path) -> DbPaths:
    data_dir = base_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return DbPaths(base_dir=base_dir, db_path=data_dir / "finance.sqlite")


def connect(db_path: Path, check_same_thread: bool = True) -> sqlite3.Connection:
    """
    Conecta ao banco de dados SQLite.
    
    Args:
        db_path: Caminho para o arquivo do banco de dados
        check_same_thread: Se False, permite usar a conexão em threads diferentes.
                          Útil para aplicações multi-thread como Streamlit.
    """
    conn = sqlite3.connect(str(db_path), check_same_thread=check_same_thread)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def new_origin_id() -> str:
    """Generates a stable UUID4 string to uniquely identify a transaction for life."""
    return str(uuid.uuid4())


def migrate(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS imports (
            hash TEXT PRIMARY KEY,
            file_path TEXT NOT NULL,
            importer TEXT NOT NULL,
            imported_at TEXT NOT NULL,
            rows INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            origin_id TEXT,
            row_hash TEXT NOT NULL,
            txn_date TEXT NOT NULL,
            cash_date TEXT NOT NULL,
            amount REAL NOT NULL,
            description TEXT NOT NULL,
            group_name TEXT,
            category TEXT,
            subcategory TEXT,
            payment_method TEXT NOT NULL,
            account TEXT,
            source TEXT,
            statement_closing_date TEXT,
            statement_due_date TEXT,
            person TEXT,
            reimbursable INTEGER NOT NULL DEFAULT 0,
            reference TEXT,
            notes TEXT,
            source_file TEXT,
            source_hash TEXT,
            external_id TEXT,
            hidden_in_excel INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_row_hash ON transactions(row_hash);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_origin_id ON transactions(origin_id) WHERE origin_id IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_transactions_txn_date ON transactions(txn_date);
        CREATE INDEX IF NOT EXISTS idx_transactions_cash_date ON transactions(cash_date);
        CREATE INDEX IF NOT EXISTS idx_transactions_due_date ON transactions(statement_due_date);
        CREATE INDEX IF NOT EXISTS idx_transactions_dedup ON transactions(txn_date, amount, account);

        CREATE TABLE IF NOT EXISTS pending_review (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            incoming_json TEXT NOT NULL,
            candidate_ids TEXT,
            source_file TEXT,
            match_type TEXT,
            created_at TEXT NOT NULL,
            resolved_at TEXT,
            resolution TEXT
        );

        CREATE TABLE IF NOT EXISTS credit_card_statements (
            card_source TEXT NOT NULL,
            statement_due_date TEXT NOT NULL,
            statement_closing_date TEXT,
            is_closed INTEGER NOT NULL DEFAULT 0,
            is_paid INTEGER NOT NULL DEFAULT 0,
            paid_date TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(card_source, statement_due_date)
        );

        CREATE INDEX IF NOT EXISTS idx_cc_statements_due_date ON credit_card_statements(statement_due_date);

        CREATE TABLE IF NOT EXISTS investments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            partition TEXT NOT NULL,
            issuer TEXT NOT NULL,
            product TEXT NOT NULL,
            maturity_date TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS investment_monthly (
            investment_id INTEGER NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            applied REAL,
            balance REAL,
            status TEXT,
            checked_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(investment_id, year, month),
            FOREIGN KEY(investment_id) REFERENCES investments(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_investment_monthly_period ON investment_monthly(year, month);
        """
    )

    # ── Backward-compatible column migrations ──────────────────────────────
    # Each block attempts to add a column only if it is missing; safe to run on
    # every startup because ALTER TABLE fails silently in the except clause.

    def _add_col_if_missing(table: str, col: str, definition: str) -> None:
        try:
            cols = {str(r[1]) for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
            if col not in cols:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {definition};")
        except Exception:  # noqa: BLE001
            pass

    _add_col_if_missing("investment_monthly", "checked_at", "TEXT")
    _add_col_if_missing("transactions", "origin_id", "TEXT")
    _add_col_if_missing("transactions", "hidden_in_excel", "INTEGER NOT NULL DEFAULT 0")

    # Ensure the unique index on origin_id exists (silently ignores if already there).
    try:
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_origin_id "
            "ON transactions(origin_id) WHERE origin_id IS NOT NULL;"
        )
    except Exception:  # noqa: BLE001
        pass

    # Ensure the composite dedupe index exists.
    try:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_transactions_dedup "
            "ON transactions(txn_date, amount, account);"
        )
    except Exception:  # noqa: BLE001
        pass

    conn.commit()

    # ── Data migrations (one-time) ──────────────────────────────────────────
    _migrate_populate_origin_ids(conn)


def _migrate_populate_origin_ids(conn: sqlite3.Connection) -> None:
    """
    One-time: assigns a UUID origin_id to every existing transaction that
    doesn't have one yet.  After the first run this becomes a no-op.
    """
    rows = conn.execute(
        "SELECT id FROM transactions WHERE origin_id IS NULL"
    ).fetchall()
    if not rows:
        return
    now = now_iso()
    updates = [(str(uuid.uuid4()), now, int(r["id"])) for r in rows]
    conn.executemany(
        "UPDATE transactions SET origin_id = ?, updated_at = ? WHERE id = ?",
        updates,
    )
    conn.commit()


def backfill_debit_cash_dates(conn: sqlite3.Connection) -> int:
    """
    One-time fix: older debit rows used `cash_date == txn_date`.
    We now treat debit cash impact as the next month (so Feb shows Jan debits in the dashboard).
    """
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT id, txn_date, cash_date FROM transactions WHERE payment_method = 'debit'"
    ).fetchall()
    updates: list[tuple[str, str, int]] = []
    now = now_iso()
    for r in rows:
        try:
            row_id = int(r["id"])
        except Exception:  # noqa: BLE001
            continue
        txn_dt = parse_date(r["txn_date"])
        if txn_dt is None:
            continue
        txn_s = txn_dt.isoformat()
        cash_s = str(r["cash_date"] or "").strip()
        if cash_s and cash_s != txn_s:
            continue
        desired = month_add(txn_dt, 1).isoformat()
        updates.append((desired, now, row_id))

    if updates:
        cur.executemany("UPDATE transactions SET cash_date = ?, updated_at = ? WHERE id = ?", updates)
        conn.commit()
    return len(updates)


def backfill_income_cash_dates(conn: sqlite3.Connection) -> int:
    """
    One-time fix: older income rows used `cash_date == txn_date`.
    We align income cash impact to the next month (so Feb shows Jan incomes in the dashboard).
    """
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT id, txn_date, cash_date FROM transactions WHERE payment_method = 'income'"
    ).fetchall()
    updates: list[tuple[str, str, int]] = []
    now = now_iso()
    for r in rows:
        try:
            row_id = int(r["id"])
        except Exception:  # noqa: BLE001
            continue
        txn_dt = parse_date(r["txn_date"])
        if txn_dt is None:
            continue
        txn_s = txn_dt.isoformat()
        cash_s = str(r["cash_date"] or "").strip()
        if cash_s and cash_s != txn_s:
            continue
        desired = month_add(txn_dt, 1).isoformat()
        updates.append((desired, now, row_id))

    if updates:
        cur.executemany("UPDATE transactions SET cash_date = ?, updated_at = ? WHERE id = ?", updates)
        conn.commit()
    return len(updates)


def is_imported(conn: sqlite3.Connection, file_hash: str) -> bool:
    row = conn.execute("SELECT 1 FROM imports WHERE hash = ? LIMIT 1", (file_hash,)).fetchone()
    return row is not None


def register_import(
    conn: sqlite3.Connection,
    *,
    file_hash: str,
    file_path: str,
    importer: str,
    rows: int,
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO imports(hash, file_path, importer, imported_at, rows)
        VALUES (?, ?, ?, ?, ?)
        """,
        (file_hash, file_path, importer, now_iso(), int(rows)),
    )
    conn.commit()


def get_credit_card_statement_meta(
    conn: sqlite3.Connection,
    *,
    card_source: str,
    statement_due_date: dt_date,
) -> dict | None:
    row = conn.execute(
        """
        SELECT *
        FROM credit_card_statements
        WHERE card_source = ?
          AND statement_due_date = ?
        LIMIT 1
        """,
        (str(card_source), statement_due_date.isoformat()),
    ).fetchone()
    return dict(row) if row is not None else None


def upsert_credit_card_statement_meta(
    conn: sqlite3.Connection,
    *,
    card_source: str,
    statement_due_date: dt_date,
    statement_closing_date: dt_date | None,
    is_closed: bool,
    is_paid: bool,
    paid_date: dt_date | None,
) -> None:
    now = now_iso()
    conn.execute(
        """
        INSERT INTO credit_card_statements(
            card_source,
            statement_due_date,
            statement_closing_date,
            is_closed,
            is_paid,
            paid_date,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(card_source, statement_due_date) DO UPDATE SET
            statement_closing_date = excluded.statement_closing_date,
            is_closed = excluded.is_closed,
            is_paid = excluded.is_paid,
            paid_date = excluded.paid_date,
            updated_at = excluded.updated_at
        """,
        (
            str(card_source),
            statement_due_date.isoformat(),
            statement_closing_date.isoformat() if statement_closing_date else None,
            1 if bool(is_closed) else 0,
            1 if bool(is_paid) else 0,
            paid_date.isoformat() if paid_date else None,
            now,
            now,
        ),
    )
    conn.commit()


def load_investments_df(conn: sqlite3.Connection):
    import pandas as pd

    df = pd.read_sql_query("SELECT * FROM investments ORDER BY id", conn)
    if "maturity_date" in df.columns:
        df["maturity_date"] = pd.to_datetime(df["maturity_date"], errors="coerce").dt.date
    return df


def upsert_investment(
    conn: sqlite3.Connection,
    *,
    investment_id: int | None,
    partition: str,
    issuer: str,
    product: str,
    maturity_date: dt_date | None,
) -> int:
    now = now_iso()
    if investment_id is None:
        cur = conn.execute(
            """
            INSERT INTO investments(partition, issuer, product, maturity_date, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                str(partition).strip(),
                str(issuer).strip(),
                str(product).strip(),
                maturity_date.isoformat() if maturity_date else None,
                now,
                now,
            ),
        )
        conn.commit()
        return int(cur.lastrowid)

    conn.execute(
        """
        UPDATE investments
        SET
            partition = ?,
            issuer = ?,
            product = ?,
            maturity_date = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            str(partition).strip(),
            str(issuer).strip(),
            str(product).strip(),
            maturity_date.isoformat() if maturity_date else None,
            now,
            int(investment_id),
        ),
    )
    conn.commit()
    return int(investment_id)


def delete_investment(conn: sqlite3.Connection, investment_id: int) -> None:
    conn.execute("DELETE FROM investments WHERE id = ?", (int(investment_id),))
    conn.commit()


def load_investment_monthly_df(
    conn: sqlite3.Connection,
    *,
    start_year: int,
    start_month: int,
    end_year: int,
    end_month: int,
):
    import pandas as pd

    start_period = int(start_year) * 12 + int(start_month)
    end_period = int(end_year) * 12 + int(end_month)
    df = pd.read_sql_query(
        """
        SELECT *
        FROM investment_monthly
        WHERE (year * 12 + month) BETWEEN ? AND ?
        ORDER BY investment_id, year, month
        """,
        conn,
        params=[start_period, end_period],
    )
    return df


def upsert_investment_monthly(
    conn: sqlite3.Connection,
    *,
    investment_id: int,
    year: int,
    month: int,
    applied: float | None,
    balance: float | None,
    status: str | None = None,
    checked_at: dt_date | None = None,
) -> None:
    now = now_iso()
    conn.execute(
        """
        INSERT INTO investment_monthly(
            investment_id, year, month,
            applied, balance, status,
            checked_at,
            created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(investment_id, year, month) DO UPDATE SET
            applied = excluded.applied,
            balance = excluded.balance,
            status = excluded.status,
            checked_at = excluded.checked_at,
            updated_at = excluded.updated_at
        """,
        (
            int(investment_id),
            int(year),
            int(month),
            None if applied is None else float(applied),
            None if balance is None else float(balance),
            (str(status).strip() if status is not None else None),
            checked_at.isoformat() if checked_at else None,
            now,
            now,
        ),
    )
    conn.commit()


def insert_transactions(conn: sqlite3.Connection, rows: Iterable[dict]) -> int:
    sql = """
    INSERT OR IGNORE INTO transactions(
        origin_id, row_hash, txn_date, cash_date, amount, description,
        group_name, category, subcategory,
        payment_method, account, source,
        statement_closing_date, statement_due_date,
        person, reimbursable, reference, notes,
        source_file, source_hash, external_id,
        hidden_in_excel,
        created_at, updated_at
    )
    VALUES(
        :origin_id, :row_hash, :txn_date, :cash_date, :amount, :description,
        :group_name, :category, :subcategory,
        :payment_method, :account, :source,
        :statement_closing_date, :statement_due_date,
        :person, :reimbursable, :reference, :notes,
        :source_file, :source_hash, :external_id,
        :hidden_in_excel,
        :created_at, :updated_at
    )
    """
    cur = conn.cursor()
    n = 0
    now = now_iso()
    for row in rows:
        row = dict(row)
        if not row.get("origin_id"):
            row["origin_id"] = new_origin_id()
        row.setdefault("hidden_in_excel", 0)
        row.setdefault("created_at", now)
        row.setdefault("updated_at", now)
        cur.execute(sql, row)
        n += cur.rowcount
    conn.commit()
    return n


@dataclass(frozen=True)
class SyncResult:
    inserted: int
    updated: int
    deleted: int


def sync_transactions_by_row_hash(
    conn: sqlite3.Connection,
    *,
    payment_method: str,
    rows: Iterable[dict],
    delete_missing: bool = False,
    user_fields_only: bool = False,
) -> SyncResult:
    """
    Excel-master style sync.

    Match priority:
      1. ``origin_id`` — stable UUID, survives description/hash changes.
      2. ``row_hash``  — backward compat fallback.

    ``user_fields_only=True``
        Only writes user-editable fields (description, category, subcategory,
        person, notes, reimbursable) — never overwrites source data (amounts,
        dates, source_file) that came from a CSV import.  Use for credit-card
        rows where the CSV is the authoritative source.

    ``delete_missing``
        When True, rows in the DB for this ``payment_method`` that are not in
        ``rows`` are **soft-deleted** (``hidden_in_excel = 1``) rather than
        physically removed.  Hard deletes no longer happen here to prevent
        accidentally wiping CSV-imported data by uploading an incomplete
        spreadsheet.
    """
    now = now_iso()

    # Materialise and track both identity keys.
    materialized: list[dict] = []
    origin_ids_seen: list[str] = []
    hashes_seen: list[str] = []

    for r in rows:
        rh = str(r.get("row_hash") or "").strip()
        if not rh:
            continue
        materialized.append(r)
        hashes_seen.append(rh)
        oid = str(r.get("origin_id") or "").strip()
        if oid:
            origin_ids_seen.append(oid)

    inserted = 0
    updated = 0
    cur = conn.cursor()

    insert_sql = """
    INSERT INTO transactions(
        origin_id, row_hash, txn_date, cash_date, amount, description,
        group_name, category, subcategory,
        payment_method, account, source,
        statement_closing_date, statement_due_date,
        person, reimbursable, reference, notes,
        source_file, source_hash, external_id,
        hidden_in_excel,
        created_at, updated_at
    )
    VALUES(
        :origin_id, :row_hash, :txn_date, :cash_date, :amount, :description,
        :group_name, :category, :subcategory,
        :payment_method, :account, :source,
        :statement_closing_date, :statement_due_date,
        :person, :reimbursable, :reference, :notes,
        :source_file, :source_hash, :external_id,
        0,
        :created_at, :updated_at
    )
    """

    # Full update (debit / income / household — user is the source of truth).
    full_update_sql = """
    UPDATE transactions
    SET
        row_hash = :row_hash,
        txn_date = :txn_date,
        cash_date = :cash_date,
        amount = :amount,
        description = :description,
        group_name = :group_name,
        category = :category,
        subcategory = :subcategory,
        payment_method = :payment_method,
        account = :account,
        source = :source,
        statement_closing_date = :statement_closing_date,
        statement_due_date = :statement_due_date,
        person = :person,
        reimbursable = :reimbursable,
        reference = :reference,
        notes = :notes,
        source_file = :source_file,
        source_hash = :source_hash,
        external_id = :external_id,
        hidden_in_excel = 0,
        updated_at = :updated_at
    WHERE id = :_match_id
    """

    for row in materialized:
        row = dict(row)
        row["payment_method"] = str(row.get("payment_method") or payment_method)
        row["updated_at"] = now

        # ── Try match by origin_id first ────────────────────────────────
        match_id: int | None = None
        origin_id_incoming = str(row.get("origin_id") or "").strip()
        if origin_id_incoming:
            existing = cur.execute(
                "SELECT id FROM transactions WHERE origin_id = ? LIMIT 1",
                (origin_id_incoming,),
            ).fetchone()
            if existing is not None:
                match_id = int(existing["id"])

        # ── Fallback: match by row_hash ──────────────────────────────────
        if match_id is None:
            existing = cur.execute(
                "SELECT id FROM transactions WHERE row_hash = ? LIMIT 1",
                (row["row_hash"],),
            ).fetchone()
            if existing is not None:
                match_id = int(existing["id"])

        if match_id is not None:
            if user_fields_only:
                # Only touch user-editable fields; never clobber CSV-origin data.
                _update_user_fields(conn, transaction_id=match_id, row=row, now=now)
                # Ensure row is visible in Excel again if it was hidden.
                cur.execute(
                    "UPDATE transactions SET hidden_in_excel = 0, updated_at = ? WHERE id = ?",
                    (now, match_id),
                )
            else:
                row["_match_id"] = match_id
                cur.execute(full_update_sql, row)
            updated += 1
        else:
            # Brand-new row.
            if not row.get("origin_id"):
                row["origin_id"] = new_origin_id()
            row.setdefault("created_at", now)
            row["hidden_in_excel"] = 0
            try:
                cur.execute(insert_sql, row)
                inserted += cur.rowcount
            except sqlite3.IntegrityError:
                pass  # race or true duplicate — skip silently

    # ── Soft-hide rows that are no longer present in the sheet ──────────
    hidden = 0
    if delete_missing and (hashes_seen or origin_ids_seen):
        # Build the NOT-IN exclusion using origin_ids where available.
        if origin_ids_seen:
            oid_placeholders = ", ".join(["?"] * len(origin_ids_seen))
            hash_placeholders = ", ".join(["?"] * len(hashes_seen))
            hidden = cur.execute(
                f"""
                UPDATE transactions
                SET hidden_in_excel = 1, updated_at = ?
                WHERE payment_method = ?
                  AND hidden_in_excel = 0
                  AND (origin_id IS NULL OR origin_id NOT IN ({oid_placeholders}))
                  AND row_hash NOT IN ({hash_placeholders})
                """,
                [now, payment_method] + origin_ids_seen + hashes_seen,
            ).rowcount
        else:
            hash_placeholders = ", ".join(["?"] * len(hashes_seen))
            hidden = cur.execute(
                f"""
                UPDATE transactions
                SET hidden_in_excel = 1, updated_at = ?
                WHERE payment_method = ?
                  AND hidden_in_excel = 0
                  AND row_hash NOT IN ({hash_placeholders})
                """,
                [now, payment_method] + hashes_seen,
            ).rowcount

    conn.commit()
    # ``deleted`` field repurposed to mean "soft-hidden" for callers that display it.
    return SyncResult(inserted=int(inserted), updated=int(updated), deleted=int(hidden))


def _update_user_fields(
    conn: sqlite3.Connection,
    *,
    transaction_id: int,
    row: dict,
    now: str,
) -> None:
    """
    Updates only the user-editable fields for an existing transaction.
    Preserves source-origin data (amount, txn_date, source_file, etc.).
    Always overwrites with whatever is in ``row`` so Excel edits take
    effect (caller decides what counts as user-editable).
    """
    fields: dict[str, object] = {}
    for key in ("description", "category", "subcategory", "person", "notes", "reimbursable", "reference"):
        val = row.get(key)
        if val is not None:
            fields[key] = val
    if not fields:
        return
    sets = ", ".join([f"{k} = ?" for k in fields])
    params = list(fields.values()) + [now, transaction_id]
    conn.execute(f"UPDATE transactions SET {sets}, updated_at = ? WHERE id = ?", params)


# ── Pending Review ───────────────────────────────────────────────────────────

def add_pending_review(
    conn: sqlite3.Connection,
    *,
    incoming: dict,
    candidate_ids: list[int],
    match_type: str,
) -> int:
    """
    Inserts an ambiguous incoming row into the review queue and returns its ID.
    ``incoming`` is the raw transaction dict from the importer (internal _ keys
    are stripped).  ``candidate_ids`` are the existing transaction IDs in the DB
    that partially matched.
    """
    clean = {k: v for k, v in incoming.items() if not str(k).startswith("_")}
    now = now_iso()
    cur = conn.execute(
        """
        INSERT INTO pending_review(incoming_json, candidate_ids, source_file, match_type, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            json.dumps(clean, default=str),
            json.dumps(candidate_ids),
            str(clean.get("source_file") or ""),
            str(match_type),
            now,
        ),
    )
    conn.commit()
    return int(cur.lastrowid)


def count_pending_reviews(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM pending_review WHERE resolved_at IS NULL"
    ).fetchone()
    return int(row[0]) if row else 0


def get_pending_reviews(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        """
        SELECT id, incoming_json, candidate_ids, source_file, match_type, created_at
        FROM pending_review
        WHERE resolved_at IS NULL
        ORDER BY created_at DESC
        """
    ).fetchall()
    out: list[dict] = []
    for r in rows:
        item = dict(r)
        try:
            item["incoming"] = json.loads(item.pop("incoming_json"))
        except Exception:  # noqa: BLE001
            item["incoming"] = {}
        try:
            item["candidate_ids"] = json.loads(item.get("candidate_ids") or "[]")
        except Exception:  # noqa: BLE001
            item["candidate_ids"] = []
        out.append(item)
    return out


def resolve_pending_review(
    conn: sqlite3.Connection,
    *,
    review_id: int,
    resolution: str,
    merge_into_id: int | None = None,
) -> None:
    """
    Resolves a pending review item.

    ``resolution`` must be one of:
    - ``"merge"``       — merge incoming into ``merge_into_id`` (keep user edits).
    - ``"create_new"``  — insert the incoming row as a brand-new transaction.
    - ``"skip"``        — discard the incoming row; do nothing.
    """
    now = now_iso()

    if resolution not in ("merge", "create_new", "skip"):
        raise ValueError(f"Invalid resolution: {resolution!r}")

    if resolution == "merge" and merge_into_id is not None:
        row_data_r = conn.execute(
            "SELECT incoming_json FROM pending_review WHERE id = ?",
            (int(review_id),),
        ).fetchone()
        if row_data_r:
            try:
                incoming = json.loads(row_data_r["incoming_json"])
            except Exception:  # noqa: BLE001
                incoming = {}
            if incoming:
                _update_user_fields(conn, transaction_id=merge_into_id, row=incoming, now=now)
                # Also update source fields from the incoming CSV row.
                src_fields = {
                    k: incoming[k]
                    for k in ("source_file", "source_hash", "external_id", "row_hash")
                    if incoming.get(k)
                }
                if src_fields:
                    sets = ", ".join([f"{k} = ?" for k in src_fields])
                    params = list(src_fields.values()) + [now, merge_into_id]
                    conn.execute(f"UPDATE transactions SET {sets}, updated_at = ? WHERE id = ?", params)

    elif resolution == "create_new":
        row_data_r = conn.execute(
            "SELECT incoming_json FROM pending_review WHERE id = ?",
            (int(review_id),),
        ).fetchone()
        if row_data_r:
            try:
                incoming = json.loads(row_data_r["incoming_json"])
            except Exception:  # noqa: BLE001
                incoming = {}
            if incoming:
                incoming["origin_id"] = new_origin_id()
                incoming.setdefault("created_at", now)
                incoming["updated_at"] = now
                incoming.setdefault("hidden_in_excel", 0)
                try:
                    cols = [
                        "origin_id", "row_hash", "txn_date", "cash_date", "amount", "description",
                        "group_name", "category", "subcategory", "payment_method", "account", "source",
                        "statement_closing_date", "statement_due_date", "person", "reimbursable",
                        "reference", "notes", "source_file", "source_hash", "external_id",
                        "hidden_in_excel", "created_at", "updated_at",
                    ]
                    placeholders = ", ".join([f":{c}" for c in cols])
                    conn.execute(
                        f"INSERT OR IGNORE INTO transactions({', '.join(cols)}) VALUES ({placeholders})",
                        incoming,
                    )
                except Exception:  # noqa: BLE001
                    pass

    conn.execute(
        "UPDATE pending_review SET resolved_at = ?, resolution = ? WHERE id = ?",
        (now, resolution, int(review_id)),
    )
    conn.commit()


def upsert_credit_card_transactions(conn: sqlite3.Connection, rows: Iterable[dict]) -> int:
    """
    Upserts credit-card rows with extra dedupe/migration support.

    - Inserts new rows by ``row_hash``.
    - If a row already exists (same ``row_hash``), updates non-user fields
      (cash_date, statement dates, etc.) and fills empty category fields
      (never overwrites existing categories).
    - If a legacy row_hash (older importer) exists, migrates it to the new hash
      and updates fields.
    - When a new CSV row has no match but looks like a manually-entered row
      (same date + abs(amount) + account, different source), it is added to
      ``pending_review`` instead of inserting a duplicate.
    - All inserted rows receive an ``origin_id`` UUID if they don't already
      have one.
    """

    def _strip_internal(row: dict) -> dict:
        return {k: v for k, v in row.items() if not str(k).startswith("_")}

    def _is_blank(v) -> bool:
        return v is None or str(v).strip() == ""

    def _legacy_hashes(row: dict) -> list[str]:
        amount_file = row.get("_legacy_amount_file")
        if amount_file is None:
            return []
        try:
            amount_file_f = float(amount_file)
        except Exception:  # noqa: BLE001
            return []

        source = str(row.get("source") or "")
        txn_date = str(row.get("txn_date") or "")
        description = str(row.get("description") or "")
        external_id = str(row.get("external_id") or "")

        amounts = {
            float(amount_file_f),
            -float(amount_file_f),
            abs(float(amount_file_f)),
            -abs(float(amount_file_f)),
        }
        out: list[str] = []
        for amt in sorted(amounts):
            out.append(
                sha256_text(
                    "|".join(
                        [
                            "credit_card",
                            source,
                            txn_date,
                            f"{amt:.2f}",
                            description,
                            external_id,
                        ]
                    )
                )
            )
        return out

    def _update_from_row(*, transaction_id: int, row_clean: dict) -> None:
        now = now_iso()
        fields: dict[str, object] = {
            "txn_date": row_clean.get("txn_date"),
            "cash_date": row_clean.get("cash_date"),
            "amount": row_clean.get("amount"),
            "payment_method": row_clean.get("payment_method"),
            "account": row_clean.get("account"),
            "source": row_clean.get("source"),
            "statement_closing_date": row_clean.get("statement_closing_date"),
            "statement_due_date": row_clean.get("statement_due_date"),
            "source_file": row_clean.get("source_file"),
            "source_hash": row_clean.get("source_hash"),
            "external_id": row_clean.get("external_id"),
            "updated_at": now,
        }

        # Never overwrite categorization fields; only fill blanks.
        existing = conn.execute(
            """
            SELECT description, group_name, category, subcategory, reimbursable, reference, notes, person
            FROM transactions
            WHERE id = ?
            """,
            (int(transaction_id),),
        ).fetchone()

        if existing is not None:
            # Preserve user-edited description (Excel is treated as source of truth).
            # Only fill when missing or placeholder.
            existing_desc = str(existing["description"] or "").strip()
            incoming_desc = str(row_clean.get("description") or "").strip()
            if (not existing_desc or existing_desc == "(sem descrição)") and incoming_desc:
                fields["description"] = incoming_desc
            for k in ("group_name", "category", "subcategory", "reference", "notes", "person"):
                if _is_blank(existing[k]) and not _is_blank(row_clean.get(k)):
                    fields[k] = row_clean.get(k)
            if int(existing["reimbursable"] or 0) == 0 and int(row_clean.get("reimbursable") or 0) == 1:
                fields["reimbursable"] = 1

        sets = ", ".join([f"{k} = ?" for k in fields.keys()])
        params = list(fields.values()) + [int(transaction_id)]
        conn.execute(f"UPDATE transactions SET {sets} WHERE id = ?", params)

    insert_sql = """
    INSERT OR IGNORE INTO transactions(
        origin_id, row_hash, txn_date, cash_date, amount, description,
        group_name, category, subcategory,
        payment_method, account, source,
        statement_closing_date, statement_due_date,
        person, reimbursable, reference, notes,
        source_file, source_hash, external_id,
        hidden_in_excel,
        created_at, updated_at
    )
    VALUES(
        :origin_id, :row_hash, :txn_date, :cash_date, :amount, :description,
        :group_name, :category, :subcategory,
        :payment_method, :account, :source,
        :statement_closing_date, :statement_due_date,
        :person, :reimbursable, :reference, :notes,
        :source_file, :source_hash, :external_id,
        0,
        :created_at, :updated_at
    )
    """

    cur = conn.cursor()
    inserted = 0

    for row in rows:
        row_clean = _strip_internal(row)
        row_hash = str(row_clean.get("row_hash") or "")
        if not row_hash:
            continue

        # Already in new format: just refresh non-user fields.
        existing = conn.execute("SELECT id FROM transactions WHERE row_hash = ? LIMIT 1", (row_hash,)).fetchone()
        if existing is not None:
            _update_from_row(transaction_id=int(existing["id"]), row_clean=row_clean)
            continue

        # Try to migrate legacy hash (older importer versions).
        legacy_hashes = _legacy_hashes(row)
        legacy_row = None
        if legacy_hashes:
            placeholders = ", ".join(["?"] * len(legacy_hashes))
            legacy_row = conn.execute(
                f"""
                SELECT id, row_hash, group_name, category, subcategory, reimbursable, reference, notes, person
                FROM transactions
                WHERE row_hash IN ({placeholders})
                LIMIT 1
                """,
                legacy_hashes,
            ).fetchone()

        if legacy_row is not None:
            try:
                conn.execute(
                    "UPDATE transactions SET row_hash = ? WHERE id = ?",
                    (row_hash, int(legacy_row["id"])),
                )
                _update_from_row(transaction_id=int(legacy_row["id"]), row_clean=row_clean)
                continue
            except sqlite3.IntegrityError:
                # New hash exists already. Merge user fields into the new row (only when empty) and drop legacy.
                new_row = conn.execute(
                    """
                    SELECT id, group_name, category, subcategory, reimbursable, reference, notes, person
                    FROM transactions
                    WHERE row_hash = ?
                    LIMIT 1
                    """,
                    (row_hash,),
                ).fetchone()
                if new_row is not None:
                    updates: dict[str, object] = {}
                    for k in ("group_name", "category", "subcategory", "reference", "notes", "person"):
                        if _is_blank(new_row[k]) and not _is_blank(legacy_row[k]):
                            updates[k] = legacy_row[k]
                    if int(new_row["reimbursable"] or 0) == 0 and int(legacy_row["reimbursable"] or 0) == 1:
                        updates["reimbursable"] = 1
                    if updates:
                        sets = ", ".join([f"{k} = ?" for k in updates.keys()])
                        params = list(updates.values()) + [now_iso(), int(new_row["id"])]
                        conn.execute(f"UPDATE transactions SET {sets}, updated_at = ? WHERE id = ?", params)
                    conn.execute("DELETE FROM transactions WHERE id = ?", (int(legacy_row["id"]),))
                    continue

        # Fix imports that accidentally stored amount=0 by matching on source_file/txn_date/description.
        zero_match = conn.execute(
            """
            SELECT id
            FROM transactions
            WHERE payment_method = 'credit_card'
              AND amount = 0
              AND source_file = ?
              AND txn_date = ?
              AND description = ?
            LIMIT 1
            """,
            (
                row_clean.get("source_file"),
                row_clean.get("txn_date"),
                row_clean.get("description"),
            ),
        ).fetchone()
        if zero_match is not None:
            conn.execute(
                "UPDATE transactions SET row_hash = ? WHERE id = ?",
                (row_hash, int(zero_match["id"])),
            )
            _update_from_row(transaction_id=int(zero_match["id"]), row_clean=row_clean)
            continue

        # Check for duplicate by key fields (txn_date + cash_date + signed amount + account).
        # We intentionally keep the sign strict to avoid merging purchase and refund that happen
        # with the same absolute value on the same day.
        # To avoid collapsing legitimate repeated charges (same amount/date/card),
        # only merge when there is a unique candidate in DB.
        txn_date = row_clean.get("txn_date")
        cash_date = row_clean.get("cash_date")
        amount = row_clean.get("amount")
        account = row_clean.get("account")
        source_file = row_clean.get("source_file")
        source = row_clean.get("source")
        
        if txn_date and cash_date and amount is not None:
            amount_f = float(amount)
            if account:
                dup_matches = conn.execute(
                    """
                    SELECT id, row_hash
                    FROM transactions
                    WHERE payment_method = 'credit_card'
                      AND txn_date = ?
                      AND cash_date = ?
                      AND ABS(amount - ?) < 0.01
                      AND account = ?
                      AND (source = ? OR source = 'excel_credit_card' OR ? IS NULL OR source IS NULL)
                      AND COALESCE(source_file, '') <> COALESCE(?, '')
                    LIMIT 2
                    """,
                    (txn_date, cash_date, amount_f, account, source, source, source_file),
                ).fetchall()
            else:
                dup_matches = conn.execute(
                    """
                    SELECT id, row_hash
                    FROM transactions
                    WHERE payment_method = 'credit_card'
                      AND txn_date = ?
                      AND cash_date = ?
                      AND ABS(amount - ?) < 0.01
                      AND (source = ? OR source = 'excel_credit_card' OR ? IS NULL OR source IS NULL)
                      AND COALESCE(source_file, '') <> COALESCE(?, '')
                    LIMIT 2
                    """,
                    (txn_date, cash_date, amount_f, source, source, source_file),
                ).fetchall()
            
            duplicate_match = dup_matches[0] if len(dup_matches) == 1 else None
            if duplicate_match is not None:
                # Update existing row with CSV data (better source) and new hash
                conn.execute(
                    "UPDATE transactions SET row_hash = ? WHERE id = ?",
                    (row_hash, int(duplicate_match["id"])),
                )
                _update_from_row(transaction_id=int(duplicate_match["id"]), row_clean=row_clean)
                continue

        # Handle statement updates where only the description changes (e.g., "PENDING" -> final merchant name).
        # We only attempt this when the (card, date, amount, person, etc.) tuple is UNIQUE within the current file
        # to avoid merging two distinct transactions that coincidentally share these fields.
        stable_key = str(row.get("_stable_key") or "").strip()
        stable_unique = bool(row.get("_stable_key_unique_in_file"))
        if stable_key and stable_unique and txn_date and amount is not None:
            amount_f = float(amount)

            source = row_clean.get("source")
            person = row_clean.get("person")
            external_id = row_clean.get("external_id")
            source_file = row_clean.get("source_file")

            ambiguous = False
            stable_match = None
            sql = """
                SELECT id
                FROM transactions
                WHERE payment_method = 'credit_card'
                  AND txn_date = ?
                  AND ABS(amount - ?) < 0.01
                  AND (source = ? OR source = 'excel_credit_card')
                  AND source_file = ?
            """
            params = [txn_date, amount_f, source, source_file]
            if person:
                sql += " AND person = ?"
                params.append(person)
            if external_id:
                sql += " AND external_id = ?"
                params.append(external_id)
            sql += " LIMIT 2"

            matches = conn.execute(sql, params).fetchall()
            if len(matches) > 1:
                ambiguous = True
            elif len(matches) == 1:
                stable_match = matches[0]

            if not ambiguous and stable_match is not None:
                _update_from_row(transaction_id=int(stable_match["id"]), row_clean=row_clean)
                continue

        # ── Manual-entry duplicate check ─────────────────────────────────────
        # If there's a row from a manual/Excel source with the same (date, amount,
        # account), it's probably the same transaction entered by hand before the
        # CSV was imported.  When there's exactly one candidate we route to
        # pending_review so the user can decide; when there are many candidates
        # (e.g. repeated small purchases) we just insert.
        txn_date2 = row_clean.get("txn_date")
        amount2 = row_clean.get("amount")
        account2 = row_clean.get("account")
        source_file2 = row_clean.get("source_file")
        if txn_date2 and amount2 is not None:
            amount_f2 = float(amount2)
            manual_candidates = conn.execute(
                """
                SELECT id FROM transactions
                WHERE payment_method = 'credit_card'
                  AND txn_date = ?
                  AND ABS(amount - ?) < 0.01
                  AND (account = ? OR ? IS NULL)
                  AND source IN ('excel_credit_card', 'excel_manual', 'manual')
                  AND COALESCE(source_file, '') <> COALESCE(?, '')
                LIMIT 2
                """,
                (txn_date2, amount_f2, account2, account2, source_file2 or ""),
            ).fetchall()
            if len(manual_candidates) == 1:
                add_pending_review(
                    conn,
                    incoming=row_clean,
                    candidate_ids=[int(manual_candidates[0]["id"])],
                    match_type="manual_vs_csv",
                )
                continue  # don't insert yet; user decides

        # Brand-new row — assign a stable origin_id.
        row_clean["origin_id"] = new_origin_id()
        row_clean["hidden_in_excel"] = 0
        row_clean.setdefault("created_at", now_iso())
        row_clean["updated_at"] = now_iso()
        cur.execute(insert_sql, row_clean)
        inserted += cur.rowcount

    conn.commit()
    return inserted


def update_transaction_categories(
    conn: sqlite3.Connection,
    *,
    transaction_id: int,
    group_name: str | None,
    category: str | None,
    subcategory: str | None,
    reimbursable: bool | None = None,
    person: str | None = None,
    reference: str | None = None,
    notes: str | None = None,
) -> None:
    fields = {
        "group_name": group_name,
        "category": category,
        "subcategory": subcategory,
        "person": person,
        "reference": reference,
        "notes": notes,
    }
    if reimbursable is not None:
        fields["reimbursable"] = 1 if reimbursable else 0
    sets = ", ".join([f"{k} = ?" for k in fields.keys()])
    values = list(fields.values())
    values.extend([now_iso(), int(transaction_id)])
    conn.execute(f"UPDATE transactions SET {sets}, updated_at = ? WHERE id = ?", values)
    conn.commit()


def update_categories_by_row_hash(
    conn: sqlite3.Connection,
    *,
    row_hash: str,
    category: str | None = None,
    subcategory: str | None = None,
    reimbursable: bool | None = None,
    person: str | None = None,
    allow_clear: bool = False,
) -> bool:
    """
    Updates user-controlled fields for an existing transaction identified by `row_hash`.
    Returns True when a row was found and updated.
    """
    rh = str(row_hash or "").strip()
    if not rh:
        return False

    existing = conn.execute("SELECT id FROM transactions WHERE row_hash = ? LIMIT 1", (rh,)).fetchone()
    if existing is None:
        return False

    fields: dict[str, object] = {}
    if allow_clear or (category is not None and str(category).strip() != ""):
        fields["category"] = (str(category).strip() or None) if category is not None else None
    if allow_clear or (subcategory is not None and str(subcategory).strip() != ""):
        fields["subcategory"] = (str(subcategory).strip() or None) if subcategory is not None else None
    if allow_clear or (person is not None and str(person).strip() != ""):
        fields["person"] = (str(person).strip() or None) if person is not None else None
    if reimbursable is not None:
        fields["reimbursable"] = 1 if bool(reimbursable) else 0

    if not fields:
        return True

    sets = ", ".join([f"{k} = ?" for k in fields.keys()])
    values = list(fields.values())
    values.extend([now_iso(), int(existing["id"])])
    conn.execute(f"UPDATE transactions SET {sets}, updated_at = ? WHERE id = ?", values)
    return True


def bulk_update_categories_by_row_hash(
    conn: sqlite3.Connection,
    updates: Iterable[dict],
    *,
    allow_clear: bool = False,
) -> tuple[int, int]:
    """
    Updates many rows identified by `row_hash`.
    Returns `(updated_count, missing_count)`.
    """
    cur = conn.cursor()
    updated = 0
    missing = 0
    now = now_iso()

    for u in updates:
        rh = str(u.get("row_hash") or "").strip()
        if not rh:
            continue

        row = cur.execute("SELECT id FROM transactions WHERE row_hash = ? LIMIT 1", (rh,)).fetchone()
        if row is None:
            missing += 1
            continue

        category = u.get("category")
        subcategory = u.get("subcategory")
        person = u.get("person")
        reimbursable = u.get("reimbursable")

        fields: dict[str, object] = {}
        if allow_clear or (category is not None and str(category).strip() != ""):
            fields["category"] = (str(category).strip() or None) if category is not None else None
        if allow_clear or (subcategory is not None and str(subcategory).strip() != ""):
            fields["subcategory"] = (str(subcategory).strip() or None) if subcategory is not None else None
        if allow_clear or (person is not None and str(person).strip() != ""):
            fields["person"] = (str(person).strip() or None) if person is not None else None
        if reimbursable is not None:
            fields["reimbursable"] = 1 if bool(reimbursable) else 0

        if not fields:
            updated += 1
            continue

        sets = ", ".join([f"{k} = ?" for k in fields.keys()])
        params = list(fields.values()) + [now, int(row["id"])]
        cur.execute(f"UPDATE transactions SET {sets}, updated_at = ? WHERE id = ?", params)
        updated += cur.rowcount

    conn.commit()
    return updated, missing


def delete_all(conn: sqlite3.Connection) -> None:
    """
    Legacy helper: clears transactional data used by the dashboard.

    Note: this does NOT remove investments nor credit-card statement metadata.
    """
    delete_all_transactions(conn)
    delete_all_imports(conn)


def delete_all_transactions(conn: sqlite3.Connection) -> int:
    cur = conn.execute("DELETE FROM transactions")
    conn.commit()
    return int(cur.rowcount or 0)


def delete_all_imports(conn: sqlite3.Connection) -> int:
    cur = conn.execute("DELETE FROM imports")
    conn.commit()
    return int(cur.rowcount or 0)


def delete_all_credit_card_statements(conn: sqlite3.Connection) -> int:
    cur = conn.execute("DELETE FROM credit_card_statements")
    conn.commit()
    return int(cur.rowcount or 0)


def delete_all_investments(conn: sqlite3.Connection) -> tuple[int, int]:
    """
    Clears investments data.

    Returns (deleted_investments, deleted_monthly_rows).
    """
    cur_monthly = conn.execute("DELETE FROM investment_monthly")
    cur_inv = conn.execute("DELETE FROM investments")
    conn.commit()
    return int(cur_inv.rowcount or 0), int(cur_monthly.rowcount or 0)


def delete_transactions_by_sources(conn: sqlite3.Connection, sources: Iterable[str]) -> int:
    src = sorted({str(s).strip() for s in sources if str(s).strip()})
    if not src:
        return 0
    placeholders = ", ".join(["?"] * len(src))
    cur = conn.execute(f"DELETE FROM transactions WHERE source IN ({placeholders})", src)
    conn.commit()
    return int(cur.rowcount or 0)


def delete_imports_by_importers(conn: sqlite3.Connection, importers: Iterable[str]) -> int:
    imps = sorted({str(s).strip() for s in importers if str(s).strip()})
    if not imps:
        return 0
    placeholders = ", ".join(["?"] * len(imps))
    cur = conn.execute(f"DELETE FROM imports WHERE importer IN ({placeholders})", imps)
    conn.commit()
    return int(cur.rowcount or 0)


def delete_everything(conn: sqlite3.Connection) -> None:
    """
    Clears ALL application data from the database (transactions, imports, statement meta, investments).
    """
    delete_all_transactions(conn)
    delete_all_imports(conn)
    delete_all_credit_card_statements(conn)
    delete_all_investments(conn)


def upsert_from_excel(
    conn: sqlite3.Connection,
    rows: Iterable[dict],
) -> tuple[int, int, int]:
    """
    Upserts transactions from Excel data.
    - If row_hash exists: updates category, subcategory, person, reimbursable
    - If row_hash doesn't exist: inserts new transaction
    
    Returns (inserted_count, updated_count, skipped_count).
    """
    cur = conn.cursor()
    inserted = 0
    updated = 0
    skipped = 0
    now = now_iso()

    for row in rows:
        rh = str(row.get("row_hash") or "").strip()
        origin_id = str(row.get("origin_id") or "").strip()

        if not rh and not origin_id:
            skipped += 1
            continue

        # ── Find existing row: prefer origin_id, fall back to row_hash ──
        existing = None
        if origin_id:
            existing = cur.execute(
                "SELECT id, category, subcategory, person, reimbursable FROM transactions WHERE origin_id = ? LIMIT 1",
                (origin_id,),
            ).fetchone()
        if existing is None and rh:
            existing = cur.execute(
                "SELECT id, category, subcategory, person, reimbursable FROM transactions WHERE row_hash = ? LIMIT 1",
                (rh,),
            ).fetchone()

        if existing is not None:
            # Update existing row with category/subcategory/person/reimbursable
            fields: dict[str, object] = {}

            category = row.get("category")
            subcategory = row.get("subcategory")
            person = row.get("person")
            reimbursable = row.get("reimbursable")

            # Only update if value is provided in Excel
            if category is not None and str(category).strip():
                fields["category"] = str(category).strip()
            if subcategory is not None and str(subcategory).strip():
                fields["subcategory"] = str(subcategory).strip()
            if person is not None and str(person).strip():
                fields["person"] = str(person).strip()
            if reimbursable is not None:
                fields["reimbursable"] = 1 if bool(reimbursable) else 0

            if fields:
                sets = ", ".join([f"{k} = ?" for k in fields.keys()])
                params = list(fields.values()) + [now, int(existing["id"])]
                cur.execute(f"UPDATE transactions SET {sets}, updated_at = ? WHERE id = ?", params)

            updated += 1
        else:
            # Insert new row
            txn_date = row.get("txn_date")
            amount = row.get("amount")
            description = row.get("description")

            # Require minimal data for new rows
            if not txn_date or amount is None or not description:
                skipped += 1
                continue

            cash_date = row.get("statement_due_date") or txn_date

            insert_row = {
                "origin_id": new_origin_id(),
                "row_hash": rh or sha256_text(f"excel_manual|{txn_date}|{float(amount):.2f}|{description}"),
                "txn_date": txn_date,
                "cash_date": cash_date,
                "amount": float(amount),
                "description": str(description).strip(),
                "group_name": None,
                "category": str(row.get("category") or "").strip() or None,
                "subcategory": str(row.get("subcategory") or "").strip() or None,
                "payment_method": "credit_card",
                "account": str(row.get("account") or "").strip() or None,
                "source": "excel_manual",
                "statement_closing_date": None,
                "statement_due_date": row.get("statement_due_date"),
                "person": str(row.get("person") or "").strip() or None,
                "reimbursable": 1 if row.get("reimbursable") else 0,
                "reference": None,
                "notes": None,
                "source_file": "manual_excel",
                "source_hash": None,
                "external_id": None,
                "hidden_in_excel": 0,
                "created_at": now,
                "updated_at": now,
            }

            cur.execute(
                """
                INSERT OR IGNORE INTO transactions(
                    origin_id, row_hash, txn_date, cash_date, amount, description,
                    group_name, category, subcategory,
                    payment_method, account, source,
                    statement_closing_date, statement_due_date,
                    person, reimbursable, reference, notes,
                    source_file, source_hash, external_id,
                    hidden_in_excel,
                    created_at, updated_at
                )
                VALUES(
                    :origin_id, :row_hash, :txn_date, :cash_date, :amount, :description,
                    :group_name, :category, :subcategory,
                    :payment_method, :account, :source,
                    :statement_closing_date, :statement_due_date,
                    :person, :reimbursable, :reference, :notes,
                    :source_file, :source_hash, :external_id,
                    :hidden_in_excel,
                    :created_at, :updated_at
                )
                """,
                insert_row,
            )
            inserted += 1

    conn.commit()
    return inserted, updated, skipped
