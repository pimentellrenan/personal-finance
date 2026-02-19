from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping

from pf import db as pf_db
from pf.utils import normalize_str


@dataclass(frozen=True)
class CompiledRule:
    name: str
    description_contains: tuple[str, ...]
    description_regex: re.Pattern[str] | None
    payment_methods: frozenset[str] | None
    accounts: frozenset[str] | None
    sources: frozenset[str] | None
    kind: str  # "any" | "expense" | "income"
    set_category: str | None
    set_subcategory: str | None
    set_reimbursable: bool | None
    set_reference: str | None
    set_notes: str | None
    override: bool
    stop: bool


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(x) for x in value if str(x).strip()]
    text = str(value).strip()
    return [text] if text else []


def _parse_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(int(value))
    v = normalize_str(value)
    if v in ("sim", "s", "yes", "y", "true", "1"):
        return True
    if v in ("nao", "não", "n", "no", "false", "0"):
        return False
    return None


def compile_rules(rules: list[dict[str, Any]] | None) -> list[CompiledRule]:
    if not rules:
        return []

    compiled: list[CompiledRule] = []
    for raw in rules:
        if not isinstance(raw, dict):
            continue
        if raw.get("enabled", True) is False:
            continue

        match = raw.get("match") if isinstance(raw.get("match"), dict) else raw
        set_ = raw.get("set") if isinstance(raw.get("set"), dict) else raw

        name = str(raw.get("name") or raw.get("id") or raw.get("pattern") or "regra").strip() or "regra"

        contains = match.get("description_contains") or match.get("contains")
        regex_text = match.get("description_regex") or match.get("regex")

        # Support shorthand: {"pattern": "...", "match_type": "contains|regex"}
        if not contains and not regex_text and match.get("pattern"):
            pattern = str(match.get("pattern") or "").strip()
            match_type = normalize_str(match.get("match_type") or match.get("match") or "contains")
            if pattern:
                if match_type == "regex":
                    regex_text = pattern
                else:
                    contains = [pattern]

        contains_norm = tuple(normalize_str(x) for x in _as_list(contains) if normalize_str(x))

        regex = None
        if regex_text:
            try:
                regex = re.compile(str(regex_text), flags=re.IGNORECASE)
            except re.error:
                regex = None

        payment_methods = _as_list(match.get("payment_methods") or match.get("payment_method"))
        pm_norm = frozenset(normalize_str(x) for x in payment_methods if normalize_str(x)) or None

        accounts = _as_list(match.get("accounts") or match.get("account"))
        acct_norm = frozenset(normalize_str(x) for x in accounts if normalize_str(x)) or None

        sources = _as_list(match.get("sources") or match.get("source"))
        src_norm = frozenset(normalize_str(x) for x in sources if normalize_str(x)) or None

        kind = normalize_str(match.get("kind") or match.get("amount_sign") or "any")
        if kind not in ("any", "expense", "income"):
            kind = "any"

        set_category = str(set_.get("category") or set_.get("set_category") or "").strip() or None
        set_subcategory = str(set_.get("subcategory") or set_.get("set_subcategory") or "").strip() or None
        set_reimbursable = _parse_bool(set_.get("reimbursable") or set_.get("reembolsavel"))
        set_reference = str(set_.get("reference") or set_.get("referencia") or "").strip() or None
        set_notes = str(set_.get("notes") or set_.get("observacoes") or "").strip() or None

        override = bool(raw.get("override", False))
        stop = bool(raw.get("stop", True))

        if not contains_norm and regex is None:
            continue
        if not any([set_category, set_subcategory, set_reimbursable is not None, set_reference, set_notes]):
            continue

        compiled.append(
            CompiledRule(
                name=name,
                description_contains=contains_norm,
                description_regex=regex,
                payment_methods=pm_norm,
                accounts=acct_norm,
                sources=src_norm,
                kind=kind,
                set_category=set_category,
                set_subcategory=set_subcategory,
                set_reimbursable=set_reimbursable,
                set_reference=set_reference,
                set_notes=set_notes,
                override=override,
                stop=stop,
            )
        )

    return compiled


def _is_blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _matches(rule: CompiledRule, row: Mapping[str, Any]) -> bool:
    amount = row.get("amount")
    try:
        amt = float(amount) if amount is not None else 0.0
    except (TypeError, ValueError):
        amt = 0.0

    if rule.kind == "expense" and not (amt < 0):
        return False
    if rule.kind == "income" and not (amt > 0):
        return False

    pm = normalize_str(row.get("payment_method"))
    if rule.payment_methods is not None and pm not in rule.payment_methods:
        return False

    acct = normalize_str(row.get("account"))
    if rule.accounts is not None and acct and acct not in rule.accounts:
        return False
    if rule.accounts is not None and not acct:
        return False

    src = normalize_str(row.get("source"))
    if rule.sources is not None and src and src not in rule.sources:
        return False
    if rule.sources is not None and not src:
        return False

    desc = normalize_str(row.get("description"))
    if not desc:
        return False

    if rule.description_contains and not any(pat in desc for pat in rule.description_contains):
        return False
    if rule.description_regex is not None and not rule.description_regex.search(desc):
        return False

    return True


def _apply(rule: CompiledRule, row: dict[str, Any]) -> bool:
    changed = False

    if rule.set_category and (rule.override or _is_blank(row.get("category"))):
        row["category"] = rule.set_category
        changed = True

    if rule.set_subcategory and (rule.override or _is_blank(row.get("subcategory"))):
        row["subcategory"] = rule.set_subcategory
        changed = True

    if rule.set_reimbursable is True:
        if int(row.get("reimbursable") or 0) != 1:
            row["reimbursable"] = 1
            changed = True
    elif rule.set_reimbursable is False and rule.override:
        if int(row.get("reimbursable") or 0) != 0:
            row["reimbursable"] = 0
            changed = True

    if rule.set_reference and (rule.override or _is_blank(row.get("reference"))):
        row["reference"] = rule.set_reference
        changed = True

    if rule.set_notes and (rule.override or _is_blank(row.get("notes"))):
        row["notes"] = rule.set_notes
        changed = True

    return changed


def apply_rules_to_rows(rows: list[dict[str, Any]], rules: list[dict[str, Any]] | None) -> int:
    compiled = compile_rules(rules)
    if not compiled:
        return 0

    updated = 0
    for row in rows:
        any_change = False
        for rule in compiled:
            if not _matches(rule, row):
                continue
            changed = _apply(rule, row)
            any_change = any_change or changed
            if changed and rule.stop:
                break
        if any_change:
            updated += 1
    return updated


def apply_rules_to_transactions(conn, rules: list[dict[str, Any]] | None) -> int:
    """
    Applies rules to existing DB transactions that are missing category/subcategory.
    Returns how many transactions were updated.
    """
    compiled = compile_rules(rules)
    if not compiled:
        return 0

    sql = """
    SELECT
        id, amount, description, payment_method, account, source,
        category, subcategory, reimbursable, reference, notes
    FROM transactions
    WHERE
      (
        payment_method = 'income'
        AND (category IS NULL OR TRIM(category) = '')
      )
      OR
      (
        amount < 0
        AND payment_method IN ('credit_card','debit','pix','transfer','cash')
        AND (
          category IS NULL OR TRIM(category) = ''
          OR subcategory IS NULL OR TRIM(subcategory) = ''
        )
      )
    ORDER BY cash_date, id
    """

    cur = conn.execute(sql)
    rows = cur.fetchall()
    updated = 0
    for r in rows:
        row = dict(r)
        before = (
            str(row.get("category") or ""),
            str(row.get("subcategory") or ""),
            int(row.get("reimbursable") or 0),
            str(row.get("reference") or ""),
            str(row.get("notes") or ""),
        )

        any_change = False
        for rule in compiled:
            if not _matches(rule, row):
                continue
            changed = _apply(rule, row)
            any_change = any_change or changed
            if changed and rule.stop:
                break

        after = (
            str(row.get("category") or ""),
            str(row.get("subcategory") or ""),
            int(row.get("reimbursable") or 0),
            str(row.get("reference") or ""),
            str(row.get("notes") or ""),
        )
        if not any_change or after == before:
            continue

        pf_db.update_transaction_categories(
            conn,
            transaction_id=int(row["id"]),
            group_name=None,
            category=str(row.get("category") or "").strip() or None,
            subcategory=str(row.get("subcategory") or "").strip() or None,
            reimbursable=bool(int(row.get("reimbursable") or 0) == 1),
            person=None,
            reference=str(row.get("reference") or "").strip() or None,
            notes=str(row.get("notes") or "").strip() or None,
        )
        updated += 1

    if updated:
        # update_transaction_categories already commits per row; this is a no-op but explicit.
        conn.commit()
    return updated

