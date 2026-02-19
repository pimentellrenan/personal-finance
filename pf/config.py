from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def repo_root() -> Path:
    here = Path(__file__).resolve()
    for parent in [here.parent] + list(here.parents):
        if (parent / "config").exists():
            return parent
    return Path.cwd()


def _load_json(path: Path, *, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default


@dataclass(frozen=True)
class CardConfig:
    id: str
    name: str
    closing_day: int
    due_day: int
    closing_day_alt: tuple[int, ...] = ()
    owner: str | None = None


def load_cards_config(base_dir: Path | None = None) -> dict[str, CardConfig]:
    base_dir = base_dir or repo_root()
    raw = _load_json(base_dir / "config" / "cards.json", default={"cards": []})
    cards: dict[str, CardConfig] = {}
    for item in raw.get("cards", []):
        card = CardConfig(
            id=str(item["id"]),
            name=str(item.get("name", item["id"])),
            closing_day=int(item["closing_day"]),
            due_day=int(item["due_day"]),
            closing_day_alt=tuple(int(x) for x in item.get("closing_day_alt", []) or ()),
            owner=str(item.get("owner") or "").strip() or None,
        )
        cards[card.id] = card
    return cards


def load_pay_schedule(base_dir: Path | None = None) -> dict[str, Any]:
    base_dir = base_dir or repo_root()
    return _load_json(base_dir / "config" / "pay_schedule.json", default={"events": []})


def load_expense_categories(base_dir: Path | None = None) -> dict[str, Any]:
    base_dir = base_dir or repo_root()
    return _load_json(base_dir / "config" / "categories_expenses.json", default={})


def load_income_categories(base_dir: Path | None = None) -> dict[str, Any]:
    base_dir = base_dir or repo_root()
    return _load_json(base_dir / "config" / "categories_income.json", default={})


def load_rules(base_dir: Path | None = None) -> dict[str, Any]:
    base_dir = base_dir or repo_root()
    return _load_json(base_dir / "config" / "rules.json", default={"rules": []})


def load_budgets(base_dir: Path | None = None) -> dict[str, Any]:
    base_dir = base_dir or repo_root()
    return _load_json(base_dir / "config" / "budgets.json", default={"budgets": {}})


def categories_triplets(categories_tree: dict[str, Any]) -> list[tuple[str, str | None, str | None]]:
    """
    Returns a flat list of valid (group, category, subcategory) combinations.

    The JSON supports:
    - group -> [category, ...]
    - group -> { category -> [subcategory, ...], ... }
    """
    out: list[tuple[str, str | None, str | None]] = []
    for group, node in categories_tree.items():
        if isinstance(node, list):
            for category in node:
                out.append((group, str(category), None))
        elif isinstance(node, dict):
            for category, subcats in node.items():
                if isinstance(subcats, list) and subcats:
                    for sub in subcats:
                        out.append((group, str(category), str(sub)))
                else:
                    out.append((group, str(category), None))
        else:
            out.append((group, None, None))
    return out
