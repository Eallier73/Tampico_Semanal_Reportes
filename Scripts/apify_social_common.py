#!/usr/bin/env python3
"""Utilidades compartidas para extractores sociales basados en Apify."""

from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent
SOCIAL_COLUMNS = [
    "id",
    "tipo_registro",
    "origen_busqueda",
    "query_busqueda",
    "input_url",
    "usuario",
    "fecha",
    "texto",
    "url",
    "url_contexto",
    "likes",
    "comentarios",
    "shares",
    "vistas",
    "es_institucional",
    "datos_originales_json",
]


@dataclass(frozen=True)
class ActorRunPlan:
    name: str
    actor_input: dict[str, Any]
    institutional: bool = False
    query: str = ""


def load_repo_env() -> None:
    """Carga .env.local para ejecución directa sin imprimir secretos."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    env_path = REPO_ROOT / ".env.local"
    if env_path.exists():
        load_dotenv(env_path)


def valid_date(value: str) -> str:
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Fecha inválida '{value}', usa YYYY-MM-DD"
        ) from exc
    return value


def validate_window(since: str, before: str) -> None:
    if datetime.fromisoformat(before) <= datetime.fromisoformat(since):
        raise ValueError("--before debe ser posterior a --since")


def normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def normalize_datetime(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, (int, float)):
        raw = float(value)
        if raw > 10_000_000_000:
            raw /= 1000
        return datetime.fromtimestamp(raw, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(parsed):
        return str(value)
    return parsed.isoformat().replace("+00:00", "Z")


def in_window(value: Any, since: str, before: str) -> bool:
    normalized = normalize_datetime(value)
    parsed = pd.to_datetime(normalized, errors="coerce", utc=True)
    if pd.isna(parsed):
        return False
    start = pd.Timestamp(since, tz="UTC")
    end = pd.Timestamp(before, tz="UTC")
    return start <= parsed < end


def integer(value: Any) -> int:
    try:
        if value in (None, ""):
            return 0
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def first(item: dict[str, Any], *paths: str) -> Any:
    def present(value: Any) -> bool:
        if value is None or value == "":
            return False
        if isinstance(value, (list, dict)) and not value:
            return False
        return True

    for path in paths:
        if path in item and present(item[path]):
            return item[path]
        current: Any = item
        for part in path.split("."):
            if not isinstance(current, dict) or part not in current:
                current = None
                break
            current = current[part]
        if present(current):
            return current
    return ""


def run_actor_plans(
    actor_id: str,
    plans: Iterable[ActorRunPlan],
    token: str,
) -> list[tuple[ActorRunPlan, dict[str, Any]]]:
    from apify_client import ApifyClient

    client = ApifyClient(token)
    collected: list[tuple[ActorRunPlan, dict[str, Any]]] = []
    for plan in plans:
        print(f"▶ Apify: {plan.name}")
        run = client.actor(actor_id).call(run_input=plan.actor_input)
        dataset_id = run.get("defaultDatasetId") if run else None
        if not dataset_id:
            print(f"  ⚠️ Sin dataset: {plan.name}")
            continue
        items = list(client.dataset(dataset_id).iterate_items())
        print(f"  ✅ {len(items)} resultados")
        collected.extend((plan, item) for item in items)
    return collected


def print_dry_run(
    actor_id: str,
    plans: Iterable[ActorRunPlan],
    output_dir: Path,
    report_tag: str,
) -> None:
    print("=" * 72)
    print("DRY RUN · NO SE LLAMARÁ A APIFY · COSTO $0")
    print("=" * 72)
    print(f"Actor: {actor_id}")
    print(f"Salida prevista: {output_dir / report_tag}")
    for index, plan in enumerate(plans, start=1):
        print(f"\n[{index}] {plan.name}")
        print(json.dumps(plan.actor_input, ensure_ascii=False, indent=2))


def deduplicate_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    unique: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(row.get("id") or row.get("url") or "").strip()
        if not key:
            key = "|".join(
                str(row.get(name, "")) for name in ("usuario", "fecha", "texto")
            )
        unique.setdefault(key, row)
    return list(unique.values())


def write_social_outputs(
    rows: Iterable[dict[str, Any]],
    output_base: Path,
    report_tag: str,
) -> dict[str, Path]:
    output_dir = output_base / report_tag
    output_dir.mkdir(parents=True, exist_ok=True)
    normalized = deduplicate_rows(rows)
    frame = pd.DataFrame(normalized)
    for column in SOCIAL_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
    frame = frame[SOCIAL_COLUMNS]

    csv_path = output_dir / f"{report_tag}_publicaciones.csv"
    frame.to_csv(csv_path, index=False, encoding="utf-8-sig")

    def write_text(name: str, mask: Callable[[pd.DataFrame], pd.Series]) -> Path:
        path = output_dir / f"{report_tag}_{name}.txt"
        subset = frame.loc[mask(frame), "texto"].dropna().astype(str)
        lines = [normalize_text(value) for value in subset if normalize_text(value)]
        path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
        return path

    institutional_path = write_text(
        "posts_institucionales",
        lambda df: df["es_institucional"].astype(str).str.lower().isin({"true", "1"}),
    )
    mentions_path = write_text(
        "menciones",
        lambda df: ~df["es_institucional"].astype(str).str.lower().isin({"true", "1"})
        & (df["tipo_registro"] != "comentario"),
    )
    comments_path = write_text(
        "comentarios", lambda df: df["tipo_registro"] == "comentario"
    )
    return {
        "csv": csv_path,
        "institucionales": institutional_path,
        "menciones": mentions_path,
        "comentarios": comments_path,
    }


def json_original(item: dict[str, Any]) -> str:
    return json.dumps(item, ensure_ascii=False, default=str, separators=(",", ":"))


def require_token(explicit: str = "") -> str:
    token = explicit or os.getenv("APIFY_TOKEN", "")
    if not token:
        raise RuntimeError("Falta APIFY_TOKEN en .env.local, entorno o --token")
    return token
