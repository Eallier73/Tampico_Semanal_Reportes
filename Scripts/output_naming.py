from __future__ import annotations

import re
import json
from datetime import date, datetime
from pathlib import Path


SPANISH_MONTHS = (
    "enero",
    "febrero",
    "marzo",
    "abril",
    "mayo",
    "junio",
    "julio",
    "agosto",
    "septiembre",
    "octubre",
    "noviembre",
    "diciembre",
)


def _normalize_source_label(source: str) -> str:
    normalized = re.sub(r"[^0-9A-Za-z]+", "_", (source or "").strip())
    return normalized.strip("_") or "Reporte"


def _as_date(value: str | date | datetime, field_name: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return datetime.strptime(str(value).strip(), "%Y-%m-%d").date()
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} debe usar YYYY-MM-DD: {value!r}") from exc


def validate_date_range(
    since: str | date | datetime,
    before: str | date | datetime,
) -> tuple[date, date]:
    """Valida el contrato temporal único del pipeline: [since, before)."""
    start = _as_date(since, "since")
    end = _as_date(before, "before")
    if end <= start:
        raise ValueError("before debe ser posterior a since; el rango es [since, before)")
    return start, end


def build_range_label(
    since: str | date | datetime,
    before: str | date | datetime,
) -> str:
    """Etiqueta legible y no ambigua que conserva ambos límites exactos."""
    start, end = validate_date_range(since, before)
    return (
        f"{start.year}_{SPANISH_MONTHS[start.month - 1]}_{start.day:02d}"
        f"_al_{end.year}_{SPANISH_MONTHS[end.month - 1]}_{end.day:02d}"
    )


def build_range_report_tag(
    since: str | date | datetime,
    before: str | date | datetime,
    source: str,
) -> str:
    return f"{build_range_label(since, before)}_{_normalize_source_label(source)}"


def build_range_output_dir(
    base_dir: str | Path,
    since: str | date | datetime,
    before: str | date | datetime,
    source: str,
) -> Path:
    return Path(base_dir) / build_range_report_tag(since, before, source)


def write_range_contract(
    output_dir: str | Path,
    since: str | date | datetime,
    before: str | date | datetime,
    source: str,
) -> Path:
    """Persiste junto a la salida los límites que debe respetar cada stage."""
    start, end = validate_date_range(since, before)
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "contrato_rango_fechas.json"
    payload = {
        "contract_version": 1,
        "since": start.isoformat(),
        "before": end.isoformat(),
        "interval": "[since,before)",
        "before_is_exclusive": True,
        "timezone": "UTC",
        "source": _normalize_source_label(source),
        "storage_tag": build_range_report_tag(start, end, source),
    }
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return path


def ensure_tagged_name(base_name: str, report_tag: str) -> str:
    if base_name.endswith(report_tag):
        return base_name
    return f"{base_name}_{report_tag}"
