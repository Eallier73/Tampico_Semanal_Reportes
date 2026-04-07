from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path


def _normalize_date_label(run_date: str | date | datetime | None, fallback: str) -> str:
    if isinstance(run_date, datetime):
        return run_date.strftime("%Y-%m-%d")
    if isinstance(run_date, date):
        return run_date.isoformat()
    if run_date:
        return str(run_date)
    return fallback


def _normalize_source_label(source: str) -> str:
    normalized = re.sub(r"[^0-9A-Za-z]+", "_", (source or "").strip())
    return normalized.strip("_") or "Reporte"


def build_report_tag(
    run_date: str | date | datetime | None,
    source: str,
    fallback: str = "sin_inicio",
) -> str:
    return f"{_normalize_date_label(run_date, fallback)}_{_normalize_source_label(source)}"


def build_output_dir(
    base_dir: str | Path,
    run_date: str | date | datetime | None,
    source: str,
    fallback: str = "sin_inicio",
) -> Path:
    return Path(base_dir) / build_report_tag(run_date, source, fallback=fallback)


def ensure_tagged_name(base_name: str, report_tag: str) -> str:
    if base_name.endswith(report_tag):
        return base_name
    return f"{base_name}_{report_tag}"
