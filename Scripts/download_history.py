from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_HISTORY_PATH = REPO_ROOT / "state" / "download_history.jsonl"
DOWNLOAD_PIPELINE_KEYS = frozenset(
    {
        "youtube",
        "twitter",
        "medios_tampico",
        "facebook_posts",
        "facebook_comentarios",
        "instagram",
        "tiktok",
    }
)
TERMINAL_STATUSES = frozenset({"completada", "fallida", "detenida"})


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00",
        "Z",
    )


def is_download_pipeline(pipeline_key: str) -> bool:
    return pipeline_key in DOWNLOAD_PIPELINE_KEYS


def append_download_record(
    *,
    pipeline_code: str,
    pipeline_key: str,
    pipeline_label: str,
    since: str,
    before: str,
    status: str,
    started_at: str,
    finished_at: str | None = None,
    output_dir: str | Path | None = None,
    return_code: int | None = None,
    history_path: str | Path = DEFAULT_HISTORY_PATH,
) -> dict[str, Any]:
    if not is_download_pipeline(pipeline_key):
        raise ValueError(f"No es un pipeline de descarga: {pipeline_key}")
    if status not in TERMINAL_STATUSES:
        raise ValueError(f"Estado de descarga inválido: {status}")

    record: dict[str, Any] = {
        "version": 1,
        "pipeline_code": str(pipeline_code),
        "pipeline_key": pipeline_key,
        "pipeline_label": pipeline_label,
        "since": since,
        "before": before,
        "interval": "[since,before)",
        "timezone": "UTC",
        "status": status,
        "started_at": started_at,
        "finished_at": finished_at or utc_now(),
        "output_dir": str(output_dir or ""),
        "return_code": return_code,
    }
    path = Path(history_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n").encode(
        "utf-8"
    )
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        remaining = memoryview(payload)
        while remaining:
            written = os.write(descriptor, remaining)
            remaining = remaining[written:]
    finally:
        os.close(descriptor)
    return record


def read_download_history(
    history_path: str | Path = DEFAULT_HISTORY_PATH,
    *,
    limit: int | None = None,
    statuses: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    path = Path(history_path)
    if not path.exists():
        return []
    allowed = set(statuses) if statuses is not None else None
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except (json.JSONDecodeError, TypeError):
                continue
            if not isinstance(record, dict):
                continue
            if allowed is not None and record.get("status") not in allowed:
                continue
            if not record.get("since") or not record.get("before"):
                continue
            records.append(record)
    if limit is not None and limit >= 0:
        return records[-limit:] if limit else []
    return records


def latest_downloads_by_pipeline(
    records: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for record in records:
        key = str(record.get("pipeline_key") or "")
        if key:
            latest[key] = record
    return sorted(
        latest.values(),
        key=lambda item: str(item.get("finished_at") or ""),
        reverse=True,
    )
