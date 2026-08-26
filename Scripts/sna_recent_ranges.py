from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Iterable

from output_naming import build_range_label, validate_date_range


SOURCE_DIRS = ("Twitter", "Facebook", "Youtube", "Medios", "Instagram", "TikTok")
DATE_COLUMNS = (
    "datetime_parsed_utc",
    "datetime",
    "fecha_post",
    "fecha_post_date",
    "fecha_comentario",
    "published_at",
    "video_published_at",
    "iso_date",
    "fecha",
)


@dataclass(frozen=True)
class SourceRange:
    since: date
    before: date
    identity: str
    sources: tuple[str, ...]
    inferred_from_rows: bool


@dataclass(frozen=True)
class RecentScope:
    since: date
    before: date
    selected_ranges: tuple[SourceRange, ...]


def _parse_datetime(value: object) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        number = float(raw)
    except ValueError:
        number = None
    if number is not None and raw.replace(".", "", 1).isdigit():
        if number > 10_000_000_000:
            number /= 1000
        try:
            return datetime.fromtimestamp(number, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    normalized = raw.replace("Z", "+00:00") if raw.endswith("Z") else raw
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            parsed = parsedate_to_datetime(raw)
        except (TypeError, ValueError, OverflowError):
            for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y"):
                try:
                    parsed = datetime.strptime(raw, fmt)
                    break
                except ValueError:
                    continue
            else:
                return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _read_contract(folder: Path) -> tuple[date, date] | None:
    path = folder / "contrato_rango_fechas.json"
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        since, before = validate_date_range(payload.get("since"), payload.get("before"))
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return None
    return since, before


def _iter_csv_datetimes(paths: Iterable[Path]) -> Iterable[datetime]:
    for path in paths:
        handle = None
        for encoding in ("utf-8-sig", "latin-1"):
            try:
                handle = path.open("r", encoding=encoding, errors="strict", newline="")
                reader = csv.DictReader(handle)
                columns = [column for column in DATE_COLUMNS if column in (reader.fieldnames or [])]
                if not columns:
                    handle.close()
                    handle = None
                    break
                for row in reader:
                    for column in columns:
                        parsed = _parse_datetime(row.get(column))
                        if parsed is not None:
                            yield parsed
                            break
                handle.close()
                handle = None
                break
            except UnicodeDecodeError:
                if handle is not None:
                    handle.close()
                    handle = None
                continue
            finally:
                if handle is not None:
                    handle.close()


def discover_source_ranges(repo_root: str | Path) -> list[SourceRange]:
    root = Path(repo_root)
    ranges: list[SourceRange] = []
    inferred_batches: dict[str, dict[str, object]] = {}

    for source in SOURCE_DIRS:
        source_root = root / source
        if not source_root.exists():
            continue
        for folder in sorted(path for path in source_root.iterdir() if path.is_dir()):
            csv_paths = sorted(folder.rglob("*.csv"))
            if not csv_paths:
                continue
            contract = _read_contract(folder)
            if contract is not None:
                since, before = contract
                ranges.append(
                    SourceRange(
                        since=since,
                        before=before,
                        identity=build_range_label(since, before),
                        sources=(source,),
                        inferred_from_rows=False,
                    )
                )
                continue
            source_suffix = f"_{source}"
            batch_key = (
                folder.name[:-len(source_suffix)]
                if folder.name.endswith(source_suffix)
                else folder.name
            )
            batch = inferred_batches.setdefault(
                batch_key,
                {"paths": [], "sources": set()},
            )
            paths = batch["paths"]
            sources = batch["sources"]
            assert isinstance(paths, list)
            assert isinstance(sources, set)
            paths.extend(csv_paths)
            sources.add(source)

    for batch in inferred_batches.values():
        paths = batch["paths"]
        sources = batch["sources"]
        assert isinstance(paths, list)
        assert isinstance(sources, set)
        observed_dates = [value.date() for value in _iter_csv_datetimes(paths)]
        if not observed_dates:
            continue
        since = min(observed_dates)
        before = max(observed_dates) + timedelta(days=1)
        ranges.append(
            SourceRange(
                since=since,
                before=before,
                identity=build_range_label(since, before),
                sources=tuple(sorted(sources)),
                inferred_from_rows=True,
            )
        )

    merged: dict[tuple[date, date], SourceRange] = {}
    for item in ranges:
        key = (item.since, item.before)
        previous = merged.get(key)
        if previous is None:
            merged[key] = item
            continue
        inferred = previous.inferred_from_rows and item.inferred_from_rows
        merged[key] = SourceRange(
            since=item.since,
            before=item.before,
            identity=(
                previous.identity
                if not previous.inferred_from_rows
                else item.identity
            ),
            sources=tuple(sorted(set(previous.sources) | set(item.sources))),
            inferred_from_rows=inferred,
        )

    return sorted(
        merged.values(),
        key=lambda item: (item.before, item.since, item.identity),
    )


def resolve_recent_scope(repo_root: str | Path, count: int) -> RecentScope:
    if count < 1:
        raise ValueError("count debe ser mayor que cero")
    available = discover_source_ranges(repo_root)
    if len(available) < count:
        raise RuntimeError(
            f"Se solicitaron {count} rangos recientes, pero solo hay "
            f"{len(available)} rango(s) con fechas verificables."
        )
    selected = tuple(available[-count:])
    return RecentScope(
        since=min(item.since for item in selected),
        before=max(item.before for item in selected),
        selected_ranges=selected,
    )
