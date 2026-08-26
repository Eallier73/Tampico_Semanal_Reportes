#!/usr/bin/env python3
"""Consolida todo el historico tabular descargado para el SNA de Tampico."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import unicodedata
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

import pandas as pd

from output_naming import (
    build_range_label,
    build_range_report_tag,
    validate_date_range,
    write_range_contract,
)


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT = REPO_ROOT / "SNA" / "Datos" / "tampico_datos_tabulares_consolidados.csv"
DEFAULT_RADAR_DIR = Path("/home/emilio/Documentos/RAdAR/Datos_RAdAR/Juntos")
SOURCE_ROOTS: list[tuple[Path, str]] = [(REPO_ROOT, "")]

URL_RE = re.compile(r"(?:https?://|www\.)[^\s<>\"']+", re.IGNORECASE)
MENTION_RE = re.compile(r"(?<!\w)@([A-Za-z0-9_]{1,50})")
HASHTAG_RE = re.compile(r"(?<!\w)#([\wÀ-ÿ]{1,80})", re.UNICODE)
EMOJI_RE = re.compile(
    "["
    "\U0001F300-\U0001F5FF\U0001F600-\U0001F64F"
    "\U0001F680-\U0001F6FF\U0001F900-\U0001F9FF"
    "\U0001FA70-\U0001FAFF\U00002600-\U000026FF"
    "\U00002700-\U000027BF\U0001F1E0-\U0001F1FF"
    "]+",
    flags=re.UNICODE,
)
DATE_PREFIX_RE = re.compile(r"(20\d{2}-\d{2}-\d{2})")
INSTITUTIONAL_HANDLES = {"monicavtampico", "tampicogob"}
MEDIA_NAME_ALIASES = {
    "el_sol_de_mexico": "El Sol de México",
    "milenio": "Milenio",
}

OUTPUT_COLUMNS = [
    "id", "plataforma", "tipo_registro", "usuario", "rango", "rangos_origen",
    "fecha", "texto_original", "texto_limpio", "urls_extraidas",
    "menciones_extraidas", "hashtags_extraidos", "emojis_extraidos",
    "idioma_detectado", "likes", "comentarios", "shares", "vistas", "es_reply",
    "url_origen", "url_contexto", "query_busqueda", "titulo_contexto",
    "autor_contexto", "archivo_origen", "archivos_origen", "ruta_origen",
    "n_apariciones_descarga", "clave_deduplicacion", "datos_originales_json",
]


def text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def integer(value: Any) -> int:
    try:
        if value is None or pd.isna(value):
            return 0
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def boolean(value: Any) -> bool:
    return text(value).lower() in {"1", "true", "si", "sí", "yes", "y", "t"}


def first(row: pd.Series, *names: str) -> str:
    for name in names:
        if name in row:
            value = text(row.get(name))
            if value:
                return value
    return ""


def normalize_date(value: Any) -> str:
    raw = text(value)
    if not raw:
        return ""
    parsed = pd.to_datetime(raw, errors="coerce", utc=True)
    if pd.isna(parsed):
        return raw
    return parsed.isoformat().replace("+00:00", "Z")


def source_relative(path: Path) -> Path:
    for root, label in SOURCE_ROOTS:
        try:
            relative = path.relative_to(root)
        except ValueError:
            continue
        return Path(label) / relative if label else relative
    return Path("externo") / path.name


def date_or_source_date(value: Any, path: Path) -> str:
    raw = text(value)
    parsed = pd.to_datetime(raw, errors="coerce", utc=True)
    if raw and not pd.isna(parsed):
        return raw
    match = DATE_PREFIX_RE.search(str(path))
    return match.group(1) if match else raw


def range_from_date(value: Any) -> str:
    """Representa la fecha observable como un rango diario exacto."""
    parsed = pd.to_datetime(text(value), errors="coerce", utc=True)
    if pd.isna(parsed):
        return "sin_rango_verificable"
    since = parsed.date()
    before = since + pd.Timedelta(days=1)
    return build_range_label(since, before)


def stable_hash(*values: Any) -> str:
    raw = "|".join(text(value) for value in values)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def slug(value: Any) -> str:
    normalized = unicodedata.normalize("NFKD", text(value).lower())
    ascii_text = "".join(char for char in normalized if not unicodedata.combining(char))
    return re.sub(r"[^a-z0-9]+", "_", ascii_text).strip("_") or "sin_query"


def clean_and_extract(value: Any) -> dict[str, str]:
    raw = text(value)
    clean_chars = []
    for char in raw.replace("\ufffd", " "):
        if unicodedata.category(char).startswith("C") and char not in "\t\n\r":
            clean_chars.append(" ")
        else:
            clean_chars.append(char)
    clean = "".join(clean_chars)

    def unique(values: list[str]) -> list[str]:
        return list(dict.fromkeys(v for v in values if v))

    urls = unique([u.rstrip(".,;:!?)") for u in URL_RE.findall(clean)])
    mentions = unique(MENTION_RE.findall(clean))
    hashtags = unique(HASHTAG_RE.findall(clean))
    emojis = unique(EMOJI_RE.findall(clean))
    return {
        "texto_limpio": clean,
        "urls_extraidas": " ".join(urls),
        "menciones_extraidas": " ".join(mentions),
        "hashtags_extraidos": " ".join(hashtags),
        "emojis_extraidos": " ".join(emojis),
    }


def raw_json(row: pd.Series) -> str:
    data = {}
    for key, value in row.items():
        if value is None or pd.isna(value):
            data[str(key)] = None
        elif hasattr(value, "item"):
            data[str(key)] = value.item()
        else:
            data[str(key)] = value
    return json.dumps(data, ensure_ascii=False, default=str, separators=(",", ":"))


def base_record(
    row: pd.Series,
    path: Path,
    plataforma: str,
    tipo_registro: str,
    usuario: str,
    fecha: str,
    contenido: str,
    stable_key: str,
    **extra: Any,
) -> dict[str, Any]:
    relative = source_relative(path)
    record_range = range_from_date(fecha)
    clean = clean_and_extract(contenido)
    fallback_key = "|".join([plataforma, tipo_registro, usuario, fecha, contenido])
    dedup_key = f"{plataforma}:{tipo_registro}:{stable_key or hashlib.sha1(fallback_key.encode('utf-8')).hexdigest()}"
    record = {
        "plataforma": plataforma,
        "tipo_registro": tipo_registro,
        "usuario": usuario,
        "rango": record_range,
        "fecha": normalize_date(fecha),
        "texto_original": contenido,
        **clean,
        "idioma_detectado": "indeterminado",
        "likes": 0,
        "comentarios": 0,
        "shares": 0,
        "vistas": 0,
        "es_reply": False,
        "url_origen": "",
        "url_contexto": "",
        "query_busqueda": "",
        "titulo_contexto": "",
        "autor_contexto": "",
        "archivo_origen": path.name,
        "ruta_origen": str(relative),
        "clave_deduplicacion": dedup_key,
        "datos_originales_json": raw_json(row),
    }
    record.update(extra)
    return record


def adapt_twitter(row: pd.Series, path: Path, institutional: bool) -> dict[str, Any]:
    return base_record(
        row, path, "Twitter", "publicacion_institucional" if institutional else "comentario",
        first(row, "author"), first(row, "datetime_parsed_utc", "datetime"), first(row, "text"),
        first(row, "url"), likes=integer(row.get("likes")), comentarios=integer(row.get("replies")),
        shares=integer(row.get("retweets")), vistas=integer(row.get("views")),
        es_reply=boolean(row.get("is_reply")), url_origen=first(row, "url"),
        url_contexto=first(row, "in_reply_to_url"), query_busqueda=first(row, "query_used"),
    )


def adapt_facebook_comment(row: pd.Series, path: Path) -> dict[str, Any]:
    return base_record(
        row, path, "Facebook", "comentario", first(row, "autor"),
        first(row, "fecha_comentario"), first(row, "comentario_texto"),
        first(row, "url_comentario"), likes=integer(row.get("likes_comentario")),
        es_reply=boolean(row.get("es_respuesta")), url_origen=first(row, "url_comentario"),
        url_contexto=first(row, "post_url"),
    )


def adapt_facebook_post(row: pd.Series, path: Path) -> dict[str, Any]:
    return base_record(
        row, path, "Facebook", "publicacion_institucional",
        first(row, "autor", "page_handle"), first(row, "fecha_post", "fecha_post_date"),
        first(row, "post_texto"), first(row, "post_url"),
        likes=integer(row.get("reacciones_post")), comentarios=integer(row.get("num_comentarios_post")),
        url_origen=first(row, "post_url"), url_contexto=first(row, "page_url"),
        autor_contexto=first(row, "page_handle"),
    )


def adapt_youtube_comment(row: pd.Series, path: Path) -> dict[str, Any]:
    video_id = first(row, "video_id")
    comment_id = first(row, "comment_id")
    video_url = f"https://www.youtube.com/watch?v={video_id}" if video_id else ""
    comment_url = f"{video_url}&lc={comment_id}" if video_url and comment_id else comment_id
    return base_record(
        row, path, "YouTube", "comentario", first(row, "author"),
        first(row, "published_at"), first(row, "comment_text"), comment_id,
        likes=integer(row.get("like_count")), url_origen=comment_url, url_contexto=video_url,
        query_busqueda=first(row, "query"), titulo_contexto=first(row, "video_title"),
        autor_contexto=first(row, "channel_title"),
    )


def adapt_youtube_script(row: pd.Series, path: Path) -> dict[str, Any]:
    video_id = first(row, "video_id")
    video_url = f"https://www.youtube.com/watch?v={video_id}" if video_id else ""
    return base_record(
        row, path, "YouTube", "transcripcion", first(row, "channel_handle", "channel_title"),
        first(row, "video_published_at"), first(row, "transcript_text"), video_id,
        url_origen=video_url, titulo_contexto=first(row, "video_title"),
        autor_contexto=first(row, "channel_title"),
    )


def media_name(row: pd.Series) -> str:
    raw_name = first(row, "fuente")
    if raw_name:
        return MEDIA_NAME_ALIASES.get(slug(raw_name), raw_name)

    raw_url = first(row, "url", "url_google")
    hostname = urlparse(raw_url).hostname or ""
    return hostname.removeprefix("www.") or "Medio sin identificar"


def adapt_medio(row: pd.Series, path: Path) -> dict[str, Any]:
    url = first(row, "url", "url_google")
    titulo = first(row, "titulo")
    contenido = first(row, "texto") or titulo
    nombre_medio = media_name(row)
    return base_record(
        row, path, "Medios", "articulo", nombre_medio,
        first(row, "iso_date", "fecha"), contenido,
        url or stable_hash(nombre_medio, row.get("iso_date"), titulo, contenido),
        url_origen=url, url_contexto=url, titulo_contexto=titulo,
        autor_contexto=first(row, "autor"),
        query_busqueda=first(row, "origen"),
    )


def adapt_radar_facebook_legacy(row: pd.Series, path: Path) -> dict[str, Any]:
    contenido = first(row, "message")
    query_type = first(row, "query_type")
    level = integer(row.get("level"))
    is_comment = "comments" in query_type.lower() or level > 1
    object_id = first(row, "object_id")
    synthetic_url = f"radar://facebook/{object_id}" if object_id else ""
    return base_record(
        row, path, "Facebook", "comentario" if is_comment else "publicacion_institucional",
        "", date_or_source_date(row.get("created_time"), path), contenido,
        first(row, "path") or stable_hash(row.get("id"), row.get("created_time"), contenido),
        likes=integer(row.get("like_count")), comentarios=integer(row.get("comment_count")),
        url_origen=synthetic_url, url_contexto=synthetic_url,
        query_busqueda=query_type, autor_contexto="RAdAR Facebook",
    )


def adapt_radar_facebook_actual(row: pd.Series, path: Path) -> dict[str, Any]:
    tipo_raw = first(row, "tipo").upper()
    is_post = tipo_raw == "POST"
    contenido = first(row, "texto")
    url = first(row, "url")
    parent = first(row, "post_url_padre") or url
    fecha = date_or_source_date(row.get("fecha"), path)
    stable = stable_hash(tipo_raw, url, parent, fecha, contenido)
    return base_record(
        row, path, "Facebook", "publicacion_institucional" if is_post else "comentario",
        "", fecha, contenido, stable,
        comentarios=integer(row.get("num_comentarios")), url_origen=url,
        url_contexto=parent, autor_contexto="RAdAR Facebook",
    )


def _radar_twitter_institutional(handle: str) -> bool:
    return handle.lower().lstrip("@").strip() in INSTITUTIONAL_HANDLES


def adapt_radar_twitter_legacy(row: pd.Series, path: Path) -> dict[str, Any]:
    usuario = first(row, "Author_Handle", "Author_Name")
    contenido = first(row, "Tweet_Content")
    url = first(row, "Tweet_URL")
    stable = url or first(row, "Post_ID") or stable_hash(usuario, row.get("UTC_Time"), contenido)
    institutional = _radar_twitter_institutional(usuario)
    return base_record(
        row, path, "Twitter", "publicacion_institucional" if institutional else "comentario",
        usuario, date_or_source_date(row.get("UTC_Time"), path), contenido, stable,
        likes=integer(row.get("Like_Count")), comentarios=integer(row.get("Reply_Count")),
        shares=integer(row.get("Repost_Count")), vistas=integer(row.get("View_Count")),
        es_reply=boolean(row.get("Replying_to")), url_origen=url, url_contexto=url,
        query_busqueda=first(row, "Query_Str"),
        idioma_detectado=first(row, "Language") or "indeterminado",
    )


def adapt_radar_twitter_actual(row: pd.Series, path: Path) -> dict[str, Any]:
    usuario = first(row, "author")
    return adapt_twitter(row, path, _radar_twitter_institutional(usuario))


def adapt_radar_youtube_simple(row: pd.Series, path: Path) -> dict[str, Any]:
    contenido = first(row, "comment_text")
    fecha = date_or_source_date(row.get("published_at"), path)
    query = first(row, "query")
    range_label = range_from_date(fecha)
    context = f"radar://youtube/{range_label}/{slug(query)}"
    stable = stable_hash(fecha, contenido.lower(), query.lower())
    return base_record(
        row, path, "YouTube", "comentario", "", fecha, contenido, stable,
        url_contexto=context, query_busqueda=query,
        autor_contexto="RAdAR YouTube sin metadata",
    )


def adapt_radar_youtube_full(row: pd.Series, path: Path) -> dict[str, Any]:
    video_id = first(row, "video_id")
    comment_id = first(row, "comment_id")
    video_url = f"https://www.youtube.com/watch?v={video_id}" if video_id else ""
    comment_url = f"{video_url}&lc={comment_id}" if video_url and comment_id else comment_id
    contenido = first(row, "comment_text")
    return base_record(
        row, path, "YouTube", "comentario", first(row, "author"),
        date_or_source_date(row.get("published_at"), path), contenido,
        comment_id or stable_hash(row.get("published_at"), contenido, row.get("query")),
        likes=integer(row.get("like_count")), url_origen=comment_url,
        url_contexto=video_url or "radar://youtube/sin_rango_verificable/sin_video",
        query_busqueda=first(row, "query"), titulo_contexto=first(row, "video_title"),
        autor_contexto=first(row, "channel_title"),
    )


def adapt_radar_recovered(
    row: pd.Series, path: Path, plataforma: str, line_number: int
) -> dict[str, Any]:
    contenido = first(row, "texto_recuperado")
    context = (
        f"radar://recuperado/{plataforma.lower()}/"
        f"sin_rango_verificable/{path.stem}"
    )
    return base_record(
        row, path, plataforma, "comentario", "", date_or_source_date("", path),
        contenido, stable_hash(path.name, line_number, contenido),
        url_contexto=context, query_busqueda="recuperacion_sin_encabezado",
        autor_contexto="RAdAR recuperacion parcial",
    )


def adapt_apify_social(
    row: pd.Series, path: Path, plataforma: str
) -> dict[str, Any]:
    return base_record(
        row,
        path,
        plataforma,
        first(row, "tipo_registro") or "mencion",
        first(row, "usuario"),
        first(row, "fecha"),
        first(row, "texto"),
        first(row, "id", "url"),
        likes=integer(row.get("likes")),
        comentarios=integer(row.get("comentarios")),
        shares=integer(row.get("shares")),
        vistas=integer(row.get("vistas")),
        es_reply=first(row, "tipo_registro") == "comentario",
        url_origen=first(row, "url"),
        url_contexto=first(row, "url_contexto", "input_url"),
        query_busqueda=first(row, "query_busqueda"),
        datos_originales_json=first(row, "datos_originales_json") or raw_json(row),
    )


SOURCES: list[tuple[str, str, Callable[[pd.Series, Path], dict[str, Any]]]] = [
    ("Twitter comentarios", "Twitter/*/*_comentarios.csv", lambda r, p: adapt_twitter(r, p, False)),
    ("Twitter institucionales", "Twitter/*/*_post_institucionales.csv", lambda r, p: adapt_twitter(r, p, True)),
    ("Facebook comentarios", "Facebook/*/*_comentarios.csv", adapt_facebook_comment),
    ("Facebook posts", "Facebook/*/*_posts.csv", adapt_facebook_post),
    ("YouTube comentarios", "Youtube/*/*_comentarios.csv", adapt_youtube_comment),
    ("YouTube transcripciones", "Youtube/*/*_scripts.csv", adapt_youtube_script),
    ("Medios", "Medios/*/*_Medios.csv", adapt_medio),
    (
        "Instagram",
        "Instagram/*/*_publicaciones.csv",
        lambda r, p: adapt_apify_social(r, p, "Instagram"),
    ),
    (
        "TikTok",
        "TikTok/*/*_publicaciones.csv",
        lambda r, p: adapt_apify_social(r, p, "TikTok"),
    ),
]


def read_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, encoding="utf-8-sig", low_memory=False, on_bad_lines="skip")
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="latin-1", low_memory=False, on_bad_lines="skip")


def read_radar_csv(path: Path, plataforma: str) -> tuple[pd.DataFrame, str]:
    try:
        frame = read_csv(path)
    except Exception:
        if plataforma == "Facebook":
            try:
                frame = pd.read_csv(
                    path, encoding="utf-8-sig", sep=";", engine="python",
                    on_bad_lines="skip",
                )
            except Exception:
                frame = pd.DataFrame()
        else:
            frame = pd.DataFrame()

    if plataforma == "Facebook" and len(frame.columns) == 1:
        try:
            frame = pd.read_csv(
                path, encoding="utf-8-sig", sep=";", engine="python",
                on_bad_lines="skip",
            )
        except Exception:
            frame = pd.DataFrame()

    if plataforma == "Facebook" and "message" in frame.columns:
        return frame, "facebook_legacy"
    if plataforma == "Facebook" and "texto" in frame.columns:
        return frame, "facebook_actual"
    if plataforma == "Twitter" and "Tweet_Content" in frame.columns:
        return frame, "twitter_legacy"
    if plataforma == "Twitter" and "text" in frame.columns:
        return frame, "twitter_actual"
    if plataforma == "YouTube" and "video_id" in frame.columns:
        return frame, "youtube_full"
    if plataforma == "YouTube" and "comment_text" in frame.columns:
        return frame, "youtube_simple"

    lines = [
        line.lstrip("\ufeff").strip()
        for line in path.read_text(encoding="utf-8-sig", errors="ignore").splitlines()
        if line.strip()
    ]
    return pd.DataFrame({"texto_recuperado": lines}), "recuperado_sin_encabezado"


def consolidate_radar(
    radar_dir: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    records: list[dict[str, Any]] = []
    inventory: list[dict[str, Any]] = []
    adapters: dict[str, Callable[[pd.Series, Path], dict[str, Any]]] = {
        "facebook_legacy": adapt_radar_facebook_legacy,
        "facebook_actual": adapt_radar_facebook_actual,
        "twitter_legacy": adapt_radar_twitter_legacy,
        "twitter_actual": adapt_radar_twitter_actual,
        "youtube_full": adapt_radar_youtube_full,
        "youtube_simple": adapt_radar_youtube_simple,
    }

    for plataforma, suffix in (
        ("Facebook", "*_facebook.csv"),
        ("Twitter", "*_twitter.csv"),
        ("YouTube", "*_youtube.csv"),
    ):
        for path in sorted(radar_dir.rglob(suffix)):
            frame, schema = read_radar_csv(path, plataforma)
            useful = 0
            if schema == "recuperado_sin_encabezado":
                for line_number, (_, row) in enumerate(frame.iterrows(), 1):
                    record = adapt_radar_recovered(row, path, plataforma, line_number)
                    if text(record.get("texto_original")):
                        records.append(record)
                        useful += 1
            else:
                adapter = adapters[schema]
                for _, row in frame.iterrows():
                    record = adapter(row, path)
                    if text(record.get("texto_original")):
                        records.append(record)
                        useful += 1
            inventory.append({
                "familia": f"RAdAR {schema}",
                "archivo": str(source_relative(path)),
                "filas": len(frame),
                "filas_utiles": useful,
            })
    return records, inventory


def consolidate(
    radar_dir: Path | None = None,
    since: str | None = None,
    before: str | None = None,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    if bool(since) != bool(before):
        raise ValueError("--since y --before deben enviarse juntos")
    if since and before:
        validate_date_range(since, before)
        range_start = pd.Timestamp(since, tz="UTC")
        range_end = pd.Timestamp(before, tz="UTC")
    else:
        range_start = None
        range_end = None

    def belongs_to_exact_range(record: dict[str, Any]) -> bool:
        if range_start is None or range_end is None:
            return True
        parsed = pd.to_datetime(text(record.get("fecha")), errors="coerce", utc=True)
        return not pd.isna(parsed) and range_start <= parsed < range_end

    records: list[dict[str, Any]] = []
    inventory: list[dict[str, Any]] = []
    for family, pattern, adapter in SOURCES:
        paths = sorted(REPO_ROOT.glob(pattern))
        for path in paths:
            frame = read_csv(path)
            inventory.append({"familia": family, "archivo": str(path.relative_to(REPO_ROOT)), "filas": len(frame)})
            for _, row in frame.iterrows():
                record = adapter(row, path)
                if text(record.get("texto_original")) and belongs_to_exact_range(record):
                    records.append(record)

    if radar_dir and radar_dir.exists():
        SOURCE_ROOTS.append((radar_dir, "RAdAR/Juntos"))
        radar_records, radar_inventory = consolidate_radar(radar_dir)
        records.extend(
            record for record in radar_records if belongs_to_exact_range(record)
        )
        inventory.extend(radar_inventory)

    if not records:
        return pd.DataFrame(columns=OUTPUT_COLUMNS), inventory

    raw = pd.DataFrame(records)
    consolidated: list[dict[str, Any]] = []
    for _, group in raw.groupby("clave_deduplicacion", sort=False, dropna=False):
        selected = group.iloc[-1].to_dict()
        source_ranges = sorted({text(v) for v in group["rango"] if text(v)})
        files = list(dict.fromkeys(text(v) for v in group["archivo_origen"] if text(v)))
        selected["rangos_origen"] = "|".join(source_ranges)
        selected["archivos_origen"] = "|".join(files)
        selected["n_apariciones_descarga"] = len(group)
        selected["id"] = hashlib.sha1(str(selected["clave_deduplicacion"]).encode("utf-8")).hexdigest()[:20]
        consolidated.append(selected)

    output = pd.DataFrame(consolidated)
    for column in OUTPUT_COLUMNS:
        if column not in output:
            output[column] = ""
    output = output[OUTPUT_COLUMNS]
    output = output.sort_values(["fecha", "plataforma", "tipo_registro", "id"], na_position="last").reset_index(drop=True)
    return output, inventory


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--since", help="Límite inicial inclusivo YYYY-MM-DD")
    parser.add_argument("--before", help="Límite final exclusivo YYYY-MM-DD")
    parser.add_argument("--radar-dir", type=Path, default=DEFAULT_RADAR_DIR)
    parser.add_argument(
        "--sin-radar", action="store_true",
        help="Consolida solo las fuentes del repo de Tampico",
    )
    args = parser.parse_args()
    if bool(args.since) != bool(args.before):
        parser.error("--since y --before deben enviarse juntos")
    if args.since and args.before:
        try:
            validate_date_range(args.since, args.before)
        except ValueError as exc:
            parser.error(str(exc))
    output_path = args.output
    if output_path is None:
        if args.since and args.before:
            range_label = build_range_label(args.since, args.before)
            output_path = (
                REPO_ROOT
                / "SNA"
                / "Datos"
                / build_range_report_tag(args.since, args.before, "SNA")
                / f"tampico_datos_tabulares_{range_label}.csv"
            )
        else:
            output_path = DEFAULT_OUTPUT

    radar_dir = (
        None
        if args.sin_radar or args.since
        else args.radar_dir
    )
    if radar_dir and not radar_dir.exists():
        print(f"[AVISO] No existe RAdAR: {radar_dir}; se continua sin esa fuente")
        radar_dir = None
    output, inventory = consolidate(
        radar_dir=radar_dir,
        since=args.since,
        before=args.before,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(output_path, index=False, encoding="utf-8")
    if args.since and args.before:
        write_range_contract(
            output_path.parent,
            args.since,
            args.before,
            "SNA",
        )

    print(f"Archivo: {output_path}")
    if args.since and args.before:
        scope_label = f"[{args.since}, {args.before}) (solo fuentes locales)"
    elif radar_dir:
        scope_label = "histórico local + RAdAR"
    else:
        scope_label = "histórico local"
    print(f"Alcance: {scope_label}")
    print(f"Archivos fuente: {len(inventory)}")
    print(f"Filas fuente: {sum(item['filas'] for item in inventory)}")
    if radar_dir:
        radar_items = [item for item in inventory if item["familia"].startswith("RAdAR ")]
        print(f"Archivos RAdAR: {len(radar_items)}")
        print(f"Filas utiles RAdAR: {sum(item.get('filas_utiles', 0) for item in radar_items)}")
    print(f"Filas consolidadas: {len(output)}")
    print("Por plataforma:")
    for platform, count in output["plataforma"].value_counts().items():
        print(f"  {platform}: {count}")
    print("Por tipo:")
    for kind, count in output["tipo_registro"].value_counts().items():
        print(f"  {kind}: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
