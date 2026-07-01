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

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT = REPO_ROOT / "SNA" / "Datos" / "tampico_datos_tabulares_consolidados.csv"

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
WEEK_RE = re.compile(r"(20\d{2}_W\d{2})")

OUTPUT_COLUMNS = [
    "id", "plataforma", "tipo_registro", "usuario", "semana", "semanas_origen",
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
    relative = path.relative_to(REPO_ROOT)
    week_match = WEEK_RE.search(str(relative))
    week = week_match.group(1) if week_match else ""
    clean = clean_and_extract(contenido)
    fallback_key = "|".join([plataforma, tipo_registro, usuario, fecha, contenido])
    dedup_key = f"{plataforma}:{tipo_registro}:{stable_key or hashlib.sha1(fallback_key.encode('utf-8')).hexdigest()}"
    record = {
        "plataforma": plataforma,
        "tipo_registro": tipo_registro,
        "usuario": usuario,
        "semana": week,
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


SOURCES: list[tuple[str, str, Callable[[pd.Series, Path], dict[str, Any]]]] = [
    ("Twitter comentarios", "Twitter/*/*_comentarios.csv", lambda r, p: adapt_twitter(r, p, False)),
    ("Twitter institucionales", "Twitter/*/*_post_institucionales.csv", lambda r, p: adapt_twitter(r, p, True)),
    ("Facebook comentarios", "Facebook/*/*_comentarios.csv", adapt_facebook_comment),
    ("Facebook posts", "Facebook/*/*_posts.csv", adapt_facebook_post),
    ("YouTube comentarios", "Youtube/*/*_comentarios.csv", adapt_youtube_comment),
    ("YouTube transcripciones", "Youtube/*/*_scripts.csv", adapt_youtube_script),
]


def read_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, encoding="utf-8-sig", low_memory=False, on_bad_lines="skip")
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="latin-1", low_memory=False, on_bad_lines="skip")


def consolidate() -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    records: list[dict[str, Any]] = []
    inventory: list[dict[str, Any]] = []
    for family, pattern, adapter in SOURCES:
        for path in sorted(REPO_ROOT.glob(pattern)):
            frame = read_csv(path)
            inventory.append({"familia": family, "archivo": str(path.relative_to(REPO_ROOT)), "filas": len(frame)})
            for _, row in frame.iterrows():
                records.append(adapter(row, path))

    if not records:
        return pd.DataFrame(columns=OUTPUT_COLUMNS), inventory

    raw = pd.DataFrame(records)
    consolidated: list[dict[str, Any]] = []
    for _, group in raw.groupby("clave_deduplicacion", sort=False, dropna=False):
        selected = group.iloc[-1].to_dict()
        weeks = sorted({text(v) for v in group["semana"] if text(v)})
        files = list(dict.fromkeys(text(v) for v in group["archivo_origen"] if text(v)))
        selected["semanas_origen"] = "|".join(weeks)
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
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    output, inventory = consolidate()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(args.output, index=False, encoding="utf-8")

    print(f"Archivo: {args.output}")
    print(f"Archivos fuente: {len(inventory)}")
    print(f"Filas fuente: {sum(item['filas'] for item in inventory)}")
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
