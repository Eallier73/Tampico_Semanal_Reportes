#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import random
import re
from datetime import datetime
from pathlib import Path

import anthropic
import pandas as pd

try:
    from dotenv import load_dotenv

    env_file = Path(__file__).resolve().parent.parent / ".env.local"
    if env_file.exists():
        load_dotenv(str(env_file))
except ImportError:
    pass

from output_naming import (
    build_range_output_dir,
    build_range_report_tag,
    ensure_tagged_name,
    validate_date_range,
    write_range_contract,
)


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_TWITTER_DIR = REPO_ROOT / "Twitter"
DEFAULT_FACEBOOK_DIR = REPO_ROOT / "Facebook"
DEFAULT_YOUTUBE_DIR = REPO_ROOT / "Youtube"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "Claude"
DEFAULT_MODEL = "claude-opus-4-6"
DEFAULT_MAX_CORPUS_CHARS = 120000
DEFAULT_MAX_DOC_CHARS = 6000
DEFAULT_SAMPLE_SEED = 42
API_ENV_NAME = "CLAUDE_API_KEY"
THEME_COUNT = 8
NETWORK_LABELS = {
    "twitter": "Twitter/X",
    "facebook": "Facebook",
    "youtube": "YouTube",
}
MONTH_NAMES = {
    1: "ENERO",
    2: "FEBRERO",
    3: "MARZO",
    4: "ABRIL",
    5: "MAYO",
    6: "JUNIO",
    7: "JULIO",
    8: "AGOSTO",
    9: "SEPTIEMBRE",
    10: "OCTUBRE",
    11: "NOVIEMBRE",
    12: "DICIEMBRE",
}
COMMON_COLUMNS = [
    "fecha_inicio_rango",
    "nombre_rango",
    "red_social",
    "red_social_label",
    "tipo_publicacion",
    "cuenta_origen",
    "autor",
    "titulo",
    "texto_publicacion",
    "texto_para_analisis",
    "url_publicacion",
    "id_publicacion",
    "fecha_publicacion",
    "fecha_publicacion_date",
    "archivo_origen",
]


def log_message(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def valid_date(value: str) -> str:
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Fecha invalida '{value}', usa YYYY-MM-DD") from exc
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Consolida publicaciones institucionales de Twitter, Facebook y YouTube "
            "para generar un analisis tematico con Claude en Tampico."
        )
    )
    parser.add_argument("--since", required=True, type=valid_date,
                        help="Límite inicial inclusivo YYYY-MM-DD")
    parser.add_argument("--before", required=True, type=valid_date,
                        help="Fecha fin YYYY-MM-DD")
    parser.add_argument("--twitter-dir", default=str(DEFAULT_TWITTER_DIR),
                        help=f"Carpeta base de Twitter (default: {DEFAULT_TWITTER_DIR})")
    parser.add_argument("--facebook-dir", default=str(DEFAULT_FACEBOOK_DIR),
                        help=f"Carpeta base de Facebook (default: {DEFAULT_FACEBOOK_DIR})")
    parser.add_argument("--youtube-dir", default=str(DEFAULT_YOUTUBE_DIR),
                        help=f"Carpeta base de YouTube (default: {DEFAULT_YOUTUBE_DIR})")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR),
                        help=f"Carpeta base de salida Claude (default: {DEFAULT_OUTPUT_DIR})")
    parser.add_argument("--model", default=DEFAULT_MODEL,
                        help=f"Modelo Claude a usar (default: {DEFAULT_MODEL})")
    parser.add_argument("--max-corpus-chars", type=int, default=DEFAULT_MAX_CORPUS_CHARS,
                        help=f"Maximo de caracteres a enviar (default: {DEFAULT_MAX_CORPUS_CHARS})")
    parser.add_argument("--max-doc-chars", type=int, default=DEFAULT_MAX_DOC_CHARS,
                        help=f"Maximo de caracteres por publicacion en el corpus (default: {DEFAULT_MAX_DOC_CHARS})")
    parser.add_argument("--sample-seed", type=int, default=DEFAULT_SAMPLE_SEED,
                        help=f"Semilla para muestreo cuando el corpus excede el limite (default: {DEFAULT_SAMPLE_SEED})")
    parser.add_argument("--prepare-only", action="store_true",
                        help="Solo prepara CSV consolidado, prompt y corpus; no llama a Claude")
    return parser.parse_args()


def range_dir(base_dir: Path, since: str, before: str, source: str) -> Path:
    return Path(base_dir) / build_range_report_tag(since, before, source)


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(content)


def save_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


def read_csv_with_fallback(path: Path) -> pd.DataFrame:
    last_exc: Exception | None = None
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return pd.read_csv(path, encoding=encoding)
        except Exception as exc:
            last_exc = exc
    raise RuntimeError(f"No se pudo leer {path}: {last_exc}")


def normalize_whitespace(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def parse_datetime_fields(value: object) -> tuple[str, str]:
    text = normalize_whitespace(value)
    if not text:
        return "", ""

    variants = [text]
    if text.endswith("Z"):
        variants.append(text[:-1] + "+00:00")

    for variant in variants:
        try:
            dt = datetime.fromisoformat(variant)
            return dt.isoformat(sep=" "), dt.date().isoformat()
        except ValueError:
            continue

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.isoformat(sep=" "), dt.date().isoformat()
        except ValueError:
            continue

    return text, text[:10] if len(text) >= 10 else ""


def build_text_for_analysis(title: str, body: str) -> str:
    if title and body:
        return f"{title}. {body}"
    return title or body


def load_twitter_records(
    base_dir: Path,
    since: str,
    before: str,
) -> tuple[list[dict[str, str]], dict[str, object]]:
    source_range_dir = range_dir(base_dir, since, before, "Twitter")
    csv_path = source_range_dir / f"{source_range_dir.name}_post_institucionales.csv"
    meta: dict[str, object] = {
        "source": "twitter",
        "path": str(csv_path),
        "exists": csv_path.exists(),
        "records": 0,
        "skipped_empty_text": 0,
    }
    if not csv_path.exists():
        return [], meta

    df = read_csv_with_fallback(csv_path)
    records: list[dict[str, str]] = []

    for row in df.fillna("").to_dict("records"):
        text = normalize_whitespace(row.get("text"))
        if not text:
            meta["skipped_empty_text"] = int(meta["skipped_empty_text"]) + 1
            continue

        url = normalize_whitespace(row.get("url"))
        author = normalize_whitespace(row.get("author"))
        fecha_publicacion, fecha_publicacion_date = parse_datetime_fields(
            row.get("datetime_parsed_utc") or row.get("datetime")
        )
        records.append(
            {
                "fecha_inicio_rango": since,
                "nombre_rango": normalize_whitespace(row.get("nombre_rango")) or source_range_dir.name,
                "red_social": "twitter",
                "red_social_label": NETWORK_LABELS["twitter"],
                "tipo_publicacion": "tweet",
                "cuenta_origen": author,
                "autor": author,
                "titulo": "",
                "texto_publicacion": text,
                "texto_para_analisis": text,
                "url_publicacion": url,
                "id_publicacion": url or text[:120],
                "fecha_publicacion": fecha_publicacion,
                "fecha_publicacion_date": fecha_publicacion_date,
                "archivo_origen": str(csv_path),
            }
        )

    meta["records"] = len(records)
    return records, meta


def load_facebook_records(
    base_dir: Path,
    since: str,
    before: str,
) -> tuple[list[dict[str, str]], dict[str, object]]:
    source_range_dir = range_dir(base_dir, since, before, "Facebook")
    csv_path = source_range_dir / f"{source_range_dir.name}_posts.csv"
    meta: dict[str, object] = {
        "source": "facebook",
        "path": str(csv_path),
        "exists": csv_path.exists(),
        "records": 0,
        "skipped_empty_text": 0,
    }
    if not csv_path.exists():
        return [], meta

    df = read_csv_with_fallback(csv_path)
    records: list[dict[str, str]] = []

    for row in df.fillna("").to_dict("records"):
        text = normalize_whitespace(row.get("post_texto"))
        if not text:
            meta["skipped_empty_text"] = int(meta["skipped_empty_text"]) + 1
            continue

        page_handle = normalize_whitespace(row.get("page_handle"))
        page_url = normalize_whitespace(row.get("page_url"))
        author = normalize_whitespace(row.get("autor"))
        fecha_publicacion, fecha_publicacion_date = parse_datetime_fields(
            row.get("fecha_post") or row.get("fecha_post_date")
        )
        url = normalize_whitespace(row.get("post_url"))
        records.append(
            {
                "fecha_inicio_rango": since,
                "nombre_rango": source_range_dir.name,
                "red_social": "facebook",
                "red_social_label": NETWORK_LABELS["facebook"],
                "tipo_publicacion": "post_facebook",
                "cuenta_origen": page_handle or author,
                "autor": author,
                "titulo": "",
                "texto_publicacion": text,
                "texto_para_analisis": text,
                "url_publicacion": url or page_url,
                "id_publicacion": url or text[:120],
                "fecha_publicacion": fecha_publicacion,
                "fecha_publicacion_date": fecha_publicacion_date,
                "archivo_origen": str(csv_path),
            }
        )

    meta["records"] = len(records)
    return records, meta


def load_youtube_records(
    base_dir: Path,
    since: str,
    before: str,
) -> tuple[list[dict[str, str]], dict[str, object]]:
    source_range_dir = range_dir(base_dir, since, before, "Youtube")
    csv_path = source_range_dir / f"{source_range_dir.name}_scripts.csv"
    meta: dict[str, object] = {
        "source": "youtube",
        "path": str(csv_path),
        "exists": csv_path.exists(),
        "records": 0,
        "skipped_empty_text": 0,
        "skipped_non_ok_transcript": 0,
    }
    if not csv_path.exists():
        return [], meta

    df = read_csv_with_fallback(csv_path)
    records: list[dict[str, str]] = []

    for row in df.fillna("").to_dict("records"):
        transcript_status = normalize_whitespace(row.get("transcript_status")).lower()
        transcript_text = normalize_whitespace(row.get("transcript_text"))
        if transcript_status and transcript_status != "ok":
            meta["skipped_non_ok_transcript"] = int(meta["skipped_non_ok_transcript"]) + 1
            continue
        if not transcript_text:
            meta["skipped_empty_text"] = int(meta["skipped_empty_text"]) + 1
            continue

        title = normalize_whitespace(row.get("video_title"))
        channel_handle = normalize_whitespace(row.get("channel_handle"))
        channel_title = normalize_whitespace(row.get("channel_title"))
        fecha_publicacion, fecha_publicacion_date = parse_datetime_fields(row.get("video_published_at"))
        video_id = normalize_whitespace(row.get("video_id"))
        url = f"https://www.youtube.com/watch?v={video_id}" if video_id else ""
        records.append(
            {
                "fecha_inicio_rango": since,
                "nombre_rango": source_range_dir.name,
                "red_social": "youtube",
                "red_social_label": NETWORK_LABELS["youtube"],
                "tipo_publicacion": "video_youtube",
                "cuenta_origen": channel_handle or channel_title,
                "autor": channel_title,
                "titulo": title,
                "texto_publicacion": transcript_text,
                "texto_para_analisis": build_text_for_analysis(title, transcript_text),
                "url_publicacion": url,
                "id_publicacion": video_id or title[:120],
                "fecha_publicacion": fecha_publicacion,
                "fecha_publicacion_date": fecha_publicacion_date,
                "archivo_origen": str(csv_path),
            }
        )

    meta["records"] = len(records)
    return records, meta


def consolidate_records(args: argparse.Namespace) -> tuple[pd.DataFrame, dict[str, object]]:
    loaders = [
        load_twitter_records(Path(args.twitter_dir), args.since, args.before),
        load_facebook_records(Path(args.facebook_dir), args.since, args.before),
        load_youtube_records(Path(args.youtube_dir), args.since, args.before),
    ]

    source_meta: dict[str, object] = {}
    frames: list[pd.DataFrame] = []
    for records, meta in loaders:
        source_meta[str(meta["source"])] = meta
        if records:
            frames.append(pd.DataFrame(records, columns=COMMON_COLUMNS))

    if not frames:
        raise FileNotFoundError(
            "No se encontraron publicaciones institucionales. "
            "Se esperaban CSVs de Twitter, Facebook o YouTube para el rango solicitado."
        )

    df = pd.concat(frames, ignore_index=True)
    dedupe_key = (
        df["red_social"].fillna("")
        + "||"
        + df["id_publicacion"].fillna("")
        + "||"
        + df["texto_para_analisis"].fillna("").str[:200]
    )
    df = df.assign(_dedupe_key=dedupe_key).drop_duplicates(subset=["_dedupe_key"]).drop(columns=["_dedupe_key"])
    df = df.sort_values(
        by=["fecha_publicacion_date", "red_social", "cuenta_origen"],
        ascending=[False, True, True],
        na_position="last",
    ).reset_index(drop=True)
    return df, source_meta


def build_stats(df: pd.DataFrame) -> dict[str, object]:
    total_publicaciones = int(len(df))
    por_red = {
        network: int(count)
        for network, count in df.groupby("red_social").size().sort_index().items()
    }
    por_cuenta = {
        account: int(count)
        for account, count in df.groupby("cuenta_origen").size().sort_values(ascending=False).head(10).items()
    }
    return {
        "total_publicaciones": total_publicaciones,
        "por_red": por_red,
        "top_cuentas": por_cuenta,
    }


def build_summary_block(stats: dict[str, object]) -> str:
    lines = ["Resumen cuantitativo del corpus:"]
    lines.append(f"- Total de publicaciones institucionales: {stats['total_publicaciones']}")

    por_red = stats.get("por_red", {})
    for network in ("twitter", "facebook", "youtube"):
        lines.append(f"- {NETWORK_LABELS[network]}: {int(por_red.get(network, 0))}")

    top_cuentas = stats.get("top_cuentas", {})
    if top_cuentas:
        lines.append("- Cuentas con mayor volumen:")
        for account, count in top_cuentas.items():
            lines.append(f"  - {account}: {int(count)}")

    return "\n".join(lines)


def format_corpus_record(row: dict[str, str], max_doc_chars: int) -> str:
    text = normalize_whitespace(row.get("texto_para_analisis"))
    if max_doc_chars > 0 and len(text) > max_doc_chars:
        text = text[:max_doc_chars].rstrip() + " [TRUNCADO]"

    lines = [
        f"RED: {row.get('red_social_label', '')}",
        f"CUENTA: {row.get('cuenta_origen', '')}",
        f"FECHA: {row.get('fecha_publicacion_date') or row.get('fecha_publicacion') or ''}",
    ]
    if row.get("titulo"):
        lines.append(f"TITULO: {row.get('titulo')}")
    if row.get("url_publicacion"):
        lines.append(f"URL: {row.get('url_publicacion')}")
    lines.append(f"TEXTO: {text}")
    return "\n".join(lines)


def sample_corpus(
    df: pd.DataFrame,
    max_chars: int,
    seed: int,
    max_doc_chars: int,
) -> tuple[str, dict[str, object]]:
    rows = df.to_dict("records")
    formatted = [(row, format_corpus_record(row, max_doc_chars)) for row in rows]
    separators = "\n\n---\n\n"
    original_chars = sum(len(text) for _, text in formatted) + max(0, len(formatted) - 1) * len(separators)

    if original_chars <= max_chars:
        corpus = separators.join(text for _, text in formatted)
        return corpus, {
            "sampled": False,
            "original_chars": original_chars,
            "final_chars": len(corpus),
            "original_docs": len(formatted),
            "final_docs": len(formatted),
        }

    rng = random.Random(seed)
    grouped: dict[str, list[tuple[dict[str, str], str]]] = {}
    for row, text in formatted:
        grouped.setdefault(str(row.get("red_social") or "sin_red"), []).append((row, text))

    selected: list[tuple[dict[str, str], str]] = []
    leftovers: list[tuple[dict[str, str], str]] = []
    approx_network_quota = max_chars // max(len(grouped), 1)

    for network in sorted(grouped):
        items = grouped[network][:]
        rng.shuffle(items)
        network_chars = 0
        for row, text in items:
            text_size = len(text) + len(separators)
            if network_chars == 0 or network_chars + text_size <= approx_network_quota:
                selected.append((row, text))
                network_chars += text_size
            else:
                leftovers.append((row, text))

    rng.shuffle(leftovers)
    current_chars = sum(len(text) for _, text in selected) + max(0, len(selected) - 1) * len(separators)
    for row, text in leftovers:
        extra = len(text) + (len(separators) if selected else 0)
        if current_chars + extra > max_chars and selected:
            continue
        selected.append((row, text))
        current_chars += extra

    if not selected and formatted:
        selected.append(formatted[0])

    corpus = separators.join(text for _, text in selected)
    return corpus, {
        "sampled": True,
        "original_chars": original_chars,
        "final_chars": len(corpus),
        "original_docs": len(formatted),
        "final_docs": len(selected),
        "selected_by_network": {
            network: sum(1 for row, _ in selected if str(row.get("red_social")) == network)
            for network in sorted(grouped)
        },
    }


def build_prompt(since: str, before: str, stats: dict[str, object]) -> str:
    dt = datetime.strptime(since, "%Y-%m-%d")
    month_label = MONTH_NAMES[dt.month]
    year_label = dt.year
    summary_block = build_summary_block(stats)

    return f"""
Analiza publicaciones institucionales oficiales de Tampico correspondientes al rango exacto [{since}, {before}) de Twitter/X, Facebook y YouTube.

El corpus puede incluir publicaciones del Gobierno Municipal de Tampico, de la presidencia municipal, de dependencias o areas oficiales, y de canales institucionales vinculados a la agenda del municipio.

Tu tarea es identificar los temas de comunicacion institucional que dominan estas publicaciones y estimar su peso relativo dentro del corpus.

{summary_block}

OBJETIVO ANALITICO:
- Construir un catalogo de EXACTAMENTE {THEME_COUNT} temas que describan la agenda institucional observada en las publicaciones.
- Explicar brevemente cada tema con enfoque en acciones, anuncios, programas, servicios, eventos, obras, seguridad, proteccion civil, cultura, turismo o gestion municipal.
- Indicar en que redes predomina cada tema.
- Asignar un porcentaje a cada tema con base en la proporcion relativa de publicaciones institucionales asociadas a ese tema.

REGLAS:
- Los porcentajes deben sumar 100.
- Si un tema aparece en mas de una red, puedes mencionar varias redes dominantes.
- Usa frecuencia relativa del corpus; no uses engagement como criterio.
- No inventes temas que no esten respaldados por el corpus.
- Escribe todo en espanol.

SALIDA OBLIGATORIA:
Devuelve SOLO un JSON valido, sin markdown, sin comentarios, sin texto adicional, con esta estructura exacta:
{{
  "titulo": "ANALISIS TEMATICO DE PUBLICACIONES INSTITUCIONALES DE TAMPICO - {month_label} DE {year_label}",
  "resumen_general": "Resumen ejecutivo breve de la agenda institucional observada.",
  "temas": [
    {{
      "tema": "NOMBRE DEL TEMA EN MAYUSCULAS",
      "descripcion": "Descripcion breve y concreta del tema.",
      "evidencia_institucional": "Como aparece el tema en las publicaciones institucionales.",
      "redes_dominantes": ["Twitter/X", "Facebook"],
      "porcentaje": 0.0
    }}
  ]
}}

RESTRICCIONES DE FORMATO:
- EXACTAMENTE {THEME_COUNT} elementos en "temas".
- "tema" siempre en MAYUSCULAS.
- "descripcion" y "evidencia_institucional" deben ser breves y concretos.
- "redes_dominantes" debe ser una lista con una o mas redes entre: "Twitter/X", "Facebook", "YouTube".
- "porcentaje" debe expresarse como numero, no como texto, con uno o dos decimales si hace falta.
- No devuelvas bloques de codigo ni fences.
""".strip()


def generate_analysis(api_key: str, model: str, prompt: str, corpus_text: str) -> tuple[str, dict[str, int | str]]:
    client = anthropic.Anthropic(api_key=api_key)
    message_content = f"{prompt}\n\n=== CORPUS PARA ANALISIS ===\n\n{corpus_text}"

    log_message("🚀 Enviando corpus de publicaciones institucionales a Claude API...")
    response = client.messages.create(
        model=model,
        max_tokens=5000,
        messages=[{"role": "user", "content": message_content}],
    )

    analysis_text = "\n".join(
        block.text for block in response.content if getattr(block, "type", "") == "text"
    ).strip()
    usage = {
        "model": model,
        "input_tokens": int(getattr(response.usage, "input_tokens", 0)),
        "output_tokens": int(getattr(response.usage, "output_tokens", 0)),
    }
    return analysis_text, usage


def extract_json_payload(raw_text: str) -> dict[str, object]:
    cleaned = raw_text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
    if not match:
        raise ValueError("Claude no devolvio un objeto JSON interpretable.")

    return json.loads(match.group(0))


def to_float(value: object) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    text = normalize_whitespace(value).replace("%", "")
    if not text:
        return 0.0
    return float(text)


def normalize_percentages(themes: list[dict[str, object]]) -> None:
    values = [to_float(theme.get("porcentaje")) for theme in themes]
    total = sum(values)
    if total <= 0:
        even = round(100.0 / len(themes), 1) if themes else 0.0
        normalized = [even for _ in themes]
        if normalized:
            normalized[-1] = round(100.0 - sum(normalized[:-1]), 1)
    else:
        normalized = [round((value / total) * 100.0, 1) for value in values]
        delta = round(100.0 - sum(normalized), 1)
        normalized[-1] = round(normalized[-1] + delta, 1)

    for theme, value in zip(themes, normalized):
        theme["porcentaje"] = value


def normalize_networks(value: object) -> list[str]:
    if isinstance(value, list):
        candidates = [normalize_whitespace(item) for item in value]
    else:
        text = normalize_whitespace(value)
        candidates = [part.strip() for part in re.split(r"[,;/|]", text)] if text else []

    normalized: list[str] = []
    mapping = {
        "twitter": "Twitter/X",
        "twitterx": "Twitter/X",
        "x": "Twitter/X",
        "twitter/x": "Twitter/X",
        "facebook": "Facebook",
        "youtube": "YouTube",
        "you tube": "YouTube",
    }
    for candidate in candidates:
        key = re.sub(r"\s+", "", candidate.lower())
        label = mapping.get(key)
        if label and label not in normalized:
            normalized.append(label)
    return normalized


def normalize_analysis_payload(payload: dict[str, object]) -> dict[str, object]:
    temas = payload.get("temas")
    if not isinstance(temas, list) or len(temas) != THEME_COUNT:
        raise ValueError(f"La respuesta de Claude debe incluir exactamente {THEME_COUNT} temas.")

    normalized_themes: list[dict[str, object]] = []
    for idx, item in enumerate(temas, 1):
        if not isinstance(item, dict):
            raise ValueError(f"El tema #{idx} no tiene formato de objeto JSON.")

        networks = normalize_networks(item.get("redes_dominantes"))
        if not networks:
            networks = ["Facebook"]

        normalized_themes.append(
            {
                "orden": idx,
                "tema": normalize_whitespace(item.get("tema")).upper(),
                "descripcion": normalize_whitespace(item.get("descripcion")),
                "evidencia_institucional": normalize_whitespace(item.get("evidencia_institucional")),
                "redes_dominantes": networks,
                "porcentaje": to_float(item.get("porcentaje")),
            }
        )

    normalize_percentages(normalized_themes)
    titulo = normalize_whitespace(payload.get("titulo")) or "ANALISIS TEMATICO DE PUBLICACIONES INSTITUCIONALES DE TAMPICO"
    resumen_general = normalize_whitespace(payload.get("resumen_general"))
    return {
        "titulo": titulo,
        "resumen_general": resumen_general,
        "temas": normalized_themes,
    }


def build_markdown_report(payload: dict[str, object], stats: dict[str, object]) -> str:
    lines = [f"# {payload['titulo']}", ""]
    if payload.get("resumen_general"):
        lines.extend([str(payload["resumen_general"]), ""])

    lines.extend(["## Resumen cuantitativo", "", build_summary_block(stats), ""])
    lines.extend(["## Temas", ""])
    for theme in payload.get("temas", []):
        lines.append(f"### {theme['orden']}. {theme['tema']}")
        lines.append(f"- Porcentaje: {theme['porcentaje']}%")
        lines.append(f"- Redes dominantes: {', '.join(theme['redes_dominantes'])}")
        lines.append(f"- Descripcion: {theme['descripcion']}")
        lines.append(f"- Evidencia institucional: {theme['evidencia_institucional']}")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def main() -> None:
    args = parse_args()
    try:
        validate_date_range(args.since, args.before)
    except ValueError as exc:
        raise SystemExit(f"❌ {exc}") from exc

    output_dir = Path(args.output_dir)
    claude_dir = build_range_output_dir(
        output_dir,
        args.since,
        args.before,
        "Claude_Publicaciones",
    )
    claude_tag = build_range_report_tag(
        args.since,
        args.before,
        "Claude_Publicaciones",
    )
    write_range_contract(
        claude_dir,
        args.since,
        args.before,
        "Claude_Publicaciones",
    )

    log_message("🤖 ANALISIS DE PUBLICACIONES INSTITUCIONALES CON CLAUDE")
    log_message(f"Salida Claude: {claude_dir}")

    df, source_meta = consolidate_records(args)
    stats = build_stats(df)

    csv_path = claude_dir / f"{ensure_tagged_name('publicaciones_institucionales_consolidadas', claude_tag)}.csv"
    prompt_path = claude_dir / f"{ensure_tagged_name('prompt_publicaciones_institucionales_claude', claude_tag)}.txt"
    corpus_path = claude_dir / f"{ensure_tagged_name('corpus_publicaciones_institucionales_claude', claude_tag)}.txt"
    meta_path = claude_dir / f"{ensure_tagged_name('metadata_publicaciones_institucionales_claude', claude_tag)}.json"

    claude_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False, encoding="utf-8")

    prompt = build_prompt(args.since, args.before, stats)
    write_text(prompt_path, prompt + "\n")

    sampled_corpus, sampling_stats = sample_corpus(df, args.max_corpus_chars, args.sample_seed, args.max_doc_chars)
    write_text(corpus_path, sampled_corpus + ("\n" if sampled_corpus else ""))

    log_message(f"📄 CSV consolidado generado: {csv_path.name}")
    log_message(f"🧠 Prompt generado: {prompt_path.name}")
    log_message(f"📚 Corpus preparado: {corpus_path.name}")

    if sampling_stats["sampled"]:
        log_message(
            f"⚠️ Corpus excede el limite; se aplico muestreo a {sampling_stats['final_docs']} documentos "
            f"de {sampling_stats['original_docs']}"
        )
    else:
        log_message(f"✅ Corpus completo listo para envio: {sampling_stats['final_chars']:,} caracteres")

    metadata: dict[str, object] = {
        "since": args.since,
        "before": args.before,
        "output_dir": str(claude_dir),
        "sources": source_meta,
        "stats": stats,
        "sampling": sampling_stats,
        "artifacts": {
            "consolidated_csv": str(csv_path),
            "prompt": str(prompt_path),
            "corpus": str(corpus_path),
        },
    }

    if args.prepare_only:
        metadata["prepare_only"] = True
        save_json(meta_path, metadata)
        log_message("🧪 Modo prepare-only: no se llamo a Claude")
        log_message(f"✅ Metadata guardada en: {meta_path}")
        return

    api_key = os.getenv(API_ENV_NAME, "").strip()
    if not api_key:
        raise SystemExit(
            f"No se encontro {API_ENV_NAME}. Define la variable en .env.local o en el entorno antes de ejecutar este script."
        )

    analysis_text, usage = generate_analysis(api_key, args.model, prompt, sampled_corpus)
    if not analysis_text:
        raise SystemExit("Claude no devolvio texto de analisis")

    payload = normalize_analysis_payload(extract_json_payload(analysis_text))
    json_path = claude_dir / f"{ensure_tagged_name('analisis_publicaciones_institucionales_claude', claude_tag)}.json"
    md_path = claude_dir / f"{ensure_tagged_name('analisis_publicaciones_institucionales_claude', claude_tag)}.md"
    themes_csv_path = claude_dir / f"{ensure_tagged_name('temas_publicaciones_institucionales_claude', claude_tag)}.csv"

    save_json(json_path, payload)
    write_text(md_path, build_markdown_report(payload, stats))
    pd.DataFrame(payload["temas"]).to_csv(themes_csv_path, index=False, encoding="utf-8")

    metadata["usage"] = usage
    metadata["artifacts"] = {
        **metadata["artifacts"],
        "analysis_json": str(json_path),
        "analysis_markdown": str(md_path),
        "themes_csv": str(themes_csv_path),
    }
    save_json(meta_path, metadata)

    log_message(f"✅ Analisis JSON guardado en: {json_path}")
    log_message(f"✅ Reporte Markdown guardado en: {md_path}")
    log_message(f"✅ Temas CSV guardado en: {themes_csv_path}")
    log_message(f"✅ Metadata guardada en: {meta_path}")


if __name__ == "__main__":
    main()
