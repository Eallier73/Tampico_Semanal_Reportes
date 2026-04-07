#!/usr/bin/env python3
"""
Orquestador general para los pipelines semanales de Tampico.

Objetivo:
- Centralizar la captura de argumentos en un solo prompt.
- Ejecutar uno o varios pipelines en secuencia.
- Evitar depender de los prompts interactivos internos de cada script.
"""

from __future__ import annotations

import argparse
import getpass
import os
import shlex
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

# Importar configuración centralizada de queries
from queries_config import (
    YOUTUBE_CHANNELS,
    YOUTUBE_SEARCH_QUERIES,
    TWITTER_SEARCH_QUERIES,
    MEDIOS_SITES,
    MEDIOS_SEARCH_TERMS,
    FACEBOOK_PAGES,
)


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = REPO_ROOT / "Scripts"

DEFAULT_GLOBAL_BEFORE = date.today().isoformat()
DEFAULT_GLOBAL_SINCE = (date.today() - timedelta(days=7)).isoformat()

# Usar configuración centralizada (desde queries_config.py)
DEFAULT_YOUTUBE_CHANNELS = YOUTUBE_CHANNELS
DEFAULT_YOUTUBE_QUERIES = YOUTUBE_SEARCH_QUERIES
DEFAULT_TWITTER_QUERIES = TWITTER_SEARCH_QUERIES
DEFAULT_FB_PAGES = FACEBOOK_PAGES
DEFAULT_MEDIOS_SITES = MEDIOS_SITES
DEFAULT_TERMS_TAMPICO = MEDIOS_SEARCH_TERMS


@dataclass(frozen=True)
class PipelineSpec:
    code: str
    key: str
    label: str
    filename: str


PIPELINES = [
    PipelineSpec("1", "youtube", "YouTube", "1_extractors_youtube.py"),
    PipelineSpec("2", "twitter", "Twitter/X", "2_extractors_twitter.py"),
    PipelineSpec("3", "medios_tampico", "Medios Tampico", "3_extractors_medios.py"),
    PipelineSpec("4", "facebook_comentarios", "Facebook desde CSV URLs", "4_extractors_facebook_comentarios.py"),
    PipelineSpec("5", "facebook_posts", "Facebook posts", "5_extractors_facebook_posts.py"),
]

PIPELINES_BY_CODE = {item.code: item for item in PIPELINES}
PIPELINES_BY_KEY = {item.key: item for item in PIPELINES}


def prompt_text(label: str, default: str = "", allow_blank: bool = False) -> str:
    suffix = f" [{default}]" if default else ""
    while True:
        value = input(f"{label}{suffix}: ").strip()
        if value:
            return value
        if default:
            return default
        if allow_blank:
            return ""
        print("⚠️ Este valor es obligatorio.")


def prompt_secret(label: str, env_name: str, required: bool = False) -> str:
    current = os.getenv(env_name, "")
    suffix = " [ya definido en entorno]" if current else ""
    while True:
        value = getpass.getpass(f"{label} ({env_name}){suffix}: ").strip()
        if value:
            return value
        if current:
            return current
        if not required:
            return ""
        print(f"⚠️ Debes capturar {env_name} o definirlo en el entorno.")


def prompt_choice(label: str, options: list[str], default: str) -> str:
    rendered = "/".join(options)
    while True:
        value = prompt_text(f"{label} ({rendered})", default=default)
        if value in options:
            return value
        print(f"⚠️ Opción inválida. Usa una de: {', '.join(options)}")


def prompt_bool(label: str, default: bool) -> bool:
    default_text = "s" if default else "n"
    while True:
        raw = prompt_text(f"{label} [s/n]", default=default_text).lower()
        if raw in {"s", "si", "sí", "y", "yes"}:
            return True
        if raw in {"n", "no"}:
            return False
        print("⚠️ Responde s o n.")


def prompt_int(label: str, default: int | None = None, allow_blank: bool = False) -> int | None:
    default_text = "" if default is None else str(default)
    while True:
        raw = prompt_text(label, default=default_text, allow_blank=allow_blank)
        if raw == "" and allow_blank and default is None:
            return None
        try:
            return int(raw)
        except ValueError:
            print("⚠️ Debe ser un entero.")


def prompt_float(label: str, default: float | None = None, allow_blank: bool = False) -> float | None:
    default_text = "" if default is None else str(default)
    while True:
        raw = prompt_text(label, default=default_text, allow_blank=allow_blank)
        if raw == "" and allow_blank and default is None:
            return None
        try:
            return float(raw)
        except ValueError:
            print("⚠️ Debe ser un número.")


def prompt_list(label: str, default: list[str] | None = None, allow_blank: bool = False) -> list[str]:
    default_text = ",".join(default or [])
    raw = prompt_text(label, default=default_text, allow_blank=allow_blank)
    if raw == "":
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def parse_pipeline_selection(raw: str) -> list[PipelineSpec]:
    lowered = raw.strip().lower()
    if lowered in {"all", "todos", "*"}:
        return PIPELINES

    selected: list[PipelineSpec] = []
    seen: set[str] = set()
    for chunk in raw.split(","):
        token = chunk.strip().lower()
        if not token:
            continue
        spec = PIPELINES_BY_CODE.get(token) or PIPELINES_BY_KEY.get(token)
        if spec is None:
            raise ValueError(f"Selección inválida: {token}")
        if spec.code not in seen:
            selected.append(spec)
            seen.add(spec.code)
    if not selected:
        raise ValueError("No se seleccionó ningún pipeline.")
    return selected


def append_many(cmd: list[str], flag: str, values: list[str]) -> None:
    if values:
        cmd.append(flag)
        cmd.extend(values)


def append_optional(cmd: list[str], flag: str, value: str | int | float | None) -> None:
    if value is None or value == "":
        return
    cmd.extend([flag, str(value)])


def prompt_common_context() -> tuple[str, str]:
    print("\n📅 Rango global")
    since = prompt_text("Fecha inicio global (YYYY-MM-DD)", default=DEFAULT_GLOBAL_SINCE)
    before = prompt_text("Fecha fin global (YYYY-MM-DD)", default=DEFAULT_GLOBAL_BEFORE)
    return since, before


def prompt_execution_mode() -> str:
    """
    Pregunta al usuario si desea instrucciones específicas por red o genéricas para todas.
    
    Retorna:
        - 'per_network': Instrucciones personalizadas para cada red
        - 'all_networks': Instrucciones genéricas para todas las redes con fechas comunes
    """
    print("\n" + "="*70)
    print("MODO DE EJECUCIÓN")
    print("="*70)
    print("\n1) POR RED (Específico)")
    print("   - Configura parámetros personalizados para cada extractor")
    print("   - Cada red puede tener queries, opciones y fechas diferentes")
    print("   - Más control, pero más preguntas\n")
    print("2) PARA TODAS LAS REDES (Genérico)")
    print("   - Usa parámetros por defecto para todos los extractores")
    print("   - Mismas fechas para todos")
    print("   - Más rápido, menos preguntas\n")
    
    mode = prompt_choice("Selecciona modo", ["1", "2"], "1")
    return "per_network" if mode == "1" else "all_networks"


def build_youtube(since: str, before: str, use_defaults: bool = False) -> tuple[list[str], dict[str, str]]:
    if use_defaults:
        # MODO GENÉRICO: usar parámetros por defecto
        mode = "ambos"
        channels = DEFAULT_YOUTUBE_CHANNELS
        queries = DEFAULT_YOUTUBE_QUERIES
        max_videos_query = 200
        max_videos_channel = 300
        output_dir = str(REPO_ROOT / "Youtube")
        proxy_http = ""
        proxy_https = ""
        api_key = ""
    else:
        # MODO ESPECÍFICO: preguntar parámetros
        print("\n=== YouTube ===")
        print("  1) Transcripciones")
        print("  2) Comentarios")
        print("  3) Transcripciones y comentarios")
        mode_choice = prompt_choice("Selecciona opción", ["1", "2", "3"], "3")
        mode_map = {"1": "transcripciones", "2": "comentarios", "3": "ambos"}
        mode = mode_map[mode_choice]
        channels = prompt_list("Canales YouTube separados por coma", DEFAULT_YOUTUBE_CHANNELS)
        queries = prompt_list("Queries de búsqueda separadas por coma", DEFAULT_YOUTUBE_QUERIES)
        max_videos_query = prompt_int("Máximo de videos por query", 200)
        max_videos_channel = prompt_int("Máximo de videos por canal", 300)
        output_dir = prompt_text("Directorio base de salida", str(REPO_ROOT / "Youtube"))
        proxy_http = prompt_text("Proxy HTTP opcional", allow_blank=True)
        proxy_https = prompt_text("Proxy HTTPS opcional", allow_blank=True)
        api_key = prompt_secret("YouTube API key", "YOUTUBE_API_KEY", required=True)

    env = {}
    
    # En modo específico, usar token del prompt; en modo genérico, solo si existe en env
    if not use_defaults and api_key:
        if api_key != os.getenv("YOUTUBE_API_KEY", ""):
            env["YOUTUBE_API_KEY"] = api_key

    if not use_defaults:
        if proxy_http:
            env["YT_PROXY_HTTP"] = proxy_http
        if proxy_https:
            env["YT_PROXY_HTTPS"] = proxy_https

    cmd = [
        sys.executable,
        str(SCRIPTS_DIR / "1_extractors_youtube.py"),
        "--since", since,
        "--before", before,
        "--output-dir", output_dir,
        "--mode", mode,
        "--no-prompt",
        "--max-videos-query", str(max_videos_query),
        "--max-videos-channel", str(max_videos_channel),
    ]
    append_many(cmd, "--channels", channels)
    append_many(cmd, "--queries", queries)
    return cmd, env


def build_twitter(since: str, before: str, use_defaults: bool = False) -> tuple[list[str], dict[str, str]]:
    if use_defaults:
        # MODO GENÉRICO: usar parámetros por defecto
        queries = []  # Usa defaults del script
        output_dir = str(REPO_ROOT / "Twitter")
        state_path = str(REPO_ROOT / "state" / "x_state.json")
        max_tweets = 3000
        max_replies = 200
        max_reply_scrolls = 8
        headless = True
    else:
        # MODO ESPECÍFICO: preguntar parámetros
        print("\n=== Twitter/X ===")
        use_script_defaults = prompt_bool("¿Usar queries por defecto del script?", True)
        queries = [] if use_script_defaults else prompt_list("Queries separadas por coma", DEFAULT_TWITTER_QUERIES)
        output_dir = prompt_text("Directorio base de salida", str(REPO_ROOT / "Twitter"))
        state_path = prompt_text("Ruta al storage_state de X/Twitter", str(REPO_ROOT / "state" / "x_state.json"))
        max_tweets = prompt_int("Máximo de tweets por query", 3000)
        max_replies = prompt_int("Máximo de respuestas por tweet", 200)
        max_reply_scrolls = prompt_int("Máximo de scrolls de respuestas", 8)
        headless = prompt_bool("¿Ejecutar navegador en headless?", True)

    cmd = [
        sys.executable,
        str(SCRIPTS_DIR / "2_extractors_twitter.py"),
        "--since", since,
        "--before", before,
        "--output-dir", output_dir,
        "--state-path", state_path,
        "--max-tweets", str(max_tweets),
        "--max-replies-per-tweet", str(max_replies),
        "--max-reply-scrolls", str(max_reply_scrolls),
    ]
    if not headless:
        cmd.append("--no-headless")
    for query in queries:
        cmd.extend(["--query", query])
    return cmd, {}


def build_medios(
    filename: str,
    label: str,
    default_terms: list[str],
    default_output: str,
    default_filename_base: str,
    since: str,
    before: str,
    use_defaults: bool = False,
) -> tuple[list[str], dict[str, str]]:
    if use_defaults:
        # MODO GENÉRICO: usar parámetros por defecto
        medios = DEFAULT_MEDIOS_SITES
        terminos = default_terms
        modo_queries = "combinado"
        output_dir = default_output
        nombre_archivo_base = default_filename_base
        omitir_existentes = True
        pausa = 2.0
        pausa_queries = 3.0
    else:
        # MODO ESPECÍFICO: preguntar parámetros
        print(f"\n=== {label} ===")
        medios = prompt_list("Sites/medios separados por coma", DEFAULT_MEDIOS_SITES)
        terminos = prompt_list("Términos separados por coma", default_terms)
        modo_queries = prompt_choice("Modo de queries", ["compacto", "combinado"], "combinado")
        output_dir = prompt_text("Directorio base de salida", default_output)
        nombre_archivo_base = prompt_text("Prefijo del archivo de salida", default_filename_base)
        omitir_existentes = prompt_bool("¿Omitir semanas ya procesadas?", True)
        pausa = prompt_float("Pausa entre requests", 2.0)
        pausa_queries = prompt_float("Pausa entre queries RSS", 3.0)

    cmd = [
        sys.executable,
        str(SCRIPTS_DIR / filename),
        "--since", since,
        "--before", before,
        "--modo-queries", modo_queries,
        "--output-dir", output_dir,
        "--nombre-archivo-base", nombre_archivo_base,
        "--pausa", str(pausa),
        "--pausa-entre-queries", str(pausa_queries),
    ]
    for medio in medios:
        cmd.extend(["--medio", medio])
    for termino in terminos:
        cmd.extend(["--termino", termino])
    if omitir_existentes:
        cmd.append("--omitir-semanas-existentes")
    else:
        cmd.append("--no-omitir-semanas-existentes")
    return cmd, {}


def build_facebook_comments_from_csv(since: str, before: str, use_defaults: bool = False) -> tuple[list[str], dict[str, str]]:
    if use_defaults:
        # MODO GENÉRICO: usar parámetros por defecto
        mode = "ambos"
        pages = []
        input_csv = ""
        output_dir = str(REPO_ROOT / "Facebook")
        max_comments = 200
        max_urls = None
        sample_percent = None
        sample_seed = 42
        batch_size = 25
        apify_token = ""
    else:
        # MODO ESPECÍFICO: preguntar parámetros
        print("\n=== Facebook desde CSV URLs ===")
        mode = prompt_choice("Modo", ["ambos", "posts", "comentarios"], "ambos")
        pages = prompt_list("Páginas para filtrar (opcional, separadas por coma)", allow_blank=True)
        input_csv = prompt_text("CSV de URLs (opcional)", allow_blank=True)
        output_dir = prompt_text("Directorio base de salida", str(REPO_ROOT / "Facebook"))
        max_comments = prompt_int("Máximo de comentarios por post", 200)
        max_urls = prompt_int("Máximo de URLs a procesar (opcional)", allow_blank=True)
        sample_percent = prompt_float("Sampling % de URLs (opcional)", allow_blank=True)
        sample_seed = prompt_int("Semilla de sampling", 42)
        batch_size = prompt_int("Batch size en Apify", 25)
        apify_token = prompt_secret("Apify token", "APIFY_TOKEN", required=True)

    env = {}
    if mode in {"ambos", "comentarios"}:
        # En modo específico, usar token del prompt; en modo genérico, solo si existe en env
        if not use_defaults and apify_token:
            if apify_token != os.getenv("APIFY_TOKEN", ""):
                env["APIFY_TOKEN"] = apify_token

    cmd = [
        sys.executable,
        str(SCRIPTS_DIR / "4_extractors_facebook_comentarios.py"),
        "--since", since,
        "--before", before,
        "--mode", mode,
        "--output-dir", output_dir,
        "--max-comments", str(max_comments),
        "--sample-seed", str(sample_seed),
        "--batch-size", str(batch_size),
        "--no-prompt",
    ]
    append_many(cmd, "--pages", pages)
    append_optional(cmd, "--input-csv", input_csv)
    append_optional(cmd, "--max-urls", max_urls)
    append_optional(cmd, "--sample-percent", sample_percent)
    return cmd, env


def build_facebook_posts(since: str, before: str, use_defaults: bool = False) -> tuple[list[str], dict[str, str]]:
    if use_defaults:
        # MODO GENÉRICO: usar parámetros por defecto
        pages = DEFAULT_FB_PAGES
        output_dir = str(REPO_ROOT / "Facebook")
        max_posts = 100
        max_pages = None
        sample_percent = None
        sample_seed = 42
        batch_size = 10
        apify_token = ""
    else:
        # MODO ESPECÍFICO: preguntar parámetros
        print("\n=== Facebook posts ===")
        pages = prompt_list("Páginas target separadas por coma", DEFAULT_FB_PAGES)
        output_dir = prompt_text("Directorio base de salida", str(REPO_ROOT / "Facebook"))
        max_posts = prompt_int("Máximo de posts por página", 100)
        max_pages = prompt_int("Máximo de páginas target (opcional)", allow_blank=True)
        sample_percent = prompt_float("Sampling % de páginas (opcional)", allow_blank=True)
        sample_seed = prompt_int("Semilla de sampling", 42)
        batch_size = prompt_int("Batch size", 10)
        apify_token = prompt_secret("Apify token", "APIFY_TOKEN", required=True)

    env = {}
    if not use_defaults and apify_token:
        if apify_token != os.getenv("APIFY_TOKEN", ""):
            env["APIFY_TOKEN"] = apify_token

    cmd = [
        sys.executable,
        str(SCRIPTS_DIR / "5_extractors_facebook_posts.py"),
        "--since", since,
        "--before", before,
        "--output-dir", output_dir,
        "--max-posts", str(max_posts),
        "--sample-seed", str(sample_seed),
        "--batch-size", str(batch_size),
        "--no-prompt",
    ]
    append_many(cmd, "--pages", pages)
    append_optional(cmd, "--max-pages", max_pages)
    append_optional(cmd, "--sample-percent", sample_percent)
    return cmd, env


def build_pipeline(spec: PipelineSpec, since: str, before: str, use_defaults: bool = False) -> tuple[list[str], dict[str, str]]:
    if spec.key == "youtube":
        return build_youtube(since, before, use_defaults)
    if spec.key == "twitter":
        return build_twitter(since, before, use_defaults)
    if spec.key == "medios_tampico":
        return build_medios(
            spec.filename,
            spec.label,
            DEFAULT_TERMS_TAMPICO,
            str(REPO_ROOT / "Medios"),
            "noticias_tampico",
            since,
            before,
            use_defaults,
        )
    if spec.key == "facebook_comentarios":
        return build_facebook_comments_from_csv(since, before, use_defaults)
    if spec.key == "facebook_posts":
        return build_facebook_posts(since, before, use_defaults)
    raise ValueError(f"Pipeline no soportado: {spec.key}")


def render_command(cmd: list[str]) -> str:
    return " ".join(shlex.quote(part) for part in cmd)


def main() -> None:
    parser = argparse.ArgumentParser(description="Orquestador general de pipelines Tampico")
    parser.add_argument("--dry-run", action="store_true", help="Solo imprime comandos, no los ejecuta")
    args = parser.parse_args()

    if not sys.stdin.isatty():
        print("❌ El orquestador requiere una terminal interactiva.")
        sys.exit(1)

    # 1️⃣ PASO 1: Preguntar modo de ejecución
    execution_mode = prompt_execution_mode()

    # 2️⃣ PASO 2: Seleccionar pipelines
    print("\n" + "="*70)
    print("PIPELINES DISPONIBLES")
    print("="*70)
    for item in PIPELINES:
        print(f"  {item.code}) {item.label}")

    while True:
        raw_selection = prompt_text("\nSelecciona pipelines (ej. 1,2,5 o all)", default="all")
        try:
            selected = parse_pipeline_selection(raw_selection)
            break
        except ValueError as exc:
            print(f"⚠️ {exc}")

    # 3️⃣ PASO 3: Capturar fechas
    since, before = prompt_common_context()
    
    # 4️⃣ PASO 4: Configurar según modo
    if execution_mode == "all_networks":
        # MODO GENÉRICO: Usar parámetros por defecto para todos
        print("\n" + "="*70)
        print("MODO: GENÉRICO (Todas las redes con parámetros por defecto)")
        print("="*70)
        continue_on_error = prompt_bool("\n¿Continuar si un pipeline falla?", False)
        
        prepared: list[tuple[PipelineSpec, list[str], dict[str, str]]] = []
        for spec in selected:
            cmd, env = build_pipeline(spec, since, before, use_defaults=True)
            prepared.append((spec, cmd, env))
    else:
        # MODO ESPECÍFICO: Preguntar parámetros para cada red
        print("\n" + "="*70)
        print("MODO: ESPECÍFICO POR RED")
        print("="*70)
        continue_on_error = prompt_bool("\n¿Continuar si un pipeline falla?", False)
        
        prepared: list[tuple[PipelineSpec, list[str], dict[str, str]]] = []
        for spec in selected:
            cmd, env = build_pipeline(spec, since, before, use_defaults=False)
            prepared.append((spec, cmd, env))

    # 5️⃣ PASO 5: Mostrar resumen
    print("\n" + "="*70)
    print("RESUMEN DE EJECUCIÓN")
    print("="*70)
    print(f"📋 Modo: {execution_mode.replace('_', ' ').title()}")
    print(f"📅 Fechas: {since} → {before}")
    print(f"📊 Pipelines: {len(selected)}")
    
    for spec, cmd, env in prepared:
        print(f"\n[{spec.code}] {spec.label}")
        print(f"   CMD: {render_command(cmd)[:80]}...")
        if env:
            print(f"   ENV: {', '.join(sorted(env.keys()))}")

    if not prompt_bool("\n¿Ejecutar estos pipelines?", True):
        print("Cancelado.")
        return

    if args.dry_run:
        print("Dry run finalizado.")
        return

    # 6️⃣ PASO 6: Ejecutar pipelines
    print("\n" + "="*70)
    print("INICIANDO EJECUCIÓN")
    print("="*70)
    
    for spec, cmd, env_overrides in prepared:
        print(f"\n▶ Ejecutando {spec.label}")
        env = os.environ.copy()
        env.update(env_overrides)
        result = subprocess.run(cmd, env=env, cwd=str(REPO_ROOT))
        if result.returncode == 0:
            print(f"✅ {spec.label} completado")
            continue

        print(f"❌ {spec.label} falló con código {result.returncode}")
        if not continue_on_error:
            sys.exit(result.returncode)

    print("\n" + "="*70)
    print("✅ EJECUCIÓN TERMINADA")
    print("="*70)


if __name__ == "__main__":
    main()
