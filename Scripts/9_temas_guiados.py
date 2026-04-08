#!/usr/bin/env python3
"""
9_temas_guiados.py
==================
Analisis de temas guiados por palabras clave para el reporte semanal de Tampico.

Entradas por defecto:
- Datos/{semana}/material_institucional.txt
- Datos/{semana}/material_comentarios.txt

Salidas por defecto:
- Temas_Guiados/{semana}/
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from output_naming import build_report_tag, ensure_tagged_name

DEFAULT_EXCLUDE_WORDS_PATH = (
    Path(__file__).resolve().parent
    / "diccionarios"
    / "stopwords"
    / "stop_list_espanol.txt"
)

TOPIC_KEYWORDS = {
    0: [
        "agua", "comapa", "fuga", "fugas", "inundacion", "inundaciones", "suministro",
        "potable", "corte", "bombeo", "bomba", "cortan", "tuberia", "tubo", "tubos",
        "hidraulica", "hidraulico", "sed", "grifo", "llave", "sequia",
        "basura", "recoleccion", "contenedor", "contenedores", "bolsa", "bolsas",
        "limpia", "limpiar", "limpieza", "tirar", "tiradero", "desperdicio", "desechos",
        "reciclaje", "residuos", "sucio", "suciedad", "barrer", "barrendero",
        "alumbrado", "luz", "luces", "luminaria", "luminarias", "iluminacion",
        "lampara", "lamparas", "poste", "postes", "electricidad", "foco", "focos",
        "oscuro", "oscuridad", "alumbrar", "iluminar", "electrico",
    ],
    1: [
        "seguridad", "inseguridad", "policia", "vigilancia", "guardia", "patrulla",
        "patrullas", "delincuencia", "delincuente", "delincuentes", "robo", "robos",
        "ladron", "ladrones", "crimen", "criminal", "secuestro", "asalto", "detenido",
        "peligro", "peligroso", "proteccion", "alarma",
    ],
    2: [
        "drenaje", "alcantarilla", "alcantarillado", "aguas", "negras", "desague",
        "coladera", "zanja", "fuga", "sanitario", "olor", "olores", "pestilencia",
        "inundado", "charco", "desbordamiento", "contaminacion", "contaminado",
        "bache", "baches", "pavimento", "pavimentacion", "asfalto", "reparacion",
        "repavimentacion", "hoyo", "hoyos", "hundimiento", "agrietado", "grieta",
        "cuarteado", "desnivelado", "concreto", "cemento", "transitar", "pavimentar",
        "calle", "calles", "avenida", "avenidas", "banqueta", "banquetas", "camino",
        "carretera", "vialidad", "vialidades", "cruce", "crucero", "esquina",
        "transito", "peatonal", "semaforo", "circulacion", "camellones", "vial",
        "peatones", "rodada", "puente",
    ],
    3: [
        "obra", "obras", "construccion", "proyecto", "proyectos", "infraestructura",
        "desarrollo", "mantenimiento", "edificio", "edificacion", "remodelacion",
        "ampliacion", "rehabilitacion", "mejoramiento", "reconstruccion", "puente", "renovacion",
    ],
    4: [
        "movimiento", "regeneracion", "morenistas", "morenismo", "4t", "cuarta", "transformacion",
        "amlo", "lopez", "obrador", "obradorismo", "sheinbaum",
    ],
    5: [
        "narcomorena", "morenarcos", "morenarco", "narcobrador", "narcapresidente",
        "mugrena", "kks", "kk", "cartel", "narcogobierno", "narcoestado", "dictadura",
        "corruptos", "corrupcion", "rateros", "ladrones", "fraude", "comprados", "vendidos",
        "censura", "mentirosos", "traidores", "adan", "andy", "norona", "monreal",
    ],
    6: [
        "aviadores", "chayote", "chayotera", "chayotero", "cohecho", "coima", "coludidas",
        "coludido", "coludidos", "colusion", "complice", "complices", "complicidad", "confabulacion",
        "corromper", "corrupcion", "corrupta", "corruptas", "corruptazo", "corruptazos", "corrupto",
        "corruptos", "encubrimiento", "defraudado", "defraudo", "desfalco", "desvia", "desviados",
        "desvian", "desviar", "desviaron", "diezmo", "enriquecer", "enriquecerse", "enriquecian",
        "enriquecido", "enriquecimiento", "estafa", "extorsionar", "favoritismo", "fraude", "fraudeadas",
        "fraudelentos", "fraudes", "fraudulento", "lavado", "malversacion", "maiceados", "mocha",
        "mochada", "moche", "moches", "mordida", "nepotismo", "ocultamiento", "peculado", "soborno",
        "transa", "tranza", "tranzas", "vendidos",
    ],
}

TOPIC_NAMES = {
    0: "Servicios",
    1: "Seguridad",
    2: "Vialidades",
    3: "Obras y proyectos",
    4: "Morena (+ / neutro)",
    5: "Morena (-)",
    6: "Corrupcion",
}


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


def normalize_text(text: str) -> str:
    text = text.lower().strip()
    text = text.translate(str.maketrans("áéíóúñü", "aeiounu"))
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def read_wordlist(path: Path) -> set[str]:
    words: set[str] = set()
    if not path.exists():
        return words
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            token = normalize_text(line)
            if token:
                words.add(token)
    return words


def weekly_input_dir(base_dir: Path, since: str) -> Path:
    base_path = Path(base_dir)
    tag = build_report_tag(since, "Datos")
    if base_path.name == tag:
        return base_path
    return base_path / tag


def weekly_output_dir(base_dir: Path, since: str) -> Path:
    base_path = Path(base_dir)
    tag = build_report_tag(since, "Temas_Guiados")
    if base_path.name == tag:
        return base_path
    return base_path / tag


def classify_document(text: str, exclude_words: set[str]) -> tuple[int, dict[int, list[str]]]:
    cleaned = normalize_text(text)
    words = cleaned.split()

    counts = {topic_id: 0 for topic_id in TOPIC_KEYWORDS}
    found = {topic_id: [] for topic_id in TOPIC_KEYWORDS}

    for topic_id, keywords in TOPIC_KEYWORDS.items():
        keyword_set = set(keywords)
        for word in words:
            if word in exclude_words:
                continue
            if word in keyword_set:
                counts[topic_id] += 1
                found[topic_id].append(word)

    sorted_topics = sorted(counts.items(), key=lambda item: item[1], reverse=True)
    if sorted_topics and sorted_topics[0][1] > 0:
        return sorted_topics[0][0], found
    return -1, found


def load_documents(input_week_dir: Path, input_file: str | None) -> list[str]:
    if input_file:
        source = Path(input_file)
        if not source.is_absolute():
            source = input_week_dir / input_file
        if not source.exists():
            raise FileNotFoundError(f"No existe archivo de entrada: {source}")
        lines = source.read_text(encoding="utf-8", errors="ignore").splitlines()
        return [line.strip() for line in lines if line.strip()]

    docs: list[str] = []
    for filename in ("material_institucional.txt", "material_comentarios.txt"):
        path = input_week_dir / filename
        if path.exists():
            lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
            docs.extend([line.strip() for line in lines if line.strip()])
    return docs


def run_topic_analysis(
    docs: list[str],
    output_dir: Path,
    report_tag: str,
    source_label: str,
    exclude_words: set[str],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    results = []
    topics_docs: list[int] = []
    words_by_topic = {topic_id: [] for topic_id in TOPIC_KEYWORDS}

    for idx, doc in enumerate(docs):
        topic_id, found = classify_document(doc, exclude_words)
        topics_docs.append(topic_id)
        for t_id, words in found.items():
            words_by_topic[t_id].extend(words)

        principal = found.get(topic_id, []) if topic_id >= 0 else []
        results.append(
            {
                "doc_id": idx,
                "documento": doc,
                "tema_id": topic_id,
                "tema": TOPIC_NAMES.get(topic_id, "No clasificado"),
                "palabras_clave": ", ".join(sorted(set(principal))),
            }
        )

    df = pd.DataFrame(results)
    topic_counts = Counter(topics_docs)
    unclassified = topic_counts.get(-1, 0)

    csv_name = ensure_tagged_name("clasificacion_temas_guiados", report_tag) + ".csv"
    df.to_csv(output_dir / csv_name, index=False, encoding="utf-8")

    present_topics = sorted([k for k in topic_counts if k >= 0])
    values = [topic_counts[k] for k in present_topics]
    labels = [TOPIC_NAMES[k] for k in present_topics]

    if present_topics:
        plt.figure(figsize=(12, 7))
        plt.bar(present_topics, values, color="steelblue", alpha=0.85)
        plt.xlabel("Tema")
        plt.ylabel("Numero de documentos")
        plt.title(f"Distribucion de temas guiados - {source_label}")
        plt.xticks(present_topics, labels, rotation=35, ha="right")
        plt.grid(axis="y", linestyle="--", alpha=0.5)
        plt.tight_layout()
        chart_name = ensure_tagged_name("distribucion_temas_guiados", report_tag) + ".png"
        plt.savefig(output_dir / chart_name, dpi=240, bbox_inches="tight")
        plt.close()

    all_words: list[str] = []
    for words in words_by_topic.values():
        all_words.extend(words)
    top75 = Counter(all_words).most_common(75)
    top_df = pd.DataFrame(top75, columns=["palabra", "frecuencia"])
    top_name = ensure_tagged_name("top75_palabras_temas_guiados", report_tag) + ".csv"
    top_df.to_csv(output_dir / top_name, index=False, encoding="utf-8")

    txt_name = ensure_tagged_name("informe_temas_guiados", report_tag) + ".txt"
    with (output_dir / txt_name).open("w", encoding="utf-8") as handle:
        handle.write("ANALISIS DE TEMAS GUIADOS POR PALABRAS CLAVE\n")
        handle.write("=" * 72 + "\n\n")
        handle.write(f"Semana: {report_tag}\n")
        handle.write(f"Documentos analizados: {len(docs):,}\n")
        handle.write(f"No clasificados: {unclassified:,}\n")
        handle.write(f"Fecha de ejecucion: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        handle.write("DISTRIBUCION POR TEMA\n")
        handle.write("-" * 72 + "\n")
        for topic_id in sorted([k for k in topic_counts if k >= 0]):
            pct = (topic_counts[topic_id] / max(len(docs), 1)) * 100
            handle.write(
                f"Tema {topic_id} - {TOPIC_NAMES[topic_id]}: {topic_counts[topic_id]:,} docs ({pct:.2f}%)\n"
            )
        pct_unc = (unclassified / max(len(docs), 1)) * 100
        handle.write(f"No clasificados: {unclassified:,} docs ({pct_unc:.2f}%)\n\n")

        handle.write("TOP PALABRAS POR TEMA\n")
        handle.write("-" * 72 + "\n")
        for topic_id in sorted(words_by_topic.keys()):
            if not words_by_topic[topic_id]:
                continue
            topic_top = Counter(words_by_topic[topic_id]).most_common(15)
            rendered = ", ".join([f"{w} ({c})" for w, c in topic_top])
            handle.write(f"Tema {topic_id} - {TOPIC_NAMES[topic_id]}:\n")
            handle.write(f"  {rendered}\n\n")

    log(f"✅ Clasificacion guardada: {csv_name}")
    if present_topics:
        log("✅ Grafico de distribucion generado")
    log(f"✅ Top 75 palabras guardado: {top_name}")
    log(f"✅ Informe guardado: {txt_name}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analisis de temas guiados por keywords para Tampico")
    parser.add_argument("--since", required=True, help="Fecha inicio (YYYY-MM-DD)")
    parser.add_argument("--before", required=True, help="Fecha fin (YYYY-MM-DD)")
    parser.add_argument("--input-dir", help="Directorio base de entrada (default: Datos)")
    parser.add_argument("--output-dir", help="Directorio base de salida (default: Temas_Guiados)")
    parser.add_argument("--input-file", help="Archivo de entrada opcional dentro de la carpeta semanal")
    parser.add_argument(
        "--exclude-words-path",
        default=str(DEFAULT_EXCLUDE_WORDS_PATH),
        help=f"Ruta de palabras a excluir (default: {DEFAULT_EXCLUDE_WORDS_PATH})",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    input_base_dir = Path(args.input_dir) if args.input_dir else repo_root / "Datos"
    output_base_dir = Path(args.output_dir) if args.output_dir else repo_root / "Temas_Guiados"

    input_week_dir = weekly_input_dir(input_base_dir, args.since)
    output_week_dir = weekly_output_dir(output_base_dir, args.since)
    report_tag = build_report_tag(args.since, "Temas_Guiados")

    log("=" * 70)
    log("ANALISIS DE TEMAS GUIADOS - TAMPICO")
    log("=" * 70)
    log(f"Periodo: {args.since} a {args.before}")
    log(f"Input semanal: {input_week_dir}")
    log(f"Output semanal: {output_week_dir}")

    exclude_words_path = Path(args.exclude_words_path)
    exclude_words = read_wordlist(exclude_words_path)
    if exclude_words:
        log(f"✅ Palabras de exclusion cargadas: {len(exclude_words)}")
    else:
        log(f"⚠️ No se cargaron palabras de exclusion desde: {exclude_words_path}")

    try:
        docs = load_documents(input_week_dir, args.input_file)
    except FileNotFoundError as exc:
        log(f"❌ {exc}")
        return 1

    if not docs:
        log("❌ No se encontraron documentos para analizar")
        return 1

    log(f"📖 Documentos cargados: {len(docs):,}")

    run_topic_analysis(
        docs=docs,
        output_dir=output_week_dir,
        report_tag=report_tag,
        source_label=report_tag,
        exclude_words=exclude_words,
    )

    log("🎉 Analisis completado")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
