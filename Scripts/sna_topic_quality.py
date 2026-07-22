#!/usr/bin/env python3
"""Evalua coherencia por tema y agrega una capa editorial recuperable.

La coherencia c_v detecta mezclas semanticas, pero no siempre identifica
artefactos editoriales (por ejemplo, columnas o resúmenes informativos). Por
eso se combina con reglas por firma léxica. Los temas de calidad baja no se
eliminan: quedan marcados para ocultarlos por defecto en la interfaz.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CLUSTERS = REPO_ROOT / "SNA" / "Resultados" / "historico" / "clusters"


@dataclass(frozen=True)
class EditorialRule:
    signature: frozenset[str]
    minimum: int
    title: str
    summary: str
    quality: str
    reason: str


EDITORIAL_RULES = (
    EditorialRule(
        frozenset({"temporada", "accidente", "educacion", "petrolero"}), 3,
        "Educación, accidentes y actividad petrolera",
        "Reúne conversación sobre educación y temporada escolar junto con accidentes y actividad petrolera. Es un agrupamiento mixto: conviene abrir sus mensajes antes de atribuirle una sola interpretación.",
        "baja", "mezcla asuntos con poca relación entre sí",
    ),
    EditorialRule(
        frozenset({"responder", "concierto", "internet", "publico"}), 3,
        "Atención pública, entretenimiento y conectividad",
        "Combina respuestas de atención al público con menciones a conciertos e internet. La coincidencia parece provenir del formato de publicación más que de un asunto único.",
        "baja", "mezcla atención institucional con entretenimiento y conectividad",
    ),
    EditorialRule(
        frozenset({"puente", "restaurante", "metro", "sanitario"}), 3,
        "Infraestructura, comercio y alertas sanitarias",
        "Agrupa referencias a infraestructura y movilidad con restaurantes y alertas sanitarias. Debe leerse como un cruce de conversaciones, no como un tema homogéneo.",
        "baja", "agrupamiento heterogéneo de infraestructura y comercio",
    ),
    EditorialRule(
        frozenset({"laguna", "registro", "informacion", "movilidad"}), 3,
        "Registros, movilidad y condiciones locales",
        "Reúne solicitudes de información y registros con referencias a movilidad y condiciones de distintos lugares. Su amplitud reduce la precisión temática.",
        "baja", "tema amplio con contextos locales distintos",
    ),
    EditorialRule(
        frozenset({"columna", "informativo", "domingo", "noticia"}), 3,
        "Contenido editorial y agenda informativa",
        "Concentra columnas, cortes informativos y publicaciones de agenda. Describe principalmente un formato editorial, por lo que no debe interpretarse como una preocupación ciudadana sustantiva.",
        "baja", "artefacto de formato editorial",
    ),
    EditorialRule(
        frozenset({"hermoso", "cierre", "gustar", "vivienda", "vacante"}), 3,
        "Vivienda, imagen urbana y reacciones generales",
        "Combina referencias a vivienda e imagen urbana con cierres y expresiones generales de agrado. Puede contener conversación útil, pero exige revisar los mensajes para separar sus componentes.",
        "media", "mezcla un asunto urbano con reacciones genéricas",
    ),
    EditorialRule(
        frozenset({"quedar", "conversacion", "correo", "mandar"}), 3,
        "Opiniones generales y contenido compartido",
        "Agrupa intercambios conversacionales, envíos y referencias generales. La unión parece depender de fórmulas de interacción y no de un problema público definido.",
        "baja", "lenguaje conversacional poco específico",
    ),
    EditorialRule(
        frozenset({"semana", "noticias", "articulo", "resultado"}), 3,
        "Agenda noticiosa y resultados",
        "Reúne resúmenes de la semana, artículos y resultados de distintos ámbitos. Es útil como agenda, pero no representa por sí solo un tema sustantivo coherente.",
        "baja", "agregación por formato de resumen",
    ),
    EditorialRule(
        frozenset({"resumen", "asociacion", "popular", "entretenimiento"}), 3,
        "Resúmenes, asociaciones y entretenimiento",
        "Combina resúmenes, menciones a asociaciones y contenido de entretenimiento. Se conserva para auditoría, aunque su interpretación temática es débil.",
        "baja", "mezcla de formatos y asuntos no relacionados",
    ),
    EditorialRule(
        frozenset({"agenda", "evento", "musica", "viaje"}), 3,
        "Agenda cultural, eventos y viajes",
        "Conversa sobre actividades culturales, música, eventos y desplazamientos asociados. Aunque es amplio, mantiene un hilo reconocible de agenda pública y recreativa.",
        "media", "agenda pública amplia pero interpretable",
    ),
    EditorialRule(
        frozenset({"playa", "abril", "comision", "cambio", "meteorologico"}), 3,
        "Playa, cambios ambientales y actividades públicas",
        "Combina referencias a playa y fenómenos meteorológicos con comisiones, artistas y actividades públicas. Se conserva como cruce exploratorio, pero no debe leerse como un asunto homogéneo.",
        "baja", "mezcla ambiente, instituciones y actividades culturales",
    ),
    EditorialRule(
        frozenset({"publico", "hernandez", "alcalde", "politico", "recurso"}), 3,
        "Funcionarios, cargos y recursos públicos",
        "Concentra nombres de funcionarios, cargos políticos y referencias al uso de recursos públicos. La presencia de nombres propios y contenido editorial reduce su cohesión como asunto único.",
        "baja", "agrupación por nombres propios y cargos públicos",
    ),
    EditorialRule(
        frozenset({"gobernador", "futbol", "americo", "equipo", "financiero"}), 3,
        "Gobernador, fútbol y referencias económicas",
        "Mezcla menciones al gobernador Américo Villarreal con fútbol y asuntos financieros. La coincidencia no sostiene una interpretación temática única.",
        "baja", "mezcla política, deporte y economía",
    ),
    EditorialRule(
        frozenset({"nacional", "veracruz", "proceso", "sede", "sanitario", "arma"}), 3,
        "Convocatorias, sedes y asuntos nacionales diversos",
        "Agrupa convocatorias y sedes con referencias sanitarias, educativas y de seguridad. Es un conjunto heterogéneo que requiere separar sus conversaciones originales.",
        "baja", "mezcla convocatorias, salud, educación y seguridad",
    ),
)


def parse_topic_words(raw: Any) -> list[str]:
    return [
        part.split("(", 1)[0].strip().lower()
        for part in str(raw).split(",")
        if part.split("(", 1)[0].strip()
    ]


def matching_rule(words: list[str]) -> EditorialRule | None:
    word_set = set(words[:20])
    matches = [
        (len(word_set & rule.signature), rule)
        for rule in EDITORIAL_RULES
        if len(word_set & rule.signature) >= rule.minimum
    ]
    return max(matches, key=lambda item: item[0])[1] if matches else None


def coherence_quality(value: float) -> str:
    if value < 0.27:
        return "baja"
    if value < 0.42:
        return "media"
    return "alta"


def compute_per_topic_coherence(
    corpus_path: Path,
    topics: list[list[str]],
) -> list[float | None]:
    from gensim.corpora import Dictionary
    from gensim.models.coherencemodel import CoherenceModel

    corpus_df = pd.read_csv(corpus_path, usecols=["lemas"])
    docs = [str(value).split() for value in corpus_df["lemas"].fillna("")]
    dictionary = Dictionary(docs)
    filtered_topics = [
        [word for word in topic if word in dictionary.token2id]
        for topic in topics
    ]
    valid_indices = [index for index, topic in enumerate(filtered_topics) if topic]
    if not valid_indices:
        return [None] * len(topics)

    valid_topics = [filtered_topics[index] for index in valid_indices]
    model = CoherenceModel(
        topics=valid_topics,
        texts=docs,
        dictionary=dictionary,
        coherence="c_v",
        processes=1,
    )
    result: list[float | None] = [None] * len(topics)
    for index, value in zip(valid_indices, model.get_coherence_per_topic()):
        result[index] = float(value)
    return result


def enrich_topics(clusters_dir: Path) -> pd.DataFrame:
    topics_path = clusters_dir / "temas_terminos.csv"
    corpus_path = clusters_dir / "corpus_modelado.csv"
    if not topics_path.exists() or not corpus_path.exists():
        raise SystemExit(f"Faltan insumos: {topics_path} o {corpus_path}")

    topics_df = pd.read_csv(topics_path)
    if not {"tema_id", "top_20_terminos"}.issubset(topics_df.columns):
        raise SystemExit("temas_terminos.csv no contiene tema_id y top_20_terminos")
    topic_words = [parse_topic_words(value) for value in topics_df["top_20_terminos"]]
    coherences = compute_per_topic_coherence(corpus_path, topic_words)

    enriched: list[dict[str, Any]] = []
    for (_, row), words, coherence in zip(topics_df.iterrows(), topic_words, coherences):
        missing_coherence = coherence is None
        coherence_value = 0.0 if missing_coherence else coherence
        rule = None if missing_coherence else matching_rule(words)
        if missing_coherence:
            quality = "baja"
            title = "Tema sin términos suficientes"
            summary = (
                "El modelo no asignó términos con peso suficiente a este tema. "
                "Se conserva para mantener la numeración del análisis, pero se oculta "
                "por defecto y no debe interpretarse como una conversación sustantiva."
            )
            reason = "sin términos válidos para calcular coherencia"
        elif rule:
            quality = rule.quality
            title = rule.title
            summary = rule.summary
            reason = rule.reason
        else:
            quality = coherence_quality(coherence_value)
            title = ""

        if not missing_coherence and not rule and quality == "baja":
            summary = (
                "Este agrupamiento tiene baja coherencia estadística y puede mezclar conversaciones distintas. "
                "Se conserva para auditoría, pero conviene revisar sus mensajes antes de usarlo en conclusiones."
            )
            reason = "coherencia c_v baja"
        elif not missing_coherence and not rule:
            summary = ""
            reason = "coherencia c_v"
        enriched.append({
            **row.to_dict(),
            "coherencia_tema_cv": round(coherence_value, 4),
            "calidad_tema": quality,
            "visible_por_defecto": quality != "baja",
            "titulo_curado": title,
            "resumen_curado": summary,
            "motivo_calidad": reason,
            "curacion_manual": bool(rule),
        })

    result = pd.DataFrame(enriched)
    result.to_csv(topics_path, index=False)
    report = {
        "n_temas": int(len(result)),
        "calidades": result["calidad_tema"].value_counts().to_dict(),
        "ocultos_por_defecto": result.loc[~result["visible_por_defecto"], "tema_id"].astype(int).tolist(),
        "criterio": "coherencia c_v por tema + reglas editoriales por firma lexica",
        "nota": "los temas de calidad baja se conservan y pueden recuperarse en la interfaz",
    }
    (clusters_dir / "calidad_temas.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clusters-dir", type=Path, default=DEFAULT_CLUSTERS)
    args = parser.parse_args()
    result = enrich_topics(args.clusters_dir)
    counts = result["calidad_tema"].value_counts().to_dict()
    print(f"Calidad temática: {counts}")
    print(f"Temas ocultos por defecto: {int((~result['visible_por_defecto']).sum())}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
