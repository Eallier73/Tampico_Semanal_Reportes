#!/usr/bin/env python3
"""
SNA guiado: variante independiente de la red de posiciones discursivas.

Superpone temas rastreados y polaridad sin modificar el script ni el HTML de
19_red_posiciones_discursivas.py. Escribe en clusters/red_guiada/.

Construye una red nueva, separada de 12c_red_completa.py:
  - tema -> posicion discursiva -> cuentas / palabras
  - posiciones calculadas por similitud de vocabulario y subclusters usados
  - metricas indiciarias de baja espontaneidad aparente por posicion

Entradas esperadas:
  SNA/Datos/tampico_datos_tabulares_consolidados.csv
  SNA/Resultados/historico/cuentas_clusters/
  SNA/Resultados/historico/clusters/

Salidas:
  SNA/Resultados/historico/clusters/red_posiciones/
    red_tampico_posiciones.html
    posiciones_discursivas.csv
    cuentas_posiciones.csv
    palabras_posiciones.csv
    ejemplos_posiciones.csv
    metricas_posiciones.json
"""

from __future__ import annotations

import argparse
import html
import importlib.util
import json
import math
import re
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.feature_extraction import DictVectorizer
from sklearn.feature_extraction.text import TfidfTransformer

from sna_guiada_common import (
    DEFAULT_NEGATIVE_DICTIONARY,
    DEFAULT_POSITIVE_DICTIONARY,
    DEFAULT_TOPIC_DICTIONARY,
    STRATEGIC_GROUP_BY_POLARITY,
    aggregate_words,
    aggregate_stance_texts,
    apply_stance_evidence,
    annotate_word,
    inject_guided_layer,
    load_lexicons,
    stance_from_evidence,
    write_annotation_outputs,
)
from sna_spanish_filter import (
    DEFAULT_ENGLISH_DICTIONARY,
    DEFAULT_SPANISH_DICTIONARY,
    is_spanish_word,
    load_language_vocabulary,
)
from sna_position_labels import classify_position_name

TEMA_COLORS = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
    "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
    "#aec7e8", "#ffbb78", "#98df8a",
]

PLATFORM_COLORS = {
    "YouTube": "#ff2c2c",
    "Twitter": "#1da1f2",
    "X": "#111111",
    "Facebook": "#1877f2",
    "Instagram": "#e1306c",
    "TikTok": "#ff0050",
    "Medios": "#777777",
}

LEVEL_COLORS = {
    "bajo": "#8a8a8a",
    "medio": "#ffcc33",
    "alto": "#ff4d4d",
}
STRATEGIC_GROUP_LABELS = {
    "risk": "Riesgo",
    "opportunity": "Oportunidad",
    "consolidation": "Consolidación",
}

SUPPORT_WORDS = {
    "gracias", "apoyo", "bien", "excelente", "felicidades", "defender",
    "defensa", "transformacion", "honesto", "orgullo", "pueblo",
}
CRITIC_WORDS = {
    "ratero", "delincuente", "corrupto", "corrupcion", "mentira", "mentiroso",
    "narco", "mierda", "pésimo", "pesimo", "culpa", "desastre", "criminal",
}
CLAIM_WORDS = {
    "justicia", "exigir", "derecho", "madre", "buscador", "desaparecido",
    "seguridad", "miedo", "dolor", "protesta", "manifestacion", "ayuda",
}

NOISE_WORDS = {
    "quot", "amp", "etc", "bla", "http", "https", "www", "com", "nan",
    "rt", "htt", "youtu", "youtube", "facebook", "twitter",
}

MAX_TOPIC_ENGLISH_SHARE = 0.50

TOPIC_FRAMES = [
    {
        "title": "Gestión municipal y servicios públicos",
        "subject": "la gestión del municipio y la calidad de los servicios cotidianos",
        "inference": "sugiere que se evalúan respuestas institucionales, obras y necesidades de las colonias",
        "triggers": {"tampico", "municipal", "municipio", "alcalde", "alcaldesa", "villarreal", "monica", "servicio", "colonia", "publico", "obra", "calle", "alumbrado", "basura"},
    },
    {
        "title": "Agua, territorio y medio ambiente",
        "subject": "el agua, el territorio y sus efectos ambientales o comunitarios",
        "inference": "apunta a preocupaciones por abasto, infraestructura, contaminación o cuidado del entorno",
        "triggers": {"agua", "lluvia", "laguna", "rio", "mar", "playa", "contaminacion", "ambiente", "drenaje", "inundacion", "sequia", "clima"},
    },
    {
        "title": "Movilidad e infraestructura urbana",
        "subject": "la movilidad, las calles y la infraestructura urbana",
        "inference": "permite observar problemas de tránsito, mantenimiento, accesibilidad y ejecución de obra pública",
        "triggers": {"transito", "trafico", "vialidad", "carretera", "puente", "calle", "avenida", "camion", "transporte", "auto", "obra", "pavimento", "banqueta"},
    },
    {
        "title": "Seguridad, violencia y justicia",
        "subject": "la seguridad, la violencia y las demandas de justicia",
        "inference": "sugiere relatos de riesgo, denuncia, victimización o exigencia de intervención de las autoridades",
        "triggers": {"seguridad", "violencia", "delito", "delincuente", "policia", "matar", "muerte", "asesinato", "criminal", "justicia", "desaparecido", "victima", "robar", "robo", "narco"},
    },
    {
        "title": "Gobierno y disputa política",
        "subject": "el gobierno, los partidos y la competencia política",
        "inference": "indica una discusión sobre liderazgo, decisiones públicas, identidades partidistas y responsabilidades de gobierno",
        "triggers": {"gobierno", "presidente", "presidenta", "morena", "pan", "pri", "partido", "politico", "politica", "eleccion", "voto", "obrador", "claudia", "sheinbaum"},
    },
    {
        "title": "Corrupción y rendición de cuentas",
        "subject": "la corrupción, el uso de recursos y la rendición de cuentas",
        "inference": "sugiere acusaciones, sospechas o exigencias de transparencia dirigidas a actores públicos",
        "triggers": {"corrupcion", "corrupto", "ratero", "robar", "dinero", "recurso", "impunidad", "mentira", "fraude", "denuncia", "transparencia"},
    },
    {
        "title": "Economía, trabajo y costo de vida",
        "subject": "el trabajo, el dinero y las condiciones económicas de la vida diaria",
        "inference": "apunta a preocupaciones por ingresos, precios, empleo, comercio y capacidad de las familias para sostenerse",
        "triggers": {"trabajo", "trabajador", "empleo", "dinero", "precio", "pesos", "pagar", "economia", "comercio", "negocio", "salario", "pobreza", "impuesto"},
    },
    {
        "title": "Bienestar, salud y educación",
        "subject": "el bienestar social, la salud y la educación",
        "inference": "sugiere demandas de atención, acceso a derechos y valoración de instituciones que cuidan o forman a la población",
        "triggers": {"salud", "hospital", "medico", "enfermo", "educacion", "escuela", "maestro", "estudiante", "bienestar", "pension", "apoyo", "ayuda", "derecho"},
    },
    {
        "title": "Comunidad, familia y vida cotidiana",
        "subject": "la vida comunitaria, las familias y las experiencias cotidianas",
        "inference": "reúne relatos personales y valoraciones sobre convivencia, cuidado y problemas compartidos",
        "triggers": {"familia", "madre", "padre", "hijo", "persona", "gente", "vecino", "comunidad", "casa", "vida", "dia", "amigo", "mujer", "hombre"},
    },
    {
        "title": "Protesta, derechos y participación",
        "subject": "la protesta, los derechos y la participación ciudadana",
        "inference": "apunta a expresiones de inconformidad, organización colectiva y exigencia de respuesta pública",
        "triggers": {"protesta", "marcha", "manifestacion", "exigir", "derecho", "pueblo", "ciudadano", "movimiento", "lucha", "libertad", "defender", "justicia"},
    },
    {
        "title": "Apoyo y aprobación pública",
        "subject": "las expresiones de apoyo, gratitud y aprobación",
        "inference": "permite identificar respaldo emocional o político hacia personas, decisiones y acciones públicas",
        "triggers": {"gracias", "excelente", "felicidad", "felicidades", "bien", "apoyo", "presidenta", "seguir", "orgullo", "bravo", "bendicion"},
    },
    {
        "title": "Crítica, conflicto y desaprobación",
        "subject": "la crítica, el conflicto y la desaprobación pública",
        "inference": "concentra juicios negativos, confrontación y reclamos hacia actores o situaciones percibidas como problemáticas",
        "triggers": {"mal", "peor", "mentira", "culpa", "corrupto", "ratero", "critica", "problema", "vergüenza", "odio", "desastre", "mierda"},
    },
    {
        "title": "Identidad local, cultura y entretenimiento",
        "subject": "la identidad local, la cultura, el deporte y el entretenimiento",
        "inference": "sugiere conversaciones que construyen pertenencia, celebran acontecimientos o comentan contenidos de interés público",
        "triggers": {"tampico", "jaibo", "futbol", "partido", "mundial", "musica", "video", "programa", "fiesta", "turismo", "playa", "historia", "cultura"},
    },
    {
        "title": "México y asuntos nacionales",
        "subject": "la identidad nacional y los asuntos públicos de México",
        "inference": "vincula la conversación local con debates nacionales, figuras federales y valoraciones sobre el rumbo del país",
        "triggers": {"mexico", "mexicano", "pais", "nacional", "federal", "presidente", "presidenta", "pueblo", "patria", "mundo"},
    },
    {
        "title": "Medios y conversación digital",
        "subject": "la circulación de información en medios y plataformas digitales",
        "inference": "apunta a reacciones frente a noticias, videos, transmisiones y formas de amplificación en línea",
        "triggers": {"video", "noticia", "medio", "canal", "red", "publicar", "comentario", "informacion", "television", "periodista", "facebook", "twitter", "youtube"},
    },
]

TOKEN_RE = re.compile(r"[a-záéíóúüñ]{3,}", re.IGNORECASE)
DISPLAY_TOKEN_RE = re.compile(r"[a-záéíóúüñ0-9]+", re.IGNORECASE)
NADER_STRONG_RE = re.compile(
    r"\b(?:chucho\s*nader|diputad[oa]\s+nader|"
    r"jesus\s+antonio\s+nader\s+nasrallah|nader\s+nasrallah)\b",
    re.IGNORECASE,
)
JESUS_NADER_RE = re.compile(r"\bjesus\s+nader\b", re.IGNORECASE)
NADER_CONTEXT_RE = re.compile(
    r"\b(?:tampico|diputad[oa]|alcalde(?:sa)?|pan|nasrallah)\b",
    re.IGNORECASE,
)
DISPLAY_NAME_STOP_WORDS = {
    "a", "al", "ante", "con", "de", "del", "desde", "e", "el", "en", "entre",
    "hacia", "la", "las", "los", "para", "por", "sobre", "su", "sus", "un",
    "una", "unas", "unos", "y",
}
GENERIC_POSITION_WORDS = {
    "conversacion", "enfasis", "grupo", "perspectiva", "posicion", "tema",
}
DISPLAY_UPPERCASE_WORDS = {"cdmx", "eeuu", "imss", "inegi", "unam"}
DEFAULT_BASE = Path(__file__).resolve().parent.parent / "SNA" / "Resultados" / "historico"
DEFAULT_DATA = Path(__file__).resolve().parent.parent / "SNA" / "Datos" / "tampico_datos_tabulares_consolidados.csv"


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def stacked_label(value: Any) -> str:
    """Muestra cada palabra de las etiquetas principales en un renglón."""
    return "\n".join(str(value).split())


def _topic_key(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value).strip().lower())
    return "".join(char for char in text if not unicodedata.combining(char))


def matches_jesus_nader(value: Any) -> bool:
    """Reconoce al actor político sin aceptar el apellido Nader aislado."""
    normalized = _topic_key(value)
    if NADER_STRONG_RE.search(normalized):
        return True
    return bool(
        JESUS_NADER_RE.search(normalized)
        and NADER_CONTEXT_RE.search(normalized)
    )


def row_matches_jesus_nader(row: pd.Series) -> bool:
    """Reconoce menciones en texto y metadatos trazables de la publicación."""
    direct_evidence = " ".join(
        str(row.get(column, "") or "")
        for column in (
            "texto_limpio",
            "usuario",
            "titulo_contexto",
            "autor_contexto",
            "url_contexto",
        )
    )
    if matches_jesus_nader(direct_evidence):
        return True

    query = re.sub(
        r"[^a-z0-9]+",
        " ",
        _topic_key(row.get("query_busqueda", "")),
    ).strip()
    return query in {
        "chucho nader",
        "diputado nader",
        "jesus nader",
        "jesus antonio nader",
        "jesus antonio nader nasrallah",
        "nader nasrallah",
    }


def add_nader_structural_branch(
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    meta: dict[str, dict[str, Any]],
    mensajes: pd.DataFrame,
    spanish_vocabulary: frozenset[str],
    english_vocabulary: frozenset[str],
    words_limit: int,
) -> tuple[set[str], set[str]]:
    """Añade una rama rastreada y reutiliza palabras reales como puentes."""
    matched = mensajes[mensajes.apply(row_matches_jesus_nader, axis=1)].copy()
    if matched.empty:
        return set(), set()

    root_id = "STRUCT_NADER"
    matched["engagement"] = matched["engagement"].fillna(0).astype(float)
    examples = (
        matched.sort_values("engagement", ascending=False)
        .head(8)[
            ["usuario", "plataforma", "fecha", "texto_limpio", "engagement"]
        ]
        .rename(columns={"texto_limpio": "texto"})
        .to_dict("records")
    )
    nodes.append({
        "id": root_id,
        "label": stacked_label("Jesús Nader"),
        "title": "Conversación rastreada sobre Jesús Nader",
        "short_name": "Jesús Nader",
        "kind": "posicion",
        "ibea": "bajo",
        "shape": "diamond",
        "color": {
            "background": "#0057b8",
            "border": "#ffffff",
            "highlight": {"background": "#0057b8", "border": "#ffffff"},
        },
        "borderWidth": 3,
        "font": {
            "size": 42,
            "color": "#ffffff",
            "face": "arial",
            "strokeWidth": 4,
            "strokeColor": "#111111",
        },
        "size": 30,
        "x": 0,
        "y": -460,
    })
    meta[root_id] = {
        "kind": "posicion",
        "nombre": "Jesús Nader",
        "short_name": "Jesús Nader",
        "resumen": (
            "Mensajes en español que mencionan Chucho Nader, Diputado Nader "
            "o Jesús Nader con contexto de Tampico, alcaldía, PAN o Nasrallah."
        ),
        "n_cuentas": int(matched["usuario"].nunique()),
        "n_msgs": int(len(matched)),
        "n_palabras_tema": 0,
        "top_words": [],
        "top_subclusters": "",
        "ibea_score": 0.0,
        "ibea_nivel": "bajo",
        "postura_actor": "sin_postura",
        "postura_actor_etiqueta": "Sin postura",
        "postura_actor_score": 0.0,
        "postura_actor_evidencia": 0.0,
        "postura_actor_mensajes": 0,
        "metricas": {},
        "examples": examples,
    }

    structural_ids = {root_id}
    bridge_ids: set[str] = set()
    existing_accounts = {
        str(item.get("usuario", "")): str(node_id)
        for node_id, item in meta.items()
        if item.get("kind") == "cuenta" and item.get("usuario")
    }
    for index, (usuario, group) in enumerate(
        matched.groupby("usuario", sort=True), 1
    ):
        node_id = existing_accounts.get(str(usuario))
        if node_id is None:
            node_id = f"STRUCT_NADER_A{index}"
            platform = str(group["plataforma"].iloc[0])
            color = PLATFORM_COLORS.get(platform, "#777777")
            angle = 2 * math.pi * (index - 1) / max(
                1, matched["usuario"].nunique()
            )
            nodes.append({
                "id": node_id,
                "label": "",
                "search": str(usuario),
                "kind": "cuenta",
                "shape": "dot",
                "color": {"background": color, "border": "#222222"},
                "font": {"size": 0, "color": "#ffffff", "face": "arial"},
                "size": 10,
                "x": round(math.cos(angle) * 95, 2),
                "y": round(-460 + math.sin(angle) * 95, 2),
            })
            meta[node_id] = {
                "kind": "cuenta",
                "usuario": str(usuario),
                "plataformas": [platform],
                "n_msgs": int(len(group)),
                "n_palabras": 0,
                "tema_dominante": "Jesús Nader",
                "sub_dominante": "",
                "posiciones": [
                    {
                        "posicion_id": root_id,
                        "tema_id": None,
                        "conteo_tema": int(len(group)),
                        "pct_usuario_en_tema": 100.0,
                    }
                ],
            }
        structural_ids.add(node_id)
        edges.append({
            "id": f"struct_nader_account_{index}",
            "from": root_id,
            "to": node_id,
            "value": max(1, int(len(group))),
            "kind": "posicion_cuenta",
            "color": {"color": "#0057b8", "opacity": 0.65},
            "dashes": True,
        })

    word_counts: Counter[str] = Counter()
    for value in matched["lemas"].fillna("").astype(str):
        for word in value.split():
            if (
                word.lower() not in NOISE_WORDS
                and is_spanish_word(
                    word, spanish_vocabulary, english_vocabulary
                )
            ):
                word_counts[word.lower()] += 1
    selected_words = word_counts.most_common(max(1, words_limit))
    meta[root_id]["top_words"] = [word for word, _ in selected_words]
    meta[root_id]["n_palabras_tema"] = int(sum(word_counts.values()))
    max_count = max((count for _, count in selected_words), default=1)
    existing_word_nodes = {
        _topic_key(item.get("palabra", "")): str(node_id)
        for node_id, item in meta.items()
        if item.get("kind") == "palabra" and item.get("palabra")
    }
    for index, (word, count) in enumerate(selected_words, 1):
        node_id = existing_word_nodes.get(_topic_key(word))
        if node_id is not None:
            positions = meta[node_id].setdefault("posiciones", [])
            if not any(
                position.get("posicion_id") == root_id
                for position in positions
            ):
                positions.append({
                    "posicion_id": root_id,
                    "tema_id": None,
                    "conteo": int(count),
                    "rank": index,
                })
            bridge_ids.add(node_id)
            edges.append({
                "id": f"struct_nader_word_bridge_{index}",
                "from": root_id,
                "to": node_id,
                "value": max(1, int(count)),
                "kind": "posicion_palabra",
                "color": {"color": "#0057b8", "opacity": 0.75},
            })
            continue

        node_id = f"STRUCT_NADER_W{index}"
        angle = 2 * math.pi * (index - 1) / max(1, len(selected_words))
        radius = 145 + 24 * ((index - 1) // 24)
        nodes.append({
            "id": node_id,
            "label": word,
            "kind": "palabra",
            "shape": "dot",
            "color": {"background": "#222222", "border": "#0057b8"},
            "font": {"size": 16, "color": "#f7f7f7", "face": "arial"},
            "size": scalar_size(float(count), 1, max_count, 8, 22),
            "x": round(math.cos(angle) * radius, 2),
            "y": round(-460 + math.sin(angle) * radius, 2),
        })
        meta[node_id] = {
            "kind": "palabra",
            "palabra": word,
            "posiciones": [
                {
                    "posicion_id": root_id,
                    "tema_id": None,
                    "conteo": int(count),
                    "rank": index,
                }
            ],
        }
        structural_ids.add(node_id)
        edges.append({
            "id": f"struct_nader_word_{index}",
            "from": root_id,
            "to": node_id,
            "value": max(1, int(count)),
            "kind": "posicion_palabra",
            "color": {"color": "#0057b8", "opacity": 0.55},
        })
    return structural_ids, bridge_ids


def filter_spanish_word_rows(
    rows: pd.DataFrame,
    spanish_vocabulary: frozenset[str],
    english_vocabulary: frozenset[str],
) -> pd.DataFrame:
    """Retira del vocabulario analítico las filas que no son español."""
    if "palabra" not in rows.columns:
        return rows.copy()
    mask = rows["palabra"].map(
        lambda value: is_spanish_word(
            value, spanish_vocabulary, english_vocabulary
        )
    )
    return rows.loc[mask].copy()


def english_dominated_topics(
    rows: pd.DataFrame,
    spanish_vocabulary: frozenset[str],
    english_vocabulary: frozenset[str],
    max_english_share: float = MAX_TOPIC_ENGLISH_SHARE,
) -> dict[int, float]:
    """Detecta temas cuyo vocabulario ponderado es mayoritariamente inglés."""
    if not {"tema", "palabra", "conteo"}.issubset(rows.columns):
        return {}
    checked = rows[["tema", "palabra", "conteo"]].copy()
    checked["es_espanol"] = checked["palabra"].map(
        lambda value: is_spanish_word(
            value, spanish_vocabulary, english_vocabulary
        )
    )
    output: dict[int, float] = {}
    for topic_id, group in checked.groupby("tema"):
        total = float(group["conteo"].sum())
        if total <= 0:
            continue
        english_weight = float(
            group.loc[~group["es_espanol"], "conteo"].sum()
        )
        share = english_weight / total
        if share > max_english_share:
            output[int(topic_id)] = share
    return output


def display_word_case(word: str) -> str:
    if _topic_key(word) in DISPLAY_UPPERCASE_WORDS:
        return word.upper()
    return word[:1].upper() + word[1:]


def compact_display_name(
    label: str,
    evidence: list[str] | None = None,
    fallback: str = "Sin nombre",
) -> str:
    """Crea una etiqueta semántica, legible y de máximo tres palabras."""
    evidence = evidence or []
    label_key = _topic_key(label)
    use_evidence_first = label_key.startswith(("perspectiva ", "conversacion "))
    sources = [evidence, DISPLAY_TOKEN_RE.findall(str(label))]
    if not use_evidence_first:
        sources.reverse()

    selected: list[str] = []
    seen: set[str] = set()
    for source in sources:
        for raw_word in source:
            word = str(raw_word).strip()
            key = _topic_key(word)
            if (
                not key
                or key.isdigit()
                or key in DISPLAY_NAME_STOP_WORDS
                or key in GENERIC_POSITION_WORDS
                or key in seen
            ):
                continue
            selected.append(word)
            seen.add(key)
            if len(selected) == 3:
                break
        if len(selected) == 3:
            break

    result_words = selected or DISPLAY_TOKEN_RE.findall(str(fallback))[:3]
    return " ".join(display_word_case(word) for word in result_words)


def compact_topic_display_name(
    label: str,
    evidence: list[str] | None = None,
    fallback: str = "Sin tema",
) -> str:
    """Conserva dos palabras del asunto y una palabra distintiva del tema."""
    evidence = evidence or []
    parts = str(label).split("·", 1)
    if len(parts) == 1:
        return compact_display_name(label, evidence, fallback)

    base_words = DISPLAY_TOKEN_RE.findall(parts[0])
    distinctive_words = DISPLAY_TOKEN_RE.findall(parts[1])
    selected: list[str] = []
    seen: set[str] = set()

    def add_words(words: list[str], limit: int) -> None:
        for raw_word in words:
            word = str(raw_word).strip()
            key = _topic_key(word)
            if (
                not key
                or key.isdigit()
                or key in DISPLAY_NAME_STOP_WORDS
                or key in GENERIC_POSITION_WORDS
                or key in seen
            ):
                continue
            selected.append(word)
            seen.add(key)
            if len(selected) >= limit:
                return

    add_words(base_words, 2)
    add_words(distinctive_words, 3)
    add_words(evidence, 3)
    if not selected:
        return compact_display_name("", evidence, fallback)
    return " ".join(display_word_case(word) for word in selected[:3])


def build_topic_reading(topic_id: int, words: list[str]) -> dict[str, str]:
    """Produce una lectura semantica prudente a partir de los terminos LDA."""
    clean_words = list(dict.fromkeys(str(word).strip() for word in words if str(word).strip()))
    normalized = [_topic_key(word) for word in clean_words[:16]]
    ranked_frames = []
    for frame in TOPIC_FRAMES:
        hits = [clean_words[index] for index, word in enumerate(normalized) if word in frame["triggers"]]
        score = sum(16 - index for index, word in enumerate(normalized) if word in frame["triggers"])
        if hits:
            ranked_frames.append((score, len(hits), frame, hits))
    ranked_frames.sort(key=lambda item: (item[0], item[1]), reverse=True)

    evidence = ", ".join(clean_words[:6])
    if ranked_frames:
        _, _, primary, _ = ranked_frames[0]
        distinct = next(
            (item for item in ranked_frames[1:] if item[2]["title"] != primary["title"] and item[1] >= 2),
            None,
        )
        suffix = next(
            (word for word in clean_words[:4] if _topic_key(word) not in primary["triggers"]),
            clean_words[0] if clean_words else f"tema {topic_id}",
        )
        title = f'{primary["title"]} · {suffix}'
        summary = (
            f'Este tema concentra mensajes sobre {primary["subject"]}. '
            f'La combinación de {evidence} {primary["inference"]}.'
        )
        if distinct:
            summary += f' También se cruza con {distinct[2]["subject"]}.'
    else:
        title = ", ".join(clean_words[:3]).capitalize() or f"Tema {topic_id}"
        summary = (
            f"Este grupo combina referencias a {evidence}. "
            "Más que representar un asunto único, parece reunir experiencias, acciones y valoraciones que aparecen juntas en la conversación."
        )
    summary += " Es una guía interpretativa basada en vocabulario compartido, no una etiqueta definitiva."
    return {"title": title, "summary": summary}


def load_topic_info(
    base: Path,
    spanish_vocabulary: frozenset[str],
    english_vocabulary: frozenset[str],
) -> dict[int, dict[str, Any]]:
    """Reusa las lecturas curadas de 12c cuando estan disponibles."""
    info: dict[int, dict[str, Any]] = {}
    script_path = repo_root() / "Scripts" / "12c_red_completa.py"
    if script_path.exists():
        spec = importlib.util.spec_from_file_location("red_completa_12c", script_path)
        if spec and spec.loader:
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)  # type: ignore[union-attr]
            for tid, row in getattr(mod, "TOPIC_READINGS", {}).items():
                info[int(tid)] = {
                    "title": row.get("title", f"T{int(tid):02d}"),
                    "summary": row.get("summary", ""),
                    "words": [],
                }

    temas_path = base / "clusters" / "temas_terminos.csv"
    if temas_path.exists():
        temas = pd.read_csv(temas_path)
        if {"tema_id", "top_20_terminos"}.issubset(temas.columns):
            for _, row in temas.iterrows():
                tid = int(row["tema_id"])
                words = []
                for part in str(row["top_20_terminos"]).split(","):
                    word = part.split("(")[0].strip()
                    if word and is_spanish_word(
                        word, spanish_vocabulary, english_vocabulary
                    ):
                        words.append(word)
                reading = build_topic_reading(tid, words)
                info.setdefault(tid, {"title": reading["title"], "summary": reading["summary"], "words": []})
                info[tid]["words"] = words[:40]
                curated_title = str(row.get("titulo_curado", "")).strip()
                curated_summary = str(row.get("resumen_curado", "")).strip()
                if curated_title and curated_title.lower() != "nan":
                    info[tid]["title"] = curated_title
                if curated_summary and curated_summary.lower() != "nan":
                    info[tid]["summary"] = curated_summary
                info[tid]["quality"] = str(row.get("calidad_tema", "sin_evaluar"))
                info[tid]["visible_by_default"] = str(row.get("visible_por_defecto", "true")).lower() in {"true", "1", "si", "sí"}
                info[tid]["coherence"] = float(row.get("coherencia_tema_cv", 0) or 0)
                info[tid]["quality_reason"] = str(row.get("motivo_calidad", ""))

    return info


def extract_vis_assets(base: Path) -> tuple[str, str]:
    """Carga vis-network desde PyVis y lo incrusta en el HTML."""
    import pyvis

    lib = Path(pyvis.__file__).resolve().parent / "lib" / "vis-9.1.2"
    css = (lib / "vis-network.css").read_text(encoding="utf-8")
    js = (lib / "vis-network.min.js").read_text(encoding="utf-8")
    return f"<style>{css}</style>", f"<script>{js}</script>"


def norm_text(value: Any) -> str:
    text = "" if pd.isna(value) else str(value)
    return re.sub(r"\s+", " ", text.strip().lower())


def token_set(words: list[str]) -> set[str]:
    return {w.lower() for w in words if isinstance(w, str) and w}


def read_inputs(
    base: Path, data_csv: Path
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    mensajes = pd.read_csv(data_csv)
    cuentas = pd.read_csv(base / "cuentas_clusters" / "cuentas_resumen.csv")
    palx = pd.read_csv(base / "cuentas_clusters" / "palabras_x_cuenta.csv")
    cxt = pd.read_csv(base / "cuentas_clusters" / "cuentas_x_tema.csv")

    lemas_path = base / "clusters" / "documentos_lematizados.csv"
    lemas = pd.read_csv(lemas_path, usecols=["documento_id", "lemas"])
    # La fase LDA conserva aquí solo los documentos validados como español.
    # El inner join evita reincorporar mensajes descartados desde el CSV crudo.
    mensajes = mensajes.merge(
        lemas, left_on="id", right_on="documento_id", how="inner"
    )
    mensajes["usuario"] = (
        mensajes["plataforma"].fillna("").astype(str).str.strip()
        + "::"
        + mensajes["usuario"].fillna("").astype(str).str.strip()
    )
    mensajes["fecha_dt"] = pd.to_datetime(mensajes["fecha"], errors="coerce", utc=True)
    mensajes["texto_norm"] = mensajes["texto_limpio"].map(norm_text)
    mensajes["engagement"] = (
        mensajes.get("likes", 0).fillna(0).astype(float)
        + mensajes.get("comentarios", 0).fillna(0).astype(float)
        + mensajes.get("shares", 0).fillna(0).astype(float)
    )
    return mensajes, cuentas, palx, cxt


def choose_k(n_accounts: int, requested: int) -> int:
    if n_accounts < 30:
        return 1
    if n_accounts < 90:
        return min(2, requested)
    return min(requested, max(2, n_accounts // 120), n_accounts)


def build_topic_matrix(topic_rows: pd.DataFrame, max_vocab: int) -> tuple[list[str], list[dict[str, float]]]:
    vocab = (
        topic_rows[~topic_rows["palabra"].str.lower().isin(NOISE_WORDS)]
        .groupby("palabra")["conteo"]
        .sum()
        .sort_values(ascending=False)
        .head(max_vocab)
        .index.astype(str)
        .tolist()
    )
    vocab_set = set(vocab)
    feats: list[dict[str, float]] = []
    users: list[str] = []
    for user, grp in topic_rows.groupby("usuario"):
        d: dict[str, float] = {}
        for _, row in grp.iterrows():
            palabra = str(row["palabra"])
            if palabra.lower() in NOISE_WORDS:
                continue
            conteo = float(row["conteo"])
            if palabra in vocab_set:
                d[f"w:{palabra}"] = d.get(f"w:{palabra}", 0.0) + conteo
            sub = int(row["sub"]) if not pd.isna(row["sub"]) else -1
            d[f"s:{sub}"] = d.get(f"s:{sub}", 0.0) + conteo * 0.75
        if d:
            users.append(str(user))
            feats.append(d)
    return users, feats


def words_for_position(topic_rows: pd.DataFrame, users: set[str], limit: int) -> list[tuple[str, int]]:
    rows = topic_rows[topic_rows["usuario"].isin(users)]
    if rows.empty:
        return []
    rows = rows[~rows["palabra"].str.lower().isin(NOISE_WORDS)]
    out = rows.groupby("palabra")["conteo"].sum().sort_values(ascending=False)
    if limit > 0:
        out = out.head(limit)
    return [(str(k), int(v)) for k, v in out.items()]


def subs_for_position(topic_rows: pd.DataFrame, users: set[str], limit: int = 6) -> list[tuple[str, int]]:
    rows = topic_rows[topic_rows["usuario"].isin(users)]
    if rows.empty:
        return []
    out = rows.groupby("sub")["conteo"].sum().sort_values(ascending=False).head(limit)
    return [(f"S{int(k)}", int(v)) for k, v in out.items()]


def platform_breakdown(cuentas: pd.DataFrame, users: set[str]) -> dict[str, int]:
    rows = cuentas[cuentas["usuario"].isin(users)]
    counts: Counter[str] = Counter()
    for raw in rows["plataformas"].fillna("").astype(str):
        for p in raw.split("|"):
            p = p.strip()
            if p:
                counts[p] += 1
    return dict(counts.most_common())


def extract_urls(raw: Any) -> list[str]:
    if pd.isna(raw):
        return []
    text = str(raw).strip()
    if not text or text.lower() == "nan":
        return []
    parts = re.split(r"[|,;\s]+", text)
    return [p for p in parts if p.startswith("http")]


def position_message_stats(
    mensajes: pd.DataFrame,
    users: set[str],
    topic_words: set[str],
) -> dict[str, Any]:
    rows = mensajes[mensajes["usuario"].isin(users)]
    rows = rows[
        rows["lemas"].fillna("").astype(str).map(
            lambda value: bool(set(value.split()) & topic_words)
        )
    ]
    total = int(len(rows))
    if total == 0:
        return {
            "n_msgs": 0,
            "max_hour_share": 0.0,
            "duplicate_share": 0.0,
            "top_url_share": 0.0,
            "examples": [],
            "stance": aggregate_stance_texts([]),
        }

    stance = aggregate_stance_texts(rows["texto_limpio"].fillna(""))

    hours = rows["fecha_dt"].dt.floor("h").value_counts(dropna=True)
    max_hour_share = float(hours.iloc[0] / total) if len(hours) else 0.0

    text_counts = rows["texto_norm"].replace("", np.nan).dropna().value_counts()
    duplicate_share = float(1.0 - (len(text_counts) / max(1, int(text_counts.sum()))))

    url_counter: Counter[str] = Counter()
    if "urls_extraidas" in rows.columns:
        for raw in rows["urls_extraidas"]:
            url_counter.update(extract_urls(raw))
    n_urls = sum(url_counter.values())
    top_url_share = float(url_counter.most_common(1)[0][1] / n_urls) if n_urls else 0.0

    examples_df = rows.sort_values(["engagement", "fecha_dt"], ascending=[False, False])
    examples: list[dict[str, Any]] = []
    seen: set[str] = set()
    for _, row in examples_df.iterrows():
        text = str(row.get("texto_limpio") or "").strip()
        key = norm_text(text)
        if not text or key in seen:
            continue
        seen.add(key)
        examples.append({
            "usuario": str(row.get("usuario") or ""),
            "plataforma": str(row.get("plataforma") or ""),
            "fecha": str(row.get("fecha") or ""),
            "texto": text[:420],
            "engagement": int(row.get("engagement") or 0),
        })
        if len(examples) >= 4:
            break

    return {
        "n_msgs": total,
        "max_hour_share": round(max_hour_share, 4),
        "duplicate_share": round(duplicate_share, 4),
        "top_url_share": round(top_url_share, 4),
        "examples": examples,
        "stance": stance,
    }


def hhi(values: list[float]) -> float:
    total = sum(values)
    if total <= 0:
        return 0.0
    shares = [v / total for v in values]
    return float(sum(s * s for s in shares))


def classify_name(words: list[str], topic_title: str = "") -> str:
    return classify_position_name(words, topic_title)


def coordination_score(
    max_hour_share: float,
    duplicate_share: float,
    top_url_share: float,
    mono_share: float,
    top5_account_share: float,
) -> tuple[float, str]:
    temporal = min(1.0, max_hour_share / 0.18)
    repeated = min(1.0, duplicate_share / 0.25)
    url = min(1.0, top_url_share / 0.40)
    mono = min(1.0, mono_share / 0.70)
    concentration = min(1.0, top5_account_share / 0.45)
    score = 100.0 * (
        0.28 * temporal
        + 0.24 * repeated
        + 0.18 * url
        + 0.16 * mono
        + 0.14 * concentration
    )
    if score >= 66:
        level = "alto"
    elif score >= 38:
        level = "medio"
    else:
        level = "bajo"
    return round(score, 1), level


def make_summary(topic_title: str, name: str, n_accounts: int, top_words: list[str], level: str) -> str:
    words = ", ".join(top_words[:6])
    return (
        f"{name}. Grupo de {n_accounts} cuentas dentro de {topic_title}; "
        f"usa principalmente {words}. Senal indiciaria {level}; requiere lectura cualitativa antes de concluir coordinacion."
    )


def compute_positions(
    mensajes: pd.DataFrame,
    cuentas: pd.DataFrame,
    palx: pd.DataFrame,
    cxt: pd.DataFrame,
    topic_info: dict[int, dict[str, Any]],
    positions_per_topic: int,
    min_account_words: int,
    min_topic_words: int,
    min_topic_pct: float,
    max_vocab: int,
    words_per_position: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    cuenta_meta = cuentas.set_index("usuario", drop=False)
    positions: list[dict[str, Any]] = []
    memberships: list[dict[str, Any]] = []
    position_words: list[dict[str, Any]] = []
    examples_rows: list[dict[str, Any]] = []

    palx = palx.copy()
    palx["usuario"] = palx["usuario"].astype(str)
    palx["palabra"] = palx["palabra"].astype(str)
    palx["tema"] = palx["tema"].astype(int)
    palx["conteo"] = palx["conteo"].astype(int)

    topic_cols = [c for c in cxt.columns if re.fullmatch(r"T\d{2}", c)]
    cxt_idx = cxt.set_index("usuario", drop=False)

    for tema in sorted(palx["tema"].dropna().astype(int).unique()):
        topic_rows = palx[palx["tema"] == tema]
        topic_counts = topic_rows.groupby("usuario")["conteo"].sum()

        candidates = []
        for user, topic_count in topic_counts.items():
            if user not in cuenta_meta.index or user not in cxt_idx.index:
                continue
            total_words = int(cuenta_meta.loc[user, "n_palabras"])
            if total_words < min_account_words or topic_count < min_topic_words:
                continue
            topic_pct = 100.0 * float(topic_count) / max(1, total_words)
            if topic_pct < min_topic_pct:
                continue
            candidates.append(user)

        if not candidates:
            continue

        topic_rows = topic_rows[topic_rows["usuario"].isin(candidates)]
        users, feats = build_topic_matrix(topic_rows, max_vocab=max_vocab)
        if not users:
            continue

        k = choose_k(len(users), positions_per_topic)
        if k == 1:
            labels = np.zeros(len(users), dtype=int)
        else:
            vect = DictVectorizer(sparse=True)
            x_counts = vect.fit_transform(feats)
            x_tfidf = TfidfTransformer(norm="l2").fit_transform(x_counts)
            labels = KMeans(n_clusters=k, random_state=42, n_init=20).fit_predict(x_tfidf)

        label_users: dict[int, set[str]] = defaultdict(set)
        for user, label in zip(users, labels):
            label_users[int(label)].add(user)

        sorted_labels = sorted(
            label_users,
            key=lambda lab: topic_rows[topic_rows["usuario"].isin(label_users[lab])]["conteo"].sum(),
            reverse=True,
        )

        for pos_num, label in enumerate(sorted_labels, 1):
            users_set = label_users[label]
            pos_id = f"T{tema:02d}_P{pos_num}"
            pos_rows = topic_rows[topic_rows["usuario"].isin(users_set)]
            word_pairs = words_for_position(topic_rows, users_set, words_per_position)
            top_words = [w for w, _ in word_pairs]
            sub_pairs = subs_for_position(topic_rows, users_set)
            per_user_counts = pos_rows.groupby("usuario")["conteo"].sum().sort_values(ascending=False)
            top5_share = float(per_user_counts.head(5).sum() / max(1, per_user_counts.sum()))
            mono_flags = []
            for user in users_set:
                total = int(cuenta_meta.loc[user, "n_palabras"])
                cnt = int(topic_counts.get(user, 0))
                mono_flags.append((100.0 * cnt / max(1, total)) >= 70)
            mono_share = float(sum(mono_flags) / max(1, len(mono_flags)))

            msg_stats = position_message_stats(
                mensajes,
                users_set,
                set(topic_rows["palabra"].astype(str)),
            )
            score, level = coordination_score(
                msg_stats["max_hour_share"],
                msg_stats["duplicate_share"],
                msg_stats["top_url_share"],
                mono_share,
                top5_share,
            )
            title = topic_info.get(tema, {}).get("title", f"T{tema:02d}")
            name = classify_name([w.lower() for w in top_words], title)
            short_name = compact_display_name(
                name,
                top_words,
                fallback=f"Posición {pos_num}",
            )
            summary = make_summary(title, name, len(users_set), top_words, level)
            platforms = platform_breakdown(cuentas, users_set)

            positions.append({
                "posicion_id": pos_id,
                "tema_id": tema,
                "posicion_num": pos_num,
                "nombre": name,
                "nombre_corto": short_name,
                "resumen": summary,
                "n_cuentas": len(users_set),
                "n_msgs": msg_stats["n_msgs"],
                "n_palabras_tema": int(pos_rows["conteo"].sum()),
                "top_words": ", ".join(top_words),
                "top_subclusters": ", ".join(f"{s}:{v}" for s, v in sub_pairs),
                "plataformas": json.dumps(platforms, ensure_ascii=False),
                "mono_tema_share": round(mono_share, 4),
                "top5_cuentas_share": round(top5_share, 4),
                "hhi_cuentas": round(hhi([float(v) for v in per_user_counts.tolist()]), 4),
                "max_hour_share": msg_stats["max_hour_share"],
                "duplicate_share": msg_stats["duplicate_share"],
                "top_url_share": msg_stats["top_url_share"],
                "ibea_score": score,
                "ibea_nivel": level,
                "postura_actor": msg_stats["stance"]["stance"],
                "postura_actor_etiqueta": msg_stats["stance"]["stance_label"],
                "postura_actor_score": msg_stats["stance"]["stance_score"],
                "postura_actor_apoyo": msg_stats["stance"]["stance_support_hits"],
                "postura_actor_critica": msg_stats["stance"]["stance_critic_hits"],
                "postura_actor_evidencia": msg_stats["stance"]["stance_evidence"],
                "postura_actor_referencias": msg_stats["stance"]["stance_target_hits"],
                "postura_actor_mensajes": msg_stats["stance"]["stance_classified_messages"],
            })

            for rank, (word, count) in enumerate(word_pairs, 1):
                position_words.append({
                    "posicion_id": pos_id,
                    "tema_id": tema,
                    "rank": rank,
                    "palabra": word,
                    "conteo": count,
                })

            for rank, (user, count) in enumerate(per_user_counts.items(), 1):
                row = cuenta_meta.loc[user]
                total = int(row["n_palabras"])
                memberships.append({
                    "usuario": user,
                    "posicion_id": pos_id,
                    "tema_id": tema,
                    "rank_en_posicion": rank,
                    "conteo_tema": int(count),
                    "pct_usuario_en_tema": round(100.0 * int(count) / max(1, total), 1),
                    "n_msgs": int(row["n_msgs"]),
                    "n_palabras": total,
                    "plataformas": str(row.get("plataformas", "")),
                    "tema_dominante": str(row.get("tema_dominante", "")),
                    "sub_dominante": str(row.get("sub_dominante", "")),
                })

            for i, ex in enumerate(msg_stats["examples"], 1):
                examples_rows.append({
                    "posicion_id": pos_id,
                    "tema_id": tema,
                    "rank": i,
                    **ex,
                })

    return (
        pd.DataFrame(positions),
        pd.DataFrame(memberships),
        pd.DataFrame(position_words),
        pd.DataFrame(examples_rows),
    )


def scalar_size(value: float, min_value: float, max_value: float, lo: float, hi: float) -> float:
    if max_value <= min_value:
        return (lo + hi) / 2
    pos = (value - min_value) / (max_value - min_value)
    return round(lo + (pos ** 0.65) * (hi - lo), 2)


def build_network_data(
    positions: pd.DataFrame,
    memberships: pd.DataFrame,
    position_words: pd.DataFrame,
    examples: pd.DataFrame,
    topic_info: dict[int, dict[str, Any]],
    accounts_per_position: int,
    words_per_position: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    meta: dict[str, Any] = {}
    edge_id = 0

    if positions.empty:
        return nodes, edges, meta

    topic_counts = positions.groupby("tema_id")["n_cuentas"].sum()
    min_topic, max_topic = float(topic_counts.min()), float(topic_counts.max())
    max_pos_accounts = float(positions["n_cuentas"].max())

    angle_step = 2 * math.pi / max(1, len(topic_counts))
    topic_coords: dict[int, tuple[float, float]] = {}
    position_coords: dict[str, tuple[float, float]] = {}
    for idx, tema in enumerate(sorted(topic_counts.index.astype(int))):
        info = topic_info.get(tema, {})
        topic_short_name = compact_topic_display_name(
            str(info.get("title", f"T{tema:02d}")),
            [str(word) for word in info.get("words", [])],
            fallback=f"Tema {tema:02d}",
        )
        color = TEMA_COLORS[tema % len(TEMA_COLORS)]
        angle = idx * angle_step
        tx = round(math.cos(angle) * 300, 2)
        ty = round(math.sin(angle) * 250, 2)
        topic_coords[tema] = (tx, ty)
        node_id = f"T{tema:02d}"
        node = {
            "id": node_id,
            "label": stacked_label(topic_short_name),
            "title": info.get("title", f"T{tema:02d}"),
            "short_name": topic_short_name,
            "kind": "tema",
            "tema": tema,
            "topic_quality": info.get("quality", "sin_evaluar"),
            "shape": "dot",
            "color": {"background": color, "border": "#f2f2f2"},
            "font": {
                "size": 64,
                "color": "#ffffff",
                "face": "arial",
                "strokeWidth": 5,
                "strokeColor": "#111111",
            },
            "size": scalar_size(float(topic_counts.loc[tema]), min_topic, max_topic, 34, 58),
            "x": tx,
            "y": ty,
        }
        nodes.append(node)
        meta[node_id] = {
            "kind": "tema",
            "title": info.get("title", f"T{tema:02d}"),
            "short_name": topic_short_name,
            "summary": info.get("summary", ""),
            "words": info.get("words", []),
            "n_cuentas": int(topic_counts.loc[tema]),
            "quality": info.get("quality", "sin_evaluar"),
            "coherence": float(info.get("coherence", 0) or 0),
            "quality_reason": info.get("quality_reason", ""),
        }

    positions_per_theme = positions.groupby("tema_id")["posicion_id"].count().to_dict()
    for _, row in positions.iterrows():
        tema = int(row["tema_id"])
        color = TEMA_COLORS[tema % len(TEMA_COLORS)]
        pos_id = str(row["posicion_id"])
        border = LEVEL_COLORS.get(str(row["ibea_nivel"]), "#999")
        total_pos = max(1, int(positions_per_theme.get(tema, 1)))
        position_index = int(row["posicion_num"]) - 1
        positions_per_ring = 10
        position_ring = position_index // positions_per_ring
        position_slot = position_index % positions_per_ring
        positions_in_ring = min(
            positions_per_ring,
            total_pos - position_ring * positions_per_ring,
        )
        pos_angle = (
            2 * math.pi * position_slot / max(1, positions_in_ring)
        ) - (math.pi / 2)
        tx, ty = topic_coords.get(tema, (0.0, 0.0))
        position_radius = 105 + position_ring * 95
        px = round(tx + math.cos(pos_angle) * position_radius, 2)
        py = round(ty + math.sin(pos_angle) * position_radius, 2)
        position_coords[pos_id] = (px, py)
        nodes.append({
            "id": pos_id,
            "label": stacked_label(row["nombre_corto"]),
            "title": str(row["nombre"]),
            "short_name": str(row["nombre_corto"]),
            "kind": "posicion",
            "tema": tema,
            "topic_quality": topic_info.get(tema, {}).get("quality", "sin_evaluar"),
            "ibea": str(row["ibea_nivel"]),
            "shape": "diamond",
            "color": {"background": color, "border": border, "highlight": {"background": color, "border": "#ffffff"}},
            "borderWidth": 3 if row["ibea_nivel"] == "alto" else 2,
            "font": {
                "size": 42,
                "color": "#ffffff",
                "face": "arial",
                "strokeWidth": 4,
                "strokeColor": "#111111",
            },
            "size": scalar_size(float(row["n_cuentas"]), 1, max_pos_accounts, 22, 46),
            "x": px,
            "y": py,
        })
        edge_id += 1
        edges.append({
            "id": f"e{edge_id}",
            "from": f"T{tema:02d}",
            "to": pos_id,
            "value": max(1, int(row["n_cuentas"])),
            "kind": "tema_posicion",
            "color": {"color": color, "opacity": 0.55},
        })

        ex_rows = examples[examples["posicion_id"] == pos_id] if not examples.empty else pd.DataFrame()
        meta[pos_id] = {
            "kind": "posicion",
            "tema": tema,
            "nombre": str(row["nombre"]),
            "short_name": str(row["nombre_corto"]),
            "resumen": str(row["resumen"]),
            "n_cuentas": int(row["n_cuentas"]),
            "n_msgs": int(row["n_msgs"]),
            "n_palabras_tema": int(row["n_palabras_tema"]),
            "top_words": str(row["top_words"]).split(", ") if str(row["top_words"]) else [],
            "top_subclusters": str(row["top_subclusters"]),
            "ibea_score": float(row["ibea_score"]),
            "ibea_nivel": str(row["ibea_nivel"]),
            "postura_actor": str(row["postura_actor"]),
            "postura_actor_etiqueta": str(row["postura_actor_etiqueta"]),
            "postura_actor_score": float(row["postura_actor_score"]),
            "postura_actor_evidencia": float(row["postura_actor_evidencia"]),
            "postura_actor_mensajes": int(row["postura_actor_mensajes"]),
            "metricas": {
                "mono_tema_share": float(row["mono_tema_share"]),
                "top5_cuentas_share": float(row["top5_cuentas_share"]),
                "max_hour_share": float(row["max_hour_share"]),
                "duplicate_share": float(row["duplicate_share"]),
                "top_url_share": float(row["top_url_share"]),
            },
            "examples": ex_rows.to_dict("records"),
        }

    account_node_ids: dict[str, str] = {}
    account_positions: defaultdict[str, list[str]] = defaultdict(list)
    top_memberships = (
        memberships.sort_values(["posicion_id", "rank_en_posicion"])
        .groupby("posicion_id")
        .head(accounts_per_position)
    )
    max_msgs = max(1, int(top_memberships["n_msgs"].max())) if not top_memberships.empty else 1
    for _, row in top_memberships.iterrows():
        user = str(row["usuario"])
        if user not in account_node_ids:
            node_id = f"A{len(account_node_ids) + 1}"
            account_node_ids[user] = node_id
            platforms = [p for p in str(row["plataformas"]).split("|") if p]
            color = PLATFORM_COLORS.get(platforms[0], "#777777") if platforms else "#777777"
            pos_id = str(row["posicion_id"])
            px, py = position_coords.get(pos_id, (0.0, 0.0))
            rank = int(row["rank_en_posicion"])
            angle = (2 * math.pi * ((rank - 1) % max(1, accounts_per_position)) / max(1, accounts_per_position)) + 0.2
            radius = 58 + 8 * (rank % 3)
            nodes.append({
                "id": node_id,
                "label": "",
                "search": user,
                "kind": "cuenta",
                "tema": int(row["tema_id"]),
                "shape": "dot",
                "color": {"background": color, "border": "#222222"},
                "font": {"size": 0, "color": "#ffffff", "face": "arial"},
                "size": scalar_size(float(row["n_msgs"]), 1, max_msgs, 5, 15),
                "x": round(px + math.cos(angle) * radius, 2),
                "y": round(py + math.sin(angle) * radius, 2),
            })
            meta[node_id] = {
                "kind": "cuenta",
                "usuario": user,
                "plataformas": platforms,
                "n_msgs": int(row["n_msgs"]),
                "n_palabras": int(row["n_palabras"]),
                "tema_dominante": str(row["tema_dominante"]),
                "sub_dominante": str(row["sub_dominante"]),
                "posiciones": [],
            }
        pos_id = str(row["posicion_id"])
        account_positions[user].append(pos_id)
        meta[account_node_ids[user]]["posiciones"].append({
            "posicion_id": pos_id,
            "tema_id": int(row["tema_id"]),
            "conteo_tema": int(row["conteo_tema"]),
            "pct_usuario_en_tema": float(row["pct_usuario_en_tema"]),
        })
        edge_id += 1
        edges.append({
            "id": f"e{edge_id}",
            "from": pos_id,
            "to": account_node_ids[user],
            "value": max(1, int(row["conteo_tema"])),
            "kind": "posicion_cuenta",
            "color": {"color": "#bbbbbb", "opacity": 0.35},
            "dashes": True,
        })

    top_words = position_words.sort_values(["posicion_id", "rank"])
    if words_per_position > 0:
        top_words = top_words.groupby("posicion_id").head(words_per_position)
    words_in_position = top_words.groupby("posicion_id").size().to_dict()
    max_word_count = max(1, int(top_words["conteo"].max())) if not top_words.empty else 1
    # Una palabra puede cumplir funciones distintas en dos posiciones. Se
    # conserva un nodo por pareja posicion-palabra para no fusionar grupos ni
    # reducir artificialmente el numero de palabras visibles.
    word_node_ids: dict[tuple[str, str], str] = {}
    for _, row in top_words.iterrows():
        word = str(row["palabra"])
        pos_id = str(row["posicion_id"])
        tema = int(row["tema_id"])
        word_key = (pos_id, word)
        if word_key not in word_node_ids:
            px, py = position_coords.get(pos_id, (0.0, 0.0))
            rank = int(row["rank"])
            word_index = rank - 1
            words_per_ring = 24
            word_ring = word_index // words_per_ring
            word_slot = word_index % words_per_ring
            words_this_ring = min(
                words_per_ring,
                int(words_in_position.get(pos_id, 1)) - word_ring * words_per_ring,
            )
            angle = (
                2 * math.pi * word_slot / max(1, words_this_ring)
            ) + math.pi
            radius = 110 + word_ring * 28
            node_id = f"W{len(word_node_ids) + 1}"
            word_node_ids[word_key] = node_id
            nodes.append({
                "id": node_id,
                "label": word,
                "kind": "palabra",
                "tema": tema,
                "shape": "dot",
                "color": {"background": "#222222", "border": TEMA_COLORS[tema % len(TEMA_COLORS)]},
                "font": {"size": 16, "color": "#f7f7f7", "face": "arial"},
                "size": scalar_size(float(row["conteo"]), 1, max_word_count, 8, 22),
                "x": round(px + math.cos(angle) * radius, 2),
                "y": round(py + math.sin(angle) * radius, 2),
            })
            meta[node_id] = {
                "kind": "palabra",
                "palabra": word,
                "posiciones": [],
            }
        node_id = word_node_ids[word_key]
        meta[node_id]["posiciones"].append({
            "posicion_id": pos_id,
            "tema_id": tema,
            "conteo": int(row["conteo"]),
            "rank": int(row["rank"]),
        })
        edge_id += 1
        edges.append({
            "id": f"e{edge_id}",
            "from": pos_id,
            "to": node_id,
            "value": max(1, int(row["conteo"])),
            "kind": "posicion_palabra",
            "color": {"color": TEMA_COLORS[int(row["tema_id"]) % len(TEMA_COLORS)], "opacity": 0.40},
        })

    return nodes, edges, meta


def build_html(
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    meta: dict[str, Any],
    topic_info: dict[int, dict[str, Any]],
    base: Path,
    semana: str,
) -> str:
    vis_css, vis_js = extract_vis_assets(base)
    topics = sorted(topic_info)
    position_topics = {
        str(node["id"]): int(node["tema"])
        for node in nodes
        if node.get("kind") == "posicion" and node.get("tema") is not None
    }
    topic_neighbors: defaultdict[int, set[str]] = defaultdict(set)
    neighbor_topics: defaultdict[str, set[int]] = defaultdict(set)
    topic_connections: defaultdict[int, int] = defaultdict(int)
    for edge in edges:
        position_id = None
        neighbor_id = None
        if str(edge.get("from")) in position_topics:
            position_id, neighbor_id = str(edge["from"]), str(edge.get("to"))
        elif str(edge.get("to")) in position_topics:
            position_id, neighbor_id = str(edge["to"]), str(edge.get("from"))
        if position_id is None or neighbor_id is None:
            continue
        topic_id = position_topics[position_id]
        topic_connections[topic_id] += 1
        if edge.get("kind") in {"posicion_cuenta", "posicion_palabra"}:
            topic_neighbors[topic_id].add(neighbor_id)
            neighbor_topics[neighbor_id].add(topic_id)

    topic_metrics = {}
    for tid in topics:
        bridge_score = sum(
            max(0, len(neighbor_topics[neighbor]) - 1)
            for neighbor in topic_neighbors.get(tid, set())
        )
        topic_metrics[tid] = {
            "volume": int(meta.get(f"T{tid:02d}", {}).get("n_cuentas", 0)),
            "centrality": int(bridge_score),
            "connectivity": int(topic_connections.get(tid, 0)),
        }
    topic_cards = []
    for tid in topics:
        info = topic_info[tid]
        words = [str(word).strip() for word in info.get("words", []) if str(word).strip()]
        short_name = compact_topic_display_name(
            str(info.get("title", f"T{tid:02d}")),
            words,
            fallback=f"Tema {tid:02d}",
        )
        ranked_words = "".join(
            f'<span class="topic-word-row"><b>{rank}.</b> {html.escape(word)}</span>'
            for rank, word in enumerate(words[:15], start=1)
        )
        topic_cards.append(
            f'<button class="network-topic-filter" data-network-topic="{tid}" data-topic="{tid}" '
            f'data-volume="{topic_metrics[tid]["volume"]}" '
            f'data-centrality="{topic_metrics[tid]["centrality"]}" '
            f'data-connectivity="{topic_metrics[tid]["connectivity"]}">'
            f'<span data-topic-quality="{html.escape(str(info.get("quality", "sin_evaluar")))}"></span>'
            f'<span class="topic-card-title"><i class="sw" style="background:{TEMA_COLORS[tid % len(TEMA_COLORS)]}"></i>'
            f'<span class="topic-title-text">Tema {tid:02d} · {html.escape(short_name)}</span>'
            f'<span class="topic-disclosure" role="button" tabindex="0" aria-expanded="false" '
            f'aria-label="Mostrar las 15 palabras principales del Tema {tid:02d}">▸</span></span>'
            f'<span class="topic-words" hidden>{ranked_words}</span>'
            f'<small class="topic-score"></small></button>'
        )
    topic_cards_html = "\n".join(topic_cards)
    image_slug = re.sub(r"[^a-z0-9]+", "_", _topic_key(semana)).strip("_")
    image_filename = f"red_{image_slug}_posiciones.png"

    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Red {semana} · posiciones discursivas</title>
{vis_css}
{vis_js}
<style>
  :root {{ color-scheme: dark; }}
  body {{ margin:0; background:#121212; color:#eee; font-family:Arial, sans-serif; overflow:hidden; }}
  #networkStage {{
    position:fixed; inset:0 calc(360px - 1cm) 0 calc(320px - 1cm);
    background:#1d1d1d; overflow:hidden;
  }}
  #network {{ position:absolute; inset:0; background:#1d1d1d; }}
  #networkStage:fullscreen, #networkStage.network-expanded {{
    position:fixed; inset:0; width:100vw; height:100vh; z-index:1000;
  }}
  #networkStage:-webkit-full-screen {{
    position:fixed; inset:0; width:100vw; height:100vh;
  }}
  #left, #right {{
    position:fixed; top:0; bottom:0; overflow:auto; z-index:5;
    background:#151515; border-color:#333; padding:12px; box-sizing:border-box;
  }}
  #left {{ left:0; width:calc(320px - 1cm); border-right:1px solid #333; }}
  #right {{ right:0; width:calc(360px - 1cm); border-left:1px solid #333; }}
  h1 {{ font-size:16px; margin:0 0 8px; }}
  h2 {{ font-size:12px; text-transform:uppercase; letter-spacing:.04em; color:#aaa; margin:14px 0 6px; }}
  label {{ display:block; font-size:12px; margin:5px 0; line-height:1.3; }}
  input[type="text"] {{ width:100%; box-sizing:border-box; padding:7px; background:#101010; color:#fff; border:1px solid #444; border-radius:4px; }}
  select {{ width:100%; box-sizing:border-box; padding:6px; background:#101010; color:#fff; border:1px solid #444; border-radius:4px; }}
  input[type="range"] {{ width:100%; }}
  .range-scale {{ display:flex; justify-content:space-between; margin-top:-3px; color:#999; font-size:10px; }}
  .tool-help {{ margin:5px 0 0; color:#999; font-size:10px; line-height:1.35; }}
  button {{ background:#333; color:#fff; border:1px solid #555; border-radius:4px; padding:5px 8px; cursor:pointer; }}
  .grp {{ border:1px solid #333; border-radius:6px; padding:8px; margin-bottom:10px; background:#191919; }}
  .sw {{ display:inline-block; width:10px; height:10px; border-radius:2px; margin-right:5px; vertical-align:-1px; }}
  .network-topic-filter {{ display:block; width:100%; margin:5px 0; padding:7px; text-align:left; box-sizing:border-box; }}
  .network-topic-filter.guided-active {{ outline:2px solid #f5f5f5; background:#505050; }}
  .topic-card-title {{ display:flex; align-items:flex-start; gap:4px; font-size:15px; font-weight:bold; line-height:1.25; }}
  .topic-title-text {{ flex:1; min-width:0; }}
  .topic-disclosure {{ flex:none; width:18px; height:18px; display:grid; place-items:center; margin:-2px -2px 0 2px; border-radius:3px; color:#ddd; font-size:14px; }}
  .topic-disclosure:hover, .topic-disclosure:focus {{ background:#666; color:#fff; outline:none; }}
  .topic-words {{ display:grid; grid-template-columns:1fr 1fr; gap:2px 8px; margin:5px 0 1px 15px; color:#bbb; font-size:10px; line-height:1.3; font-weight:normal; }}
  .topic-words[hidden] {{ display:none; }}
  .topic-word-row {{ display:block; min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }}
  .topic-word-row b {{ color:#888; font-weight:normal; }}
  .topic-score {{ display:block; margin:3px 0 0 15px; color:#75bfff; font-size:10px; }}
  .quality-note {{ color:#d8b15c; }}
  .pill {{ display:inline-block; padding:2px 6px; margin:2px 3px 2px 0; border-radius:10px; background:#26384a; font-size:11px; }}
  .metric {{ display:grid; grid-template-columns:1fr auto; gap:6px; font-size:12px; border-bottom:1px solid #2b2b2b; padding:4px 0; }}
  .muted {{ color:#aaa; font-size:12px; line-height:1.35; }}
  .example {{ border-left:3px solid #555; padding:6px 8px; margin:8px 0; background:#101010; font-size:12px; line-height:1.35; }}
  .level-bajo {{ color:#cfcfcf; }}
  .level-medio {{ color:#ffcc33; }}
  .level-alto {{ color:#ff6b6b; }}
  #strategicBar {{ position:absolute; left:10px; right:10px; top:10px; z-index:9;
    display:flex; align-items:center; gap:7px; flex-wrap:wrap; padding:7px 9px; box-sizing:border-box;
    background:rgba(17,17,17,.96); border:1px solid #555; border-radius:5px; box-shadow:0 3px 12px #0008; font-size:11px; }}
  #strategicBar .strategic-title {{ color:#fff; font-weight:bold; white-space:nowrap; }}
  #strategicBar .strategic-actions {{ display:flex; gap:5px; }}
  #strategicBar .strategy-button {{ padding:5px 10px; }}
  #strategicBar .strategy-button.strategy-active {{ outline:2px solid #fff; }}
  #strategicBar [data-strategy="risk"].strategy-active {{ background:#8d1717; }}
  #strategicBar [data-strategy="opportunity"].strategy-active {{ background:#8a6410; }}
  #strategicBar [data-strategy="consolidation"].strategy-active {{ background:#176b42; }}
  #strategyHelp {{ flex:1 1 250px; color:#aaa; line-height:1.3; }}
  #stats {{ margin-left:auto; color:#ddd; white-space:nowrap; }}
  #fullscreenNetwork {{
    flex:none; display:inline-flex; align-items:center; gap:6px; padding:5px 9px;
    background:#214d27; border-color:#43b64f; font-weight:bold; white-space:nowrap;
  }}
  #fullscreenNetwork:hover, #fullscreenNetwork:focus {{ background:#2d6b35; outline:2px solid #8bea91; }}
  #fullscreenNetwork .fullscreen-icon {{ font-size:17px; line-height:12px; }}
  #downloadNetworkImage {{
    flex:none; display:inline-flex; align-items:center; gap:6px; padding:5px 9px;
    background:#243f5b; border-color:#4d9bd4; font-weight:bold; white-space:nowrap;
  }}
  #downloadNetworkImage:hover, #downloadNetworkImage:focus {{ background:#315778; outline:2px solid #8ac9f2; }}
  .layout-actions {{ display:grid; grid-template-columns:1fr; gap:5px; }}
  .layout-actions button {{ width:100%; }}
  .layout-actions button.layout-active {{ outline:2px solid #fff; background:#4c4c4c; }}
  #polarityGuide {{
    position:absolute; left:18px; right:18px;
    top:58px; z-index:8; display:grid; grid-template-columns:1fr 1fr 1fr;
    align-items:center; pointer-events:none; color:#fff; font-size:11px;
    font-weight:bold; text-transform:uppercase; letter-spacing:.06em;
    text-shadow:0 1px 3px #000, 0 0 5px #000;
  }}
  #polarityGuide[hidden] {{ display:none; }}
  #polarityGuide span:nth-child(1) {{ color:#63d36f; text-align:left; }}
  #polarityGuide span:nth-child(2) {{ color:#f2c744; text-align:center; }}
  #polarityGuide span:nth-child(3) {{ color:#ff6666; text-align:right; }}
  #governmentGuide {{
    position:absolute; left:18px; right:18px;
    top:58px; z-index:8; pointer-events:none; color:#fff; text-align:center;
    font-size:11px; font-weight:bold; text-transform:uppercase; letter-spacing:.06em;
    text-shadow:0 1px 3px #000, 0 0 5px #000;
  }}
  #governmentGuide[hidden] {{ display:none; }}
  #governmentGuide .mayor-label {{ color:#ff6666; }}
  #governmentGuide .government-label {{ color:#b99adf; }}
  #right #guidedPanel .guided-target {{
    font-size:16px !important; font-weight:600; padding:7px 8px !important;
  }}
  #right #guidedPanel h4 {{ font-size:12px !important; }}
</style>
</head>
<body>
<aside id="left">
  <h1>Red {semana}: posiciones discursivas</h1>
  <div class="muted">Temas, posiciones, cuentas y palabras. El borde de cada posicion indica senal indiciaria: gris baja, amarillo media, rojo alta.</div>
  <div class="tool-help"><b>Selección múltiple:</b> mantén Ctrl y haz clic en cada nodo que quieras agregar o quitar del resaltado. Al arrastrar uno o varios temas seleccionados, se moverán con ellos sus posiciones, cuentas y palabras.</div>
  <div class="grp">
    <h2>Buscar</h2>
    <input id="search" type="text" placeholder="cuenta, palabra, posicion o tema...">
    <div class="tool-help">Escribe una palabra o cuenta y pulsa Enter para encontrarla y acercar la red a ese punto.</div>
  </div>
  <div class="grp">
    <h2>Capas</h2>
    <label><input type="checkbox" id="showTema" checked> temas</label>
    <label><input type="checkbox" id="showPos" checked> posiciones</label>
    <label><input type="checkbox" id="showCuenta" checked> cuentas</label>
    <label><input type="checkbox" id="showPalabra" checked> palabras distintivas</label>
    <div class="tool-help">Activa o desactiva tipos de nodos. Por ejemplo, deja solo temas y posiciones para obtener una vista más sencilla.</div>
  </div>
  <div class="grp">
    <h2>Senal indiciaria</h2>
    <label><input type="checkbox" class="level" data-level="bajo" checked> baja</label>
    <label><input type="checkbox" class="level" data-level="medio" checked> media</label>
    <label><input type="checkbox" class="level" data-level="alto" checked> alta</label>
    <div class="tool-help">Filtra posiciones por la intensidad de señales atípicas: baja es más ordinaria; alta requiere una revisión más cuidadosa.</div>
  </div>
  <div class="grp">
    <h2>Visual</h2>
    <label>Separacion <span id="springV">0</span></label>
    <input id="spring" type="range" min="0" max="100" value="0" step="1">
    <div class="range-scale"><span>0</span><span>100</span></div>
    <label>Tamaño de nombres <span id="labelScaleV">100%</span></label>
    <input id="labelScale" type="range" min="10" max="200" value="100" step="5">
    <div class="range-scale"><span>10%</span><span>200%</span></div>
    <button id="reset">Reorganizar</button>
    <button id="stopPhysics">Detener</button>
    <button id="fitNetwork">Encajar</button>
    <button id="separateLabels">Separar etiquetas</button>
    <div class="tool-help">Separación aleja los grupos sin cambiar sus relaciones. Reorganizar activa el movimiento, Detener lo congela, Encajar centra toda la red y Separar etiquetas redistribuye los nombres de temas y posiciones para evitar empalmes.</div>
  </div>
  <div class="grp">
    <h2>Acomodos</h2>
    <div class="layout-actions">
      <button id="polarityLayout">Positivos · mixtos · negativos</button>
      <button id="governmentLayout">Alcaldesa y gobierno al centro</button>
    </div>
    <div class="tool-help">Polaridad separa positivos y negativos, con los mixtos según su balance. Alcaldesa/gobierno crea un núcleo central y distribuye alrededor los temas con sus posiciones, cuentas y palabras.</div>
  </div>
  <div class="grp">
    <h2>Temas de la red</h2>
    <div class="muted">Se muestran los temas más confiables según el orden elegido. Los agrupamientos débiles se ocultan inicialmente para reducir ruido, pero nunca se eliminan.</div>
    <label><input type="checkbox" id="showLowQuality"> mostrar temas de baja calidad</label>
    <div class="tool-help">Actívalo para auditar los temas que mezclan asuntos o formatos editoriales y que no conviene usar directamente en conclusiones.</div>
    <label>Ordenar por</label>
    <select id="topicOrder">
      <option value="volume">Volumen (cuentas)</option>
      <option value="centrality">Centralidad (puentes)</option>
      <option value="connectivity">Conectividad (enlaces)</option>
      <option value="topic">Numero de tema</option>
    </select>
    <div class="tool-help">Volumen prioriza los temas con más cuentas; centralidad destaca los que sirven de puente; conectividad ordena por cantidad de enlaces.</div>
    <div id="topicList">{topic_cards_html}</div>
  </div>
</aside>
<section id="networkStage">
<main id="network"></main>
<div id="strategicBar">
  <span class="strategic-title">Lectura estratégica</span>
  <div class="strategic-actions">
    <button class="strategy-button" data-strategy="risk">Riesgo</button>
    <button class="strategy-button" data-strategy="opportunity">Oportunidad</button>
    <button class="strategy-button" data-strategy="consolidation">Consolidación</button>
  </div>
  <span id="strategyHelp">Elige un ámbito. Los nodos con color cumplen el criterio; los grises muestran sus conexiones inmediatas como contexto.</span>
  <span id="stats"></span>
  <button id="downloadNetworkImage" type="button" aria-label="Guardar la vista actual de la red como imagen PNG" title="Guardar la pantalla central como imagen PNG">
    <span aria-hidden="true">▣</span>
    <span>Guardar imagen</span>
  </button>
  <button id="fullscreenNetwork" type="button" aria-label="Expandir solamente la red a pantalla completa" aria-pressed="false" title="Expandir solamente la red">
    <span class="fullscreen-icon" aria-hidden="true">⛶</span>
    <span class="fullscreen-label">Pantalla completa</span>
  </button>
</div>
<div id="polarityGuide" hidden>
  <span>Positivos</span>
  <span>Mixtos</span>
  <span>Negativos</span>
</div>
<div id="governmentGuide" hidden>
  Núcleo: <span class="mayor-label">Alcaldesa</span> y
  <span class="government-label">Gobierno municipal</span> · temas alrededor
</div>
</section>
<aside id="right">
  <h1>Lectura</h1>
  <div id="detail" class="muted">Haz clic en cualquier elemento de la red. Aquí aparecerá una explicación de qué representa, sus cifras y las palabras o ejemplos que ayudan a interpretarlo.</div>
</aside>
<script>
const RAW_NODES = {json.dumps(nodes, ensure_ascii=False)};
const RAW_EDGES = {json.dumps(edges, ensure_ascii=False)};
const META = {json.dumps(meta, ensure_ascii=False)};
const nodes = new vis.DataSet(RAW_NODES);
const edges = new vis.DataSet(RAW_EDGES);
const network = new vis.Network(document.getElementById('network'), {{nodes, edges}}, {{
  nodes: {{ borderWidth: 1, borderWidthSelected: 5, shadow: false }},
  edges: {{ smooth: {{type:'continuous'}}, font: {{size:0}} }},
  physics: {{
    enabled: false,
    solver: 'forceAtlas2Based',
    forceAtlas2Based: {{ gravitationalConstant: -95, centralGravity: 0.012, springLength: 170, springConstant: 0.06, damping: 0.45 }},
    stabilization: {{ enabled: false }}
  }},
  interaction: {{
    hover:true,
    tooltipDelay:100,
    navigationButtons:true,
    keyboard:true,
    multiselect:true
  }}
}});
window.network = network;

const NODE_KIND_BY_ID = new Map(
  RAW_NODES.map(node => [String(node.id), String(node.kind || '')])
);
const THEME_POSITIONS = new Map();
const POSITION_FOLLOWERS = new Map();
RAW_EDGES.forEach(edge => {{
  const from = String(edge.from);
  const to = String(edge.to);
  if (edge.kind === 'tema_posicion') {{
    const themeId = NODE_KIND_BY_ID.get(from) === 'tema' ? from : to;
    const positionId = themeId === from ? to : from;
    if (!THEME_POSITIONS.has(themeId)) THEME_POSITIONS.set(themeId, new Set());
    THEME_POSITIONS.get(themeId).add(positionId);
  }}
  if (edge.kind === 'posicion_cuenta' || edge.kind === 'posicion_palabra') {{
    const positionId = NODE_KIND_BY_ID.get(from) === 'posicion' ? from : to;
    const followerId = positionId === from ? to : from;
    if (!POSITION_FOLLOWERS.has(positionId)) POSITION_FOLLOWERS.set(positionId, new Set());
    POSITION_FOLLOWERS.get(positionId).add(followerId);
  }}
}});
function collectThemeCluster(themeIds) {{
  const clusterIds = new Set(themeIds);
  themeIds.forEach(themeId => {{
    const positionIds = THEME_POSITIONS.get(themeId) || new Set();
    positionIds.forEach(positionId => {{
      clusterIds.add(positionId);
      (POSITION_FOLLOWERS.get(positionId) || new Set()).forEach(
        followerId => clusterIds.add(followerId)
      );
    }});
  }});
  return clusterIds;
}}
let activeThemeDrag = null;
function moveThemeFollowers() {{
  if (!activeThemeDrag) return;
  const currentAnchor = network.getPosition(activeThemeDrag.anchorId);
  const dx = currentAnchor.x - activeThemeDrag.anchorStart.x;
  const dy = currentAnchor.y - activeThemeDrag.anchorStart.y;
  activeThemeDrag.followerIds.forEach(nodeId => {{
    const start = activeThemeDrag.startPositions[nodeId];
    if (start) network.moveNode(nodeId, start.x + dx, start.y + dy);
  }});
}}
network.on('dragStart', params => {{
  const pointerNode = network.getNodeAt(params.pointer.DOM);
  const anchorId = pointerNode == null ? '' : String(pointerNode);
  if (NODE_KIND_BY_ID.get(anchorId) !== 'tema') {{
    activeThemeDrag = null;
    return;
  }}
  const selectedThemeIds = network.getSelectedNodes()
    .map(String)
    .filter(nodeId => NODE_KIND_BY_ID.get(nodeId) === 'tema');
  if (!selectedThemeIds.includes(anchorId)) selectedThemeIds.push(anchorId);
  const clusterIds = collectThemeCluster(selectedThemeIds);
  const startPositions = network.getPositions(Array.from(clusterIds));
  activeThemeDrag = {{
    anchorId,
    anchorStart: startPositions[anchorId],
    followerIds: Array.from(clusterIds).filter(
      nodeId => !selectedThemeIds.includes(nodeId)
    ),
    startPositions
  }};
  network.stopSimulation();
  network.setOptions({{physics: {{enabled:false}}}});
}});
network.on('dragging', moveThemeFollowers);
network.on('dragEnd', () => {{
  moveThemeFollowers();
  activeThemeDrag = null;
}});

const networkStage = document.getElementById('networkStage');
const fullscreenNetwork = document.getElementById('fullscreenNetwork');
const downloadNetworkImage = document.getElementById('downloadNetworkImage');
function saveNetworkImage() {{
  network.redraw();
  window.requestAnimationFrame(() => {{
    const sourceCanvas = document.querySelector('#network canvas');
    if (!sourceCanvas) return;
    const outputCanvas = document.createElement('canvas');
    outputCanvas.width = sourceCanvas.width;
    outputCanvas.height = sourceCanvas.height;
    const context = outputCanvas.getContext('2d');
    context.fillStyle = '#1d1d1d';
    context.fillRect(0, 0, outputCanvas.width, outputCanvas.height);
    context.drawImage(sourceCanvas, 0, 0);
    const link = document.createElement('a');
    link.download = {json.dumps(image_filename, ensure_ascii=False)};
    link.href = outputCanvas.toDataURL('image/png');
    document.body.appendChild(link);
    link.click();
    link.remove();
  }});
}}
downloadNetworkImage.addEventListener('click', saveNetworkImage);
function isNetworkFullscreen() {{
  return document.fullscreenElement === networkStage ||
    document.webkitFullscreenElement === networkStage ||
    networkStage.classList.contains('network-expanded');
}}
function refreshNetworkViewport() {{
  window.setTimeout(() => {{
    network.setSize('100%', '100%');
    network.redraw();
    const visibleIds = nodes.get()
      .filter(node => !node.hidden)
      .map(node => node.id);
    network.fit({{
      nodes: visibleIds,
      animation: {{duration:350, easingFunction:'easeInOutQuad'}}
    }});
  }}, 80);
}}
function updateFullscreenButton() {{
  const active = isNetworkFullscreen();
  fullscreenNetwork.setAttribute('aria-pressed', String(active));
  fullscreenNetwork.setAttribute(
    'aria-label',
    active ? 'Salir de pantalla completa' : 'Expandir solamente la red a pantalla completa'
  );
  fullscreenNetwork.title = active ? 'Salir de pantalla completa (Esc)' : 'Expandir solamente la red';
  fullscreenNetwork.querySelector('.fullscreen-icon').textContent = active ? '⛶' : '⛶';
  fullscreenNetwork.querySelector('.fullscreen-label').textContent =
    active ? 'Salir de pantalla completa' : 'Pantalla completa';
  refreshNetworkViewport();
}}
async function toggleNetworkFullscreen() {{
  if (document.fullscreenElement === networkStage || document.webkitFullscreenElement === networkStage) {{
    const exitFullscreen = document.exitFullscreen || document.webkitExitFullscreen;
    if (exitFullscreen) await exitFullscreen.call(document);
    return;
  }}
  if (networkStage.classList.contains('network-expanded')) {{
    networkStage.classList.remove('network-expanded');
    document.body.classList.remove('network-fallback-active');
    updateFullscreenButton();
    return;
  }}
  const requestFullscreen = networkStage.requestFullscreen || networkStage.webkitRequestFullscreen;
  if (requestFullscreen) {{
    try {{
      await requestFullscreen.call(networkStage);
      return;
    }} catch (error) {{
      console.warn('El navegador no permitió pantalla completa nativa; se usa el modo expandido.', error);
    }}
  }}
  networkStage.classList.add('network-expanded');
  document.body.classList.add('network-fallback-active');
  updateFullscreenButton();
}}
fullscreenNetwork.addEventListener('click', toggleNetworkFullscreen);
document.addEventListener('fullscreenchange', updateFullscreenButton);
document.addEventListener('webkitfullscreenchange', updateFullscreenButton);
document.addEventListener('keydown', event => {{
  if (event.key === 'Escape' && networkStage.classList.contains('network-expanded')) {{
    networkStage.classList.remove('network-expanded');
    document.body.classList.remove('network-fallback-active');
    updateFullscreenButton();
  }}
}});

const STRATEGIC_PRESETS = {{
  risk: {{label:'Riesgo', strategic:['risk'], combine:'all', help:'Clasificación exclusiva: temas con polaridad negativa.'}},
  opportunity: {{label:'Oportunidad', strategic:['opportunity'], combine:'all', help:'Clasificación exclusiva: temas con polaridad mixta o neutral.'}},
  consolidation: {{label:'Consolidación', strategic:['consolidation'], combine:'all', help:'Clasificación exclusiva: temas con polaridad positiva.'}}
}};
function clearStrategicBar() {{
  document.querySelectorAll('.strategy-button').forEach(button => button.classList.remove('strategy-active'));
  document.getElementById('strategyHelp').textContent = 'Elige un ámbito. Los nodos con color cumplen el criterio; los grises muestran sus conexiones inmediatas como contexto.';
}}
document.querySelectorAll('.strategy-button').forEach(button => {{
  button.addEventListener('click', () => {{
    clearStrategicBar();
    button.classList.add('strategy-active');
    const preset = STRATEGIC_PRESETS[button.dataset.strategy];
    document.getElementById('strategyHelp').textContent = preset.help + ' Color: coincidencia; gris: conexión inmediata de contexto.';
    if (typeof window.guidedApplyStrategicPreset === 'function') {{
      window.guidedApplyStrategicPreset(preset);
    }}
  }});
}});
window.addEventListener('guided-strategy-cleared', clearStrategicBar);

function sortTopicMenu() {{
  const mode = document.getElementById('topicOrder').value;
  const list = document.getElementById('topicList');
  const labels = Array.from(list.querySelectorAll('.network-topic-filter'));
  labels.sort((a, b) => {{
    if (mode === 'topic') return Number(a.dataset.topic) - Number(b.dataset.topic);
    return Number(b.dataset[mode] || 0) - Number(a.dataset[mode] || 0) ||
      Number(a.dataset.topic) - Number(b.dataset.topic);
  }});
  labels.forEach((label, index) => {{
    const score = label.querySelector('.topic-score');
    const metricNames = {{volume:'cuentas', centrality:'puentes', connectivity:'enlaces'}};
    score.textContent = mode === 'topic' ? '' : `${{label.dataset[mode] || 0}} ${{metricNames[mode]}}`;
    const lowQuality = label.querySelector('[data-topic-quality]')?.dataset.topicQuality === 'baja';
    const showLowQuality = document.getElementById('showLowQuality').checked;
    label.hidden = index >= 50 || (lowQuality && !showLowQuality);
    list.appendChild(label);
  }});
}}
document.getElementById('topicOrder').addEventListener('change', sortTopicMenu);
document.getElementById('showLowQuality').addEventListener('change', sortTopicMenu);
sortTopicMenu();
document.querySelectorAll('.topic-disclosure').forEach(disclosure => {{
  function toggleExplanation(event) {{
    event.preventDefault();
    event.stopPropagation();
    const words = disclosure.closest('.network-topic-filter').querySelector('.topic-words');
    const expanding = words.hidden;
    words.hidden = !expanding;
    disclosure.textContent = expanding ? '▾' : '▸';
    disclosure.setAttribute('aria-expanded', String(expanding));
  }}
  disclosure.addEventListener('click', toggleExplanation);
  disclosure.addEventListener('keydown', event => {{
    if (event.key === 'Enter' || event.key === ' ') toggleExplanation(event);
  }});
}});

function activeLevels() {{
  const s = {{}};
  document.querySelectorAll('.level:checked').forEach(el => s[el.dataset.level] = true);
  return s;
}}
function layerFlags() {{
  return {{
    tema: document.getElementById('showTema').checked,
    posicion: document.getElementById('showPos').checked,
    cuenta: document.getElementById('showCuenta').checked,
    palabra: document.getElementById('showPalabra').checked
  }};
}}
function nodeVisible(n, levels, flags) {{
  if (!flags[n.kind]) return false;
  if ((n.kind === 'tema' || n.kind === 'posicion') && n.topic_quality === 'baja' && !document.getElementById('showLowQuality').checked) return false;
  if (n.kind === 'posicion' && !levels[n.ibea]) return false;
  if (typeof window.guidedNodeAllowed === 'function' && !window.guidedNodeAllowed(n)) return false;
  return true;
}}
function rebuild() {{
  const levels = activeLevels(), flags = layerFlags();
  const visible = {{}};
  const finalVisible = {{}};
  const connected = {{}};
  let counts = {{tema:0, posicion:0, cuenta:0, palabra:0, edges:0}};
  const nodeUpdates = [];
  const edgeUpdates = [];
  nodes.forEach(n => {{
    visible[n.id] = nodeVisible(n, levels, flags);
  }});
  edges.forEach(e => {{
    const ef = nodes.get(e.from), et = nodes.get(e.to);
    if (visible[e.from] && visible[e.to] && ef && ef.kind === 'posicion' && et && (et.kind === 'cuenta' || et.kind === 'palabra')) {{
      connected[e.to] = true;
    }}
  }});
  nodes.forEach(n => {{
    let v = visible[n.id];
    if (n.kind === 'cuenta' || n.kind === 'palabra') v = v && !!connected[n.id];
    finalVisible[n.id] = v;
    if (v) counts[n.kind] = (counts[n.kind] || 0) + 1;
    nodeUpdates.push({{id:n.id, hidden:!v}});
  }});
  edges.forEach(e => {{
    const v = !!finalVisible[e.from] && !!finalVisible[e.to] &&
      (typeof window.guidedEdgeAllowed !== 'function' || window.guidedEdgeAllowed(e));
    if (v) counts.edges++;
    edgeUpdates.push({{id:e.id, hidden:!v}});
  }});
  nodes.update(nodeUpdates);
  edges.update(edgeUpdates);
  document.getElementById('stats').innerHTML =
    `<b>temas</b>: ${{counts.tema}} | <b>posiciones</b>: ${{counts.posicion}} | <b>cuentas</b>: ${{counts.cuenta}} | <b>palabras</b>: ${{counts.palabra}} | <b>conexiones</b>: ${{counts.edges}}`;
}}
function esc(s) {{
  return String(s ?? '').replace(/[&<>"']/g, c => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[c]));
}}
function pct(v) {{ return Math.round((Number(v)||0) * 1000) / 10 + '%'; }}
function render(id) {{
  const m = META[id] || {{}};
  let h = '';
  if (m.kind === 'tema') {{
    h += `<h1>${{esc(id)}} · ${{esc(m.title)}}</h1><p>${{esc(m.summary)}}</p>`;
    h += `<div class="metric"><span>polaridad exclusiva</span><b>${{esc(m.polaridad)}}</b></div>`;
    h += `<div class="metric"><span>ámbito estratégico</span><b>${{esc(m.ambito_estrategico)}}</b></div>`;
    h += `<div class="metric"><span>calidad temática</span><b>${{esc(m.quality)}}</b></div>`;
    if (m.quality === 'baja') h += `<div class="muted quality-note">Se conserva para auditoría, pero puede mezclar conversaciones. Motivo: ${{esc(m.quality_reason)}}.</div>`;
    h += `<div class="metric"><span>cuentas mapeadas</span><b>${{m.n_cuentas}}</b></div>`;
    h += '<h2>Palabras guia</h2>' + (m.words || []).slice(0, 40).map(w => `<span class="pill">${{esc(w)}}</span>`).join('');
  }} else if (m.kind === 'posicion') {{
    h += `<h1>${{esc(id)}} · ${{esc(m.nombre)}}</h1>`;
    h += `<p>${{esc(m.resumen)}}</p>`;
    h += `<div class="metric"><span>polaridad exclusiva</span><b>${{esc(m.polaridad)}}</b></div>`;
    h += `<div class="metric"><span>ámbito estratégico</span><b>${{esc(m.ambito_estrategico)}}</b></div>`;
    h += `<div class="metric"><span>postura hacia Mónica/gobierno</span><b>${{esc(m.postura_actor_etiqueta)}}</b></div>`;
    h += `<div class="muted">Se calcula con referencias explícitas al actor y señales de respaldo o crítica; es independiente del tono emocional.</div>`;
    h += `<div class="metric"><span>mensajes con evidencia de postura</span><b>${{m.postura_actor_mensajes}}</b></div>`;
    h += `<div class="metric"><span>senal indiciaria</span><b class="level-${{esc(m.ibea_nivel)}}">${{esc(m.ibea_nivel)}} (${{m.ibea_score}})</b></div>`;
    h += `<div class="metric"><span>cuentas</span><b>${{m.n_cuentas}}</b></div>`;
    h += `<div class="metric"><span>mensajes de esas cuentas</span><b>${{m.n_msgs}}</b></div>`;
    h += `<div class="metric"><span>palabras del tema</span><b>${{m.n_palabras_tema}}</b></div>`;
    h += `<div class="metric"><span>monotematicas</span><b>${{pct(m.metricas.mono_tema_share)}}</b></div>`;
    h += `<div class="metric"><span>hora mas concentrada</span><b>${{pct(m.metricas.max_hour_share)}}</b></div>`;
    h += `<div class="metric"><span>textos repetidos</span><b>${{pct(m.metricas.duplicate_share)}}</b></div>`;
    h += '<h2>Palabras distintivas</h2>' + (m.top_words || []).map(w => `<span class="pill">${{esc(w)}}</span>`).join('');
    h += `<h2>Subclusters</h2><div class="muted">${{esc(m.top_subclusters)}}</div>`;
    if (m.examples && m.examples.length) {{
      h += '<h2>Ejemplos</h2>';
      m.examples.forEach(ex => {{
        h += `<div class="example"><b>${{esc(ex.usuario)}}</b> · ${{esc(ex.plataforma)}} · ${{esc(ex.fecha)}}<br>${{esc(ex.texto)}}</div>`;
      }});
    }}
  }} else if (m.kind === 'cuenta') {{
    h += `<h1>${{esc(m.usuario)}}</h1>`;
    h += `<div class="metric"><span>plataformas</span><b>${{esc((m.plataformas||[]).join(', '))}}</b></div>`;
    h += `<div class="metric"><span>mensajes</span><b>${{m.n_msgs}}</b></div>`;
    h += `<div class="metric"><span>palabras clasificadas</span><b>${{m.n_palabras}}</b></div>`;
    h += `<div class="metric"><span>tema dominante</span><b>${{esc(m.tema_dominante)}}</b></div>`;
    h += `<div class="metric"><span>sub dominante</span><b>${{esc(m.sub_dominante)}}</b></div>`;
    h += '<h2>Posiciones donde aparece</h2>';
    (m.posiciones || []).forEach(p => h += `<div class="metric"><span>${{esc(p.posicion_id)}}</span><b>${{p.pct_usuario_en_tema}}%</b></div>`);
  }} else if (m.kind === 'palabra') {{
    h += `<h1>${{esc(m.palabra)}}</h1><h2>Posiciones donde aparece</h2>`;
    (m.posiciones || []).forEach(p => h += `<div class="metric"><span>${{esc(p.posicion_id)}}</span><b>${{p.conteo}}</b></div>`);
  }}
  document.getElementById('detail').innerHTML = h || 'Sin detalle.';
}}
document.querySelectorAll('input').forEach(el => {{
  if (!['search', 'spring', 'labelScale'].includes(el.id)) el.addEventListener('change', rebuild);
}});
document.getElementById('spring').addEventListener('input', e => {{
  const next = parseInt(e.target.value, 10);
  document.getElementById('springV').textContent = next;
  applySeparation(next);
}});
const BASE_LABEL_FONT_SIZES = Object.fromEntries(
  RAW_NODES
    .filter(node => node.kind === 'tema' || node.kind === 'posicion')
    .map(node => [String(node.id), Number(node.font?.size || (node.kind === 'tema' ? 64 : 42))])
);
function applyLabelScale(value) {{
  const percent = Math.max(10, Math.min(200, Number(value) || 100));
  const scale = percent / 100;
  document.getElementById('labelScaleV').textContent = `${{percent}}%`;
  nodes.update(
    nodes.get()
      .filter(node => Object.hasOwn(BASE_LABEL_FONT_SIZES, String(node.id)))
      .map(node => ({{
        id:node.id,
        font:{{
          ...(node.font || {{}}),
          size:Math.round(BASE_LABEL_FONT_SIZES[String(node.id)] * scale)
        }}
      }}))
  );
}}
document.getElementById('labelScale').addEventListener('input', event =>
  applyLabelScale(event.target.value));
document.getElementById('reset').addEventListener('click', () => {{
  clearActiveLayout();
  network.setOptions({{physics: {{enabled:true, stabilization: {{enabled:false}}}}}});
  network.startSimulation();
}});
document.getElementById('stopPhysics').addEventListener('click', () => {{
  network.stopSimulation();
  network.setOptions({{physics: {{enabled:false}}}});
}});
document.getElementById('fitNetwork').addEventListener('click', () => {{
  network.fit({{animation:{{duration:400}}}});
}});
function separateVisibleLabels() {{
  clearActiveLayout();
  network.stopSimulation();
  network.setOptions({{physics: {{enabled:false}}}});
  const button = document.getElementById('separateLabels');
  button.disabled = true;
  button.textContent = 'Separando…';
  button.classList.remove('layout-active');

  window.setTimeout(() => {{
    const labeled = nodes.get()
      .filter(node =>
        !node.hidden &&
        (node.kind === 'tema' || node.kind === 'posicion') &&
        String(node.label || '').trim() &&
        Number(node.font?.size || 0) > 0
      )
      .sort((a, b) => {{
        const priority = {{tema:2, posicion:1}};
        return (priority[b.kind] || 0) - (priority[a.kind] || 0) ||
          String(a.id).localeCompare(String(b.id));
      }});
    const priority = {{tema:2, posicion:1}};
    const padding = 10;
    let totalMoves = 0;

    for (let pass = 0; pass < 18; pass += 1) {{
      let passMoves = 0;
      const boxes = new Map(
        labeled.map(node => [String(node.id), network.getBoundingBox(node.id)])
      );
      for (let leftIndex = 0; leftIndex < labeled.length; leftIndex += 1) {{
        const leftNode = labeled[leftIndex];
        const leftBox = boxes.get(String(leftNode.id));
        if (!leftBox) continue;
        for (let rightIndex = leftIndex + 1; rightIndex < labeled.length; rightIndex += 1) {{
          const rightNode = labeled[rightIndex];
          const rightBox = boxes.get(String(rightNode.id));
          if (!rightBox) continue;
          const overlapX = Math.min(leftBox.right, rightBox.right) -
            Math.max(leftBox.left, rightBox.left) + padding;
          const overlapY = Math.min(leftBox.bottom, rightBox.bottom) -
            Math.max(leftBox.top, rightBox.top) + padding;
          if (overlapX <= 0 || overlapY <= 0) continue;

          const leftPosition = network.getPosition(leftNode.id);
          const rightPosition = network.getPosition(rightNode.id);
          const moveHorizontally = overlapX <= overlapY;
          let direction = moveHorizontally
            ? Math.sign(rightPosition.x - leftPosition.x)
            : Math.sign(rightPosition.y - leftPosition.y);
          if (!direction) {{
            direction = String(leftNode.id).localeCompare(String(rightNode.id)) <= 0 ? 1 : -1;
          }}
          const displacement = Math.min(
            90,
            (moveHorizontally ? overlapX : overlapY) + padding
          );
          const leftPriority = priority[leftNode.kind] || 0;
          const rightPriority = priority[rightNode.kind] || 0;
          const leftShare = leftPriority > rightPriority ? 0 : (leftPriority < rightPriority ? 1 : 0.5);
          const rightShare = rightPriority > leftPriority ? 0 : (rightPriority < leftPriority ? 1 : 0.5);

          network.moveNode(
            leftNode.id,
            leftPosition.x - (moveHorizontally ? direction * displacement * leftShare : 0),
            leftPosition.y - (moveHorizontally ? 0 : direction * displacement * leftShare)
          );
          network.moveNode(
            rightNode.id,
            rightPosition.x + (moveHorizontally ? direction * displacement * rightShare : 0),
            rightPosition.y + (moveHorizontally ? 0 : direction * displacement * rightShare)
          );
          passMoves += 1;
        }}
      }}
      totalMoves += passMoves;
      if (!passMoves) break;
    }}

    network.redraw();
    network.fit({{animation:{{duration:500, easingFunction:'easeInOutQuad'}}}});
    button.disabled = false;
    button.textContent = totalMoves ? 'Etiquetas separadas' : 'Sin empalmes';
    button.classList.add('layout-active');
  }}, 30);
}}
document.getElementById('separateLabels').addEventListener('click', separateVisibleLabels);
document.getElementById('polarityLayout').addEventListener('click', applyPolarityLayout);
document.getElementById('governmentLayout').addEventListener('click', applyGovernmentLayout);
document.getElementById('search').addEventListener('keydown', e => {{
  if (e.key !== 'Enter') return;
  const q = e.target.value.trim().toLowerCase();
  if (!q) return;
  let found = null;
  nodes.forEach(n => {{
    if (!found && !n.hidden && String(n.search || n.label || n.id).toLowerCase().includes(q)) found = n.id;
  }});
  if (found) {{
    network.selectNodes([found]);
    network.focus(found, {{scale:1.2, animation:true}});
    render(found);
  }}
}});
network.on('click', p => {{
  if (p.nodes.length) render(p.nodes[0]);
}});
rebuild();
const BASE_POSITIONS = network.getPositions();
const BASE_IDS = Object.keys(BASE_POSITIONS);
const BASE_CENTER = BASE_IDS.reduce((center, id) => ({{x:center.x + BASE_POSITIONS[id].x / BASE_IDS.length, y:center.y + BASE_POSITIONS[id].y / BASE_IDS.length}}), {{x:0, y:0}});
function clearActiveLayout() {{
  document.querySelectorAll('.layout-actions button').forEach(button => button.classList.remove('layout-active'));
  document.getElementById('polarityGuide').hidden = true;
  document.getElementById('governmentGuide').hidden = true;
  const colorMode = document.getElementById('guidedColorMode');
  if (colorMode) colorMode.dispatchEvent(new Event('change'));
}}
function layoutJitter(id) {{
  let hash = 0;
  for (const char of String(id)) hash = ((hash * 31) + char.charCodeAt(0)) >>> 0;
  return ((hash % 201) - 100) * 1.15;
}}
function applyPolarityLayout() {{
  clearActiveLayout();
  document.getElementById('polarityLayout').classList.add('layout-active');
  document.getElementById('polarityGuide').hidden = false;
  network.stopSimulation();
  network.setOptions({{physics: {{enabled:false}}}});

  const visibleIds = [];
  nodes.get().forEach(node => {{
    const base = BASE_POSITIONS[node.id] || {{x:BASE_CENTER.x, y:BASE_CENTER.y}};
    const score = Math.max(-1, Math.min(1, Number(node.polarity_score) || 0));
    const polarity = String(node.polarity || 'neutral');
    const jitter = layoutJitter(node.id);
    const neutralOffset = Math.max(-600, Math.min(600, base.x - BASE_CENTER.x));
    let x = BASE_CENTER.x + neutralOffset * 0.55 + jitter * 0.25;
    if (polarity === 'positiva') {{
      const strength = Math.max(0, Math.min(1, (score - 0.20) / 0.80));
      x = BASE_CENTER.x - 760 - strength * 260 + jitter;
    }} else if (polarity === 'negativa') {{
      const strength = Math.max(0, Math.min(1, (-score - 0.20) / 0.80));
      x = BASE_CENTER.x + 760 + strength * 260 + jitter;
    }} else if (polarity === 'mixta') {{
      x = BASE_CENTER.x - Math.max(-1, Math.min(1, score / 0.20)) * 520 + jitter * 0.55;
    }}
    const y = BASE_CENTER.y + (base.y - BASE_CENTER.y) * 1.55;
    network.moveNode(node.id, x, y);
    if (!node.hidden) visibleIds.push(node.id);
  }});

  const semanticGroups = new Map();
  nodes.get()
    .filter(node => node.kind === 'tema' || node.kind === 'posicion')
    .forEach(node => {{
      const key = `${{node.polarity || 'neutral'}}:${{node.kind}}`;
      if (!semanticGroups.has(key)) semanticGroups.set(key, []);
      semanticGroups.get(key).push(node);
    }});
  semanticGroups.forEach((group, key) => {{
    const [polarity, kind] = key.split(':');
    const spacing = kind === 'tema' ? 210 : 110;
    group.sort((a, b) =>
      Number(a.tema) - Number(b.tema) || String(a.id).localeCompare(String(b.id)));
    group.forEach((node, index) => {{
      const current = network.getPosition(node.id);
      let x = current.x;
      if (polarity === 'positiva') {{
        x = BASE_CENTER.x - (kind === 'tema' ? 610 : 1040);
      }} else if (polarity === 'negativa') {{
        x = BASE_CENTER.x + (kind === 'tema' ? 610 : 1040);
      }}
      const y = BASE_CENTER.y + (index - (group.length - 1) / 2) * spacing;
      network.moveNode(node.id, x, y);
    }});
  }});

  const colorMode = document.getElementById('guidedColorMode');
  if (colorMode) {{
    colorMode.value = 'polarity';
    colorMode.dispatchEvent(new Event('change'));
  }}
  requestAnimationFrame(() => network.fit({{
    nodes: visibleIds,
    animation: {{duration:550, easingFunction:'easeInOutQuad'}}
  }}));
}}
function nodeTopics(node) {{
  const topics = Array.isArray(node.network_topics) ? node.network_topics : [];
  if (topics.length) return topics.map(Number).filter(Number.isFinite);
  const fallback = Number(node.tema);
  return Number.isFinite(fallback) ? [fallback] : [];
}}
function nearestBaseTopic(node, topicAnchors) {{
  const candidates = nodeTopics(node).filter(topic => topicAnchors.has(topic));
  if (!candidates.length) return null;
  const base = BASE_POSITIONS[node.id] || BASE_CENTER;
  return candidates.reduce((best, topic) => {{
    const topicBase = BASE_POSITIONS[`T${{String(topic).padStart(2, '0')}}`] || BASE_CENTER;
    const distance = Math.hypot(base.x - topicBase.x, base.y - topicBase.y);
    return !best || distance < best.distance ? {{topic, distance}} : best;
  }}, null).topic;
}}
function orbitPosition(anchor, index, total, baseRadius, perRing, ringGap, phase=0) {{
  const ring = Math.floor(index / perRing);
  const slot = index % perRing;
  const count = Math.min(perRing, total - ring * perRing);
  const angle = phase + (2 * Math.PI * slot / Math.max(1, count)) + ring * 0.27;
  const radius = baseRadius + ring * ringGap;
  return {{x:anchor.x + Math.cos(angle) * radius, y:anchor.y + Math.sin(angle) * radius}};
}}
function applyGovernmentLayout() {{
  clearActiveLayout();
  document.getElementById('governmentLayout').classList.add('layout-active');
  document.getElementById('governmentGuide').hidden = false;
  network.stopSimulation();
  network.setOptions({{physics: {{enabled:false}}}});

  const allNodes = nodes.get();
  const topicNodes = allNodes
    .filter(node => node.kind === 'tema')
    .sort((a, b) => Number(a.tema) - Number(b.tema));
  const topicAnchors = new Map();
  const topicRingRadius = 1500;
  topicNodes.forEach((node, index) => {{
    const angle = -Math.PI / 2 + (2 * Math.PI * index / Math.max(1, topicNodes.length));
    topicAnchors.set(Number(node.tema), {{
      x:BASE_CENTER.x + Math.cos(angle) * topicRingRadius,
      y:BASE_CENTER.y + Math.sin(angle) * topicRingRadius
    }});
  }});

  const mayorCategories = new Set(['Mónica Villarreal', 'Alcaldesa']);
  const centerNodes = allNodes
    .filter(node => node.kind !== 'tema' &&
      (node.guided_categories || []).some(category =>
        mayorCategories.has(category) || category === 'Gobierno municipal'))
    .sort((a, b) => String(a.id).localeCompare(String(b.id)));
  const centerIds = new Set(centerNodes.map(node => String(node.id)));
  const satellites = new Map();
  topicAnchors.forEach((_anchor, topic) => satellites.set(topic, {{posicion:[], cuenta:[], palabra:[], other:[]}}));

  allNodes.forEach(node => {{
    if (node.kind === 'tema' || centerIds.has(String(node.id))) return;
    const topic = nearestBaseTopic(node, topicAnchors);
    if (topic === null) return;
    const groups = satellites.get(topic);
    const kind = Object.hasOwn(groups, node.kind) ? node.kind : 'other';
    groups[kind].push(node);
  }});

  const visibleIds = [];
  topicNodes.forEach(node => {{
    const anchor = topicAnchors.get(Number(node.tema));
    network.moveNode(node.id, anchor.x, anchor.y);
    if (!node.hidden) visibleIds.push(node.id);
  }});
  topicAnchors.forEach((anchor, topic) => {{
    const groups = satellites.get(topic);
    const specs = {{
      posicion:{{radius:105, perRing:10, gap:34, phase:-Math.PI/2}},
      cuenta:{{radius:205, perRing:22, gap:32, phase:0.18}},
      palabra:{{radius:315, perRing:30, gap:30, phase:Math.PI}},
      other:{{radius:405, perRing:34, gap:28, phase:0}}
    }};
    Object.entries(groups).forEach(([kind, group]) => {{
      const spec = specs[kind];
      group.sort((a, b) => String(a.id).localeCompare(String(b.id)));
      group.forEach((node, index) => {{
        const point = orbitPosition(anchor, index, group.length, spec.radius, spec.perRing, spec.gap, spec.phase);
        network.moveNode(node.id, point.x, point.y);
        if (!node.hidden) visibleIds.push(node.id);
      }});
    }});
  }});

  const goldenAngle = Math.PI * (3 - Math.sqrt(5));
  centerNodes.forEach((node, index) => {{
    const radius = index ? 28 + Math.sqrt(index) * 19 : 0;
    const angle = index * goldenAngle;
    network.moveNode(
      node.id,
      BASE_CENTER.x + Math.cos(angle) * radius,
      BASE_CENTER.y + Math.sin(angle) * radius
    );
    if (!node.hidden) visibleIds.push(node.id);
  }});

  const colorMode = document.getElementById('guidedColorMode');
  if (colorMode) {{
    colorMode.value = 'original';
    colorMode.dispatchEvent(new Event('change'));
  }}
  nodes.update(centerNodes.map(node => {{
    const categories = node.guided_categories || [];
    const mayor = categories.some(category => mayorCategories.has(category));
    const both = mayor && categories.includes('Gobierno municipal');
    return {{
      id:node.id,
      color:{{
        background:mayor ? '#c62828' : '#9467bd',
        border:both ? '#ffd166' : '#ffffff'
      }},
      borderWidth:both ? 5 : 3
    }};
  }}));
  requestAnimationFrame(() => network.fit({{
    nodes: visibleIds,
    animation: {{duration:650, easingFunction:'easeInOutQuad'}}
  }}));
}}
function applySeparation(value) {{
  clearActiveLayout();
  const normalized = Math.max(0, Math.min(100, Number(value) || 0)) / 100;
  const scale = 1 + normalized * 1.25;
  network.stopSimulation();
  network.setOptions({{physics: {{enabled:false, forceAtlas2Based: {{springLength: Math.round(170 + normalized * 150)}}}}}});
  BASE_IDS.forEach(id => {{
    const base = BASE_POSITIONS[id];
    network.moveNode(id, BASE_CENTER.x + (base.x - BASE_CENTER.x) * scale, BASE_CENTER.y + (base.y - BASE_CENTER.y) * scale);
  }});
}}
requestAnimationFrame(() => network.fit({{animation:false}}));
</script>
</body>
</html>
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-dir", type=Path, default=DEFAULT_BASE)
    ap.add_argument("--input-csv", type=Path, default=DEFAULT_DATA)
    ap.add_argument(
        "--positions-per-topic",
        type=int,
        default=5,
        help="Máximo de posiciones por tema (default: 5).",
    )
    ap.add_argument("--min-account-words", type=int, default=5)
    ap.add_argument("--min-topic-words", type=int, default=3)
    ap.add_argument("--min-topic-pct", type=float, default=12.0)
    ap.add_argument("--max-vocab", type=int, default=90)
    ap.add_argument("--accounts-per-position", type=int, default=18)
    ap.add_argument(
        "--words-per-position",
        type=int,
        default=60,
        help="Máximo de palabras distintivas por posición (default: 60).",
    )
    ap.add_argument("--diccionario-temas", type=Path, default=DEFAULT_TOPIC_DICTIONARY)
    ap.add_argument("--diccionario-positivo", type=Path, default=DEFAULT_POSITIVE_DICTIONARY)
    ap.add_argument("--diccionario-negativo", type=Path, default=DEFAULT_NEGATIVE_DICTIONARY)
    ap.add_argument(
        "--diccionario-espanol",
        type=Path,
        default=DEFAULT_SPANISH_DICTIONARY,
        help="Diccionario usado para conservar vocabulario español.",
    )
    ap.add_argument(
        "--diccionario-ingles",
        type=Path,
        default=DEFAULT_ENGLISH_DICTIONARY,
        help="Diccionario usado para excluir vocabulario inglés.",
    )
    ap.add_argument("--output-filename", default="red_tampico_posiciones_guiada.html")
    ap.add_argument("--scope-label", default="Tampico histórico")
    ap.add_argument("--corpus-label", default="histórico consolidado de Tampico")
    args = ap.parse_args()
    if Path(args.output_filename).name != args.output_filename:
        ap.error("--output-filename debe ser solo un nombre de archivo")

    base = args.base_dir
    out_dir = base / "clusters" / "red_guiada"
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[1/5] Cargando insumos de {base}")
    mensajes, cuentas, palx, cxt = read_inputs(base, args.input_csv)
    spanish_vocabulary = load_language_vocabulary(str(args.diccionario_espanol))
    english_vocabulary = load_language_vocabulary(str(args.diccionario_ingles))
    words_before = len(palx)
    excluded_topics = english_dominated_topics(
        palx, spanish_vocabulary, english_vocabulary
    )
    palx = filter_spanish_word_rows(
        palx, spanish_vocabulary, english_vocabulary
    )
    if excluded_topics:
        palx = palx[~palx["tema"].astype(int).isin(excluded_topics)].copy()
    print(
        "      filtro de español: "
        f"{len(palx)} de {words_before} filas de vocabulario conservadas"
    )
    if excluded_topics:
        details = ", ".join(
            f"T{topic_id:02d} ({share:.0%} inglés)"
            for topic_id, share in sorted(excluded_topics.items())
        )
        print(f"      temas en inglés excluidos: {details}")
    topic_info = load_topic_info(
        base, spanish_vocabulary, english_vocabulary
    )

    print("[2/5] Calculando posiciones discursivas por tema")
    positions, memberships, position_words, examples = compute_positions(
        mensajes=mensajes,
        cuentas=cuentas,
        palx=palx,
        cxt=cxt,
        topic_info=topic_info,
        positions_per_topic=args.positions_per_topic,
        min_account_words=args.min_account_words,
        min_topic_words=args.min_topic_words,
        min_topic_pct=args.min_topic_pct,
        max_vocab=args.max_vocab,
        words_per_position=args.words_per_position,
    )

    print(f"      posiciones: {len(positions)}")
    print(f"      cuentas-posicion: {len(memberships)}")
    if positions.empty:
        raise SystemExit("No se generaron posiciones con los umbrales actuales.")

    print("[3/5] Escribiendo CSVs")
    positions.to_csv(out_dir / "posiciones_discursivas_guiada.csv", index=False)
    memberships.to_csv(out_dir / "cuentas_posiciones_guiada.csv", index=False)
    position_words.to_csv(out_dir / "palabras_posiciones_guiada.csv", index=False)
    examples.to_csv(out_dir / "ejemplos_posiciones_guiada.csv", index=False)

    print("[4/5] Construyendo red HTML autocontenida")
    nodes, edges, meta = build_network_data(
        positions,
        memberships,
        position_words,
        examples,
        topic_info,
        accounts_per_position=args.accounts_per_position,
        words_per_position=args.words_per_position,
    )
    nader_structural_ids, nader_bridge_ids = add_nader_structural_branch(
        nodes,
        edges,
        meta,
        mensajes,
        spanish_vocabulary,
        english_vocabulary,
        args.words_per_position,
    )
    html_path = out_dir / args.output_filename

    print("      aplicando temas rastreados, polaridad y postura")
    lexicons = load_lexicons(
        args.diccionario_temas,
        args.diccionario_positivo,
        args.diccionario_negativo,
    )
    annotations: dict[str, dict[str, Any]] = {}
    account_words = {
        str(usuario): [(str(r.palabra), float(r.conteo)) for r in grp.itertuples()]
        for usuario, grp in palx.groupby("usuario")
    }
    position_weighted = {
        str(posicion): [(str(r.palabra), float(r.conteo)) for r in grp.itertuples()]
        for posicion, grp in position_words.groupby("posicion_id")
    }
    topic_weighted = {
        int(tema): [(str(r.palabra), float(r.conteo)) for r in grp.itertuples()]
        for tema, grp in position_words.groupby("tema_id")
    }
    position_stance = {
        str(row.posicion_id): {
            "stance": str(row.postura_actor),
            "stance_label": str(row.postura_actor_etiqueta),
            "stance_score": float(row.postura_actor_score),
            "stance_support_hits": float(row.postura_actor_apoyo),
            "stance_critic_hits": float(row.postura_actor_critica),
            "stance_evidence": float(row.postura_actor_evidencia),
            "stance_target_hits": int(row.postura_actor_referencias),
            "stance_classified_messages": int(row.postura_actor_mensajes),
        }
        for row in positions.itertuples()
    }
    network_accounts = {
        str(meta.get(str(node["id"]), {}).get("usuario", ""))
        for node in nodes if node.get("kind") == "cuenta"
    }
    account_stance = {
        str(usuario): aggregate_stance_texts(grp["texto_limpio"].fillna(""))
        for usuario, grp in mensajes[mensajes["usuario"].isin(network_accounts)].groupby("usuario")
    }
    topic_stance: dict[int, dict[str, Any]] = {}
    for tema, group in positions.groupby("tema_id"):
        stance = stance_from_evidence(
            float(group["postura_actor_apoyo"].sum()),
            float(group["postura_actor_critica"].sum()),
        )
        stance["stance_target_hits"] = int(group["postura_actor_referencias"].sum())
        stance["stance_classified_messages"] = int(group["postura_actor_mensajes"].sum())
        topic_stance[int(tema)] = stance
    for node in nodes:
        node_id = str(node["id"])
        kind = str(node.get("kind", ""))
        if kind == "palabra":
            item = annotate_word(str(node.get("label", "")), lexicons)
            item["label"] = str(node.get("label", ""))
            annotations[node_id] = item
        elif kind == "cuenta":
            user = str(meta.get(node_id, {}).get("usuario", node.get("search", "")))
            item = aggregate_words(
                account_words.get(user, []), lexicons, kind="cuenta", label=user
            )
            annotations[node_id] = apply_stance_evidence(item, account_stance.get(user, {}))
        elif kind == "posicion":
            item = aggregate_words(
                position_weighted.get(node_id, []),
                lexicons,
                kind="posicion",
                label=str(meta.get(node_id, {}).get("nombre", node_id)),
            )
            annotations[node_id] = apply_stance_evidence(item, position_stance.get(node_id, {}))
        elif kind == "tema":
            tema = int(node.get("tema", -1))
            item = aggregate_words(
                topic_weighted.get(tema, []),
                lexicons,
                kind="tema",
                label=str(meta.get(node_id, {}).get("title", node_id)),
            )
            annotations[node_id] = apply_stance_evidence(item, topic_stance.get(tema, {}))
        network_topics: set[int] = set()
        if node.get("tema") is not None:
            network_topics.add(int(node["tema"]))
        for position in meta.get(node_id, {}).get("posiciones", []):
            if position.get("tema_id") is not None:
                network_topics.add(int(position["tema_id"]))
        annotations.setdefault(node_id, {})["network_topics"] = sorted(network_topics)

    for node_id in nader_structural_ids:
        item = annotations.setdefault(node_id, {})
        other_categories = [
            category
            for category in item.get("categories", [])
            if category.get("name") != "Jesús Nader"
        ]
        item["categories"] = [
            {
                "name": "Jesús Nader",
                "score": 1.0,
                "share": 1.0,
                "confidence": 1.0,
            },
            *other_categories,
        ]
        item["primary_category"] = "Jesús Nader"
        item["category_confidence"] = 1.0
        item["matched_weight"] = max(
            1.0, float(item.get("matched_weight", 0.0) or 0.0)
        )

    for node_id in nader_bridge_ids:
        item = annotations.setdefault(node_id, {})
        categories = [
            category
            for category in item.get("categories", [])
            if category.get("name") != "Jesús Nader"
        ]
        categories.append({
            "name": "Jesús Nader",
            "score": 1.0,
            "share": 0.0,
            "confidence": 1.0,
        })
        item["categories"] = categories
        item["matched_weight"] = max(
            1.0, float(item.get("matched_weight", 0.0) or 0.0)
        )

    # La vista de posiciones usa estos campos para acomodar todos los tipos de
    # nodo sobre un eje continuo de polaridad. Se conservan también dentro de
    # la capa guiada para filtros, colores y salidas de auditoría.
    for node in nodes:
        annotation = annotations.get(str(node["id"]), {})
        polarity = str(annotation.get("polarity", "neutral"))
        strategic_group = STRATEGIC_GROUP_BY_POLARITY.get(
            polarity, "opportunity"
        )
        node["polarity"] = polarity
        node["polarity_score"] = float(annotation.get("polarity_score", 0.0) or 0.0)
        node["strategic_group"] = strategic_group
        node["primary_category"] = str(annotation.get("primary_category", ""))
        node["guided_categories"] = [
            str(category.get("name", ""))
            for category in annotation.get("categories", [])
            if category.get("name")
        ]
        node["network_topics"] = [
            int(topic) for topic in annotation.get("network_topics", [])
        ]
        if str(node.get("kind", "")) in {"tema", "posicion"}:
            meta[str(node["id"])]["polaridad"] = polarity
            meta[str(node["id"])]["ambito_estrategico"] = (
                STRATEGIC_GROUP_LABELS[strategic_group]
            )

    html_out = build_html(nodes, edges, meta, topic_info, base, args.scope_label)
    html_path.write_text(html_out, encoding="utf-8")
    inject_guided_layer(
        html_path,
        annotations,
        lexicons.category_colors,
        "Filtros por capas",
        mount_id="right",
        layered_filters=True,
        exclusive_topic_strategies=True,
    )
    write_annotation_outputs(
        out_dir,
        annotations,
        "posiciones_guiadas",
        {
            "temas": args.diccionario_temas,
            "positivas": args.diccionario_positivo,
            "negativas": args.diccionario_negativo,
        },
    )

    metrics = {
        "corpus": args.corpus_label,
        "n_mensajes": int(len(mensajes)),
        "n_cuentas_total": int(len(cuentas)),
        "n_posiciones": int(len(positions)),
        "n_cuentas_posicion": int(len(memberships["usuario"].unique())) if not memberships.empty else 0,
        "n_nodes_html": int(len(nodes)),
        "n_edges_html": int(len(edges)),
        "params": {
            key: str(value) if isinstance(value, Path) else value
            for key, value in vars(args).items()
        },
        "ibea_por_nivel": positions["ibea_nivel"].value_counts().to_dict(),
    }
    (out_dir / "metricas_posiciones_guiada.json").write_text(
        json.dumps(metrics, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print("[5/5] OK")
    for path in [
        html_path,
        out_dir / "posiciones_discursivas_guiada.csv",
        out_dir / "cuentas_posiciones_guiada.csv",
        out_dir / "palabras_posiciones_guiada.csv",
        out_dir / "ejemplos_posiciones_guiada.csv",
        out_dir / "metricas_posiciones_guiada.json",
    ]:
        print(f"      - {path} ({path.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
