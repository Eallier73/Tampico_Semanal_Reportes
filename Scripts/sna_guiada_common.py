#!/usr/bin/env python3
"""Utilidades compartidas para las redes SNA guiadas por diccionarios."""

from __future__ import annotations

import html
import json
import math
import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


DEFAULT_DICT_DIR = Path(
    "/home/emilio/Documentos/RAdAR/Diccionarios_NLP/Diccionarios_Finales"
)
DEFAULT_TOPIC_DICTIONARY = DEFAULT_DICT_DIR / "diccionario_pmi_confianza_v10.xlsx"
DEFAULT_POSITIVE_DICTIONARY = DEFAULT_DICT_DIR / "diccionario_palabras_positivas.txt"
DEFAULT_NEGATIVE_DICTIONARY = DEFAULT_DICT_DIR / "diccionario_palabras_negativas.txt"
LOCAL_TOPIC_DICTIONARY = (
    Path(__file__).resolve().parent
    / "diccionarios"
    / "diccionario_temas_estructurales_tampico_local.csv"
)

CATEGORY_COLORS = {
    "Agua": "#1f77b4",
    "Alumbrado": "#f2c744",
    "Americo": "#9467bd",
    "Basura": "#2ca02c",
    "Corrupcion": "#d62728",
    "Delitos": "#8c564b",
    "Gobierno municipal": "#6a51a3",
    "Mónica Villarreal": "#c62828",
    "Morena": "#e377c2",
    "Obras": "#ff7f0e",
    "Prevencion": "#17becf",
    "Vialidad": "#7f7f7f",
}
POLARITY_COLORS = {
    "positiva": "#2ca02c",
    "negativa": "#d62728",
    "mixta": "#f2c744",
    "neutral": "#777777",
}
STRATEGIC_GROUP_BY_POLARITY = {
    "negativa": "risk",
    "mixta": "opportunity",
    "neutral": "opportunity",
    "positiva": "consolidation",
}
STANCE_COLORS = {
    "apoyo_defensa": "#2b83ba",
    "critica_oposicion": "#d7191c",
    "mixta_disputa": "#fdae61",
    "sin_postura": "#777777",
}
STANCE_LABELS = {
    "apoyo_defensa": "Apoyo/defensa",
    "critica_oposicion": "Crítica/oposición",
    "mixta_disputa": "Mixta/disputa",
    "sin_postura": "Sin postura",
}

# La postura se refiere especificamente a Monica Villarreal y a su
# administracion municipal. No reutiliza los diccionarios de polaridad.
STANCE_TARGET_WORDS = {
    "monica", "villarreal", "alcaldesa", "tampicogob", "ayuntamiento",
}
STANCE_SUPPORT_WORDS = {
    "apoyo", "apoyar", "respaldo", "respaldar", "defensa", "defender",
    "confiar", "confianza", "felicitar", "felicidades", "gracias",
    "excelente", "adelante", "compromiso", "resultado", "resultados",
    "orgullo", "bienestar", "cuidar", "cumplir", "transformacion",
}
STANCE_CRITIC_WORDS = {
    "corrupta", "corrupto", "corrupcion", "ratera", "ratero", "ladrona",
    "ladron", "inepta", "inepto", "incompetente", "pesima", "pesimo",
    "mentirosa", "mentiroso", "mentira", "robo", "saqueo", "nepotismo",
    "abandono", "incumplir", "incumplimiento", "fracaso", "renuncia",
    "dimision", "verguenza", "delincuente", "criminal",
}
_TARGET_PATTERNS = [
    re.compile(pattern)
    for pattern in (
        r"\bmonica(?:\s+villarreal(?:\s+anaya)?)?\b",
        r"\bvillarreal\b",
        r"\balcaldesa\b",
        r"\bpresidenta\s+municipal\b",
        r"\bgobierno\s+(?:municipal|de\s+tampico)\b",
        r"\bayuntamiento(?:\s+de\s+tampico)?\b",
        r"\bmunicipio\s+de\s+tampico\b",
        r"\badministracion\s+municipal\b",
        r"\btampico\s*gob\b",
    )
]
_DEFENSE_PATTERNS = [
    re.compile(pattern)
    for pattern in (
        r"\b(?:no|nunca)\s+(?:la\s+)?(?:ataquen|critiquen|insulten|difamen)\b",
        r"\bdejen\s+de\s+(?:atacar|criticar|insultar|difamar)\b",
        r"\b(?:injusto|coraje)\s+que\s+(?:la\s+)?(?:ataquen|critiquen|insulten)\b",
        r"\b(?:estamos|estoy)\s+contigo\b",
        r"\bno\s+esta\s+sola\b",
        r"\bcuenta\s+conmigo\b",
        r"\bcon\s+monica\b",
    )
]
_CRITIC_PATTERNS = [
    re.compile(pattern)
    for pattern in (
        r"\bfuera\s+(?:monica|villarreal|la\s+alcaldesa)\b",
        r"\b(?:monica|villarreal|alcaldesa).{0,30}\b(?:renuncia|que\s+se\s+vaya)\b",
        r"\b(?:no|nunca)\s+(?:sirve|cumple|trabaja|resuelve)\b",
        r"\b(?:ya\s+)?no\s+(?:la\s+)?apoyo\b",
    )
]


def normalize(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value).strip().lower())
    return "".join(ch for ch in text if not unicodedata.combining(ch))


def stance_from_evidence(support_hits: float, critic_hits: float) -> dict[str, Any]:
    total = max(0.0, support_hits) + max(0.0, critic_hits)
    score = (support_hits - critic_hits) / total if total else 0.0
    if total <= 0:
        stance = "sin_postura"
    elif score >= 0.25:
        stance = "apoyo_defensa"
    elif score <= -0.25:
        stance = "critica_oposicion"
    else:
        stance = "mixta_disputa"
    return {
        "stance": stance,
        "stance_label": STANCE_LABELS[stance],
        "stance_score": round(score, 4),
        "stance_support_hits": round(support_hits, 2),
        "stance_critic_hits": round(critic_hits, 2),
        "stance_evidence": round(total, 2),
    }


def classify_stance_text(value: Any) -> dict[str, Any]:
    """Clasifica postura hacia la alcaldesa/administracion, no tono emocional."""
    normalized = normalize(value)
    tokens = re.findall(r"[a-z]{3,}", normalized)
    target_hits = sum(len(pattern.findall(normalized)) for pattern in _TARGET_PATTERNS)
    if target_hits <= 0:
        return {**stance_from_evidence(0.0, 0.0), "stance_target_hits": 0}

    support_hits = float(sum(token in STANCE_SUPPORT_WORDS for token in tokens))
    critic_hits = float(sum(token in STANCE_CRITIC_WORDS for token in tokens))
    defensive_hits = sum(bool(pattern.search(normalized)) for pattern in _DEFENSE_PATTERNS)
    direct_critic_hits = sum(bool(pattern.search(normalized)) for pattern in _CRITIC_PATTERNS)
    support_hits += 2.0 * defensive_hits
    critic_hits += 2.0 * direct_critic_hits

    # "No apoyo" expresa critica, aunque contenga literalmente apoyo.
    if re.search(r"\b(?:ya\s+)?no\s+(?:la\s+)?apoyo\b", normalized):
        support_hits = max(0.0, support_hits - 1.0)

    result = stance_from_evidence(support_hits, critic_hits)
    result["stance_target_hits"] = target_hits
    return result


def aggregate_stance_texts(values: Iterable[Any]) -> dict[str, Any]:
    support_hits = 0.0
    critic_hits = 0.0
    target_hits = 0
    classified_messages = 0
    for value in values:
        item = classify_stance_text(value)
        target_hits += int(item.get("stance_target_hits", 0))
        support_hits += float(item.get("stance_support_hits", 0.0))
        critic_hits += float(item.get("stance_critic_hits", 0.0))
        classified_messages += int(item.get("stance_evidence", 0.0) > 0)
    result = stance_from_evidence(support_hits, critic_hits)
    result.update({
        "stance_target_hits": target_hits,
        "stance_classified_messages": classified_messages,
    })
    return result


def apply_stance_evidence(
    annotation: dict[str, Any], evidence: dict[str, Any]
) -> dict[str, Any]:
    for key in (
        "stance", "stance_label", "stance_score", "stance_support_hits",
        "stance_critic_hits", "stance_evidence", "stance_target_hits",
        "stance_classified_messages",
    ):
        if key in evidence:
            annotation[key] = evidence[key]
    return annotation


@dataclass
class GuidedLexicons:
    categories: dict[str, list[dict[str, Any]]]
    positive: set[str]
    negative: set[str]
    category_colors: dict[str, str]


def _lemma_keys(words: list[str]) -> list[set[str]]:
    """Devuelve forma normalizada y lemas para cada entrada del diccionario."""
    import spacy

    nlp = spacy.load("es_core_news_md", disable=["parser", "ner"])
    result: list[set[str]] = []
    for raw, doc in zip(words, nlp.pipe(words, batch_size=256), strict=True):
        keys = {normalize(raw)}
        keys.update(normalize(token.lemma_) for token in doc if token.is_alpha)
        result.append({key for key in keys if key})
    return result


def _read_word_list(path: Path) -> list[str]:
    return [
        line.strip()
        for line in path.read_text(encoding="utf-8-sig", errors="replace").splitlines()
        if line.strip()
    ]


def load_lexicons(
    topic_dictionary: Path = DEFAULT_TOPIC_DICTIONARY,
    positive_dictionary: Path = DEFAULT_POSITIVE_DICTIONARY,
    negative_dictionary: Path = DEFAULT_NEGATIVE_DICTIONARY,
) -> GuidedLexicons:
    for path in (topic_dictionary, positive_dictionary, negative_dictionary):
        if not path.exists():
            raise FileNotFoundError(f"No existe el diccionario requerido: {path}")

    required_columns = ["Categoria", "Palabra", "Delta_PMI", "Confianza"]
    required = set(required_columns)
    topics = pd.read_excel(topic_dictionary)
    if not required.issubset(topics.columns):
        raise ValueError(
            f"El Excel debe contener {sorted(required)}; contiene {list(topics.columns)}"
        )

    # El diccionario de RAdAR aporta las categorias generales. Este suplemento
    # versionado en Tampico agrega actores y gobierno local sin modificar el
    # archivo externo ni copiar nombres propios de otros municipios.
    if LOCAL_TOPIC_DICTIONARY.exists():
        local_topics = pd.read_csv(LOCAL_TOPIC_DICTIONARY)
        if not required.issubset(local_topics.columns):
            raise ValueError(
                f"El suplemento local debe contener {sorted(required)}; "
                f"contiene {list(local_topics.columns)}"
            )
        topics = pd.concat(
            [topics, local_topics[required_columns]],
            ignore_index=True,
        )

    raw_topic_words = topics["Palabra"].astype(str).tolist()
    topic_keys = _lemma_keys(raw_topic_words)
    category_index: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for row, keys in zip(topics.itertuples(index=False), topic_keys, strict=True):
        category = str(row.Categoria).strip()
        entry = {
            "name": category,
            "confidence": float(row.Confianza),
            "delta_pmi": float(row.Delta_PMI),
            "source_word": str(row.Palabra),
        }
        for key in keys:
            previous = category_index[key].get(category)
            if previous is None or entry["confidence"] > previous["confidence"]:
                category_index[key][category] = entry

    positive_words = _read_word_list(positive_dictionary)
    negative_words = _read_word_list(negative_dictionary)
    positive = set().union(*_lemma_keys(positive_words))
    negative = set().union(*_lemma_keys(negative_words))

    categories = {
        key: sorted(entries.values(), key=lambda item: -item["confidence"])
        for key, entries in category_index.items()
    }
    category_names = sorted(topics["Categoria"].astype(str).unique())
    fallback = [
        "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
        "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
    ]
    colors = {
        name: CATEGORY_COLORS.get(name, fallback[idx % len(fallback)])
        for idx, name in enumerate(category_names)
    }
    return GuidedLexicons(categories, positive, negative, colors)


def annotate_word(word: str, lexicons: GuidedLexicons) -> dict[str, Any]:
    key = normalize(word)
    categories = lexicons.categories.get(key, [])
    in_positive = key in lexicons.positive
    in_negative = key in lexicons.negative
    if in_positive and in_negative:
        polarity = "mixta"
        polarity_score = 0.0
    elif in_positive:
        polarity = "positiva"
        polarity_score = 1.0
    elif in_negative:
        polarity = "negativa"
        polarity_score = -1.0
    else:
        polarity = "neutral"
        polarity_score = 0.0
    stance_data = stance_from_evidence(0.0, 0.0)
    primary = categories[0] if categories else None
    return {
        "kind": "palabra",
        "label": word,
        "categories": categories,
        "primary_category": primary["name"] if primary else "",
        "category_confidence": primary["confidence"] if primary else 0.0,
        "delta_pmi": primary["delta_pmi"] if primary else 0.0,
        "polarity": polarity,
        "polarity_score": polarity_score,
        **stance_data,
        "positive_hits": int(in_positive),
        "negative_hits": int(in_negative),
        "matched_weight": int(bool(categories) or in_positive or in_negative),
    }


def annotate_words(words: Iterable[str], lexicons: GuidedLexicons) -> dict[str, dict[str, Any]]:
    return {str(word): annotate_word(str(word), lexicons) for word in words}


def aggregate_words(
    weighted_words: Iterable[tuple[str, float]],
    lexicons: GuidedLexicons,
    *,
    kind: str,
    label: str,
) -> dict[str, Any]:
    category_scores: Counter[str] = Counter()
    category_confidence: dict[str, float] = defaultdict(float)
    positive_hits = 0.0
    negative_hits = 0.0
    matched_weight = 0.0
    stance_word_weights: Counter[str] = Counter()
    for word, raw_weight in weighted_words:
        weight = max(0.0, float(raw_weight))
        if weight <= 0:
            continue
        annotation = annotate_word(str(word), lexicons)
        stance_word_weights[normalize(word)] += weight
        matched = False
        for category in annotation["categories"]:
            score = weight * float(category["confidence"])
            category_scores[category["name"]] += score
            category_confidence[category["name"]] = max(
                category_confidence[category["name"]], float(category["confidence"])
            )
            matched = True
        if annotation["positive_hits"]:
            positive_hits += weight
            matched = True
        if annotation["negative_hits"]:
            negative_hits += weight
            matched = True
        if matched:
            matched_weight += weight

    total_category = sum(category_scores.values())
    categories = [
        {
            "name": name,
            "score": round(score, 4),
            "share": round(score / total_category, 4) if total_category else 0.0,
            "confidence": round(category_confidence[name], 4),
        }
        for name, score in category_scores.most_common()
    ]
    polarity_total = positive_hits + negative_hits
    polarity_score = (
        (positive_hits - negative_hits) / polarity_total if polarity_total else 0.0
    )
    if not polarity_total:
        polarity = "neutral"
    elif polarity_score >= 0.20:
        polarity = "positiva"
    elif polarity_score <= -0.20:
        polarity = "negativa"
    else:
        polarity = "mixta"
    target_hits = sum(stance_word_weights[word] for word in STANCE_TARGET_WORDS)
    support_hits = sum(stance_word_weights[word] for word in STANCE_SUPPORT_WORDS)
    critic_hits = sum(stance_word_weights[word] for word in STANCE_CRITIC_WORDS)
    stance_data = stance_from_evidence(
        support_hits if target_hits else 0.0,
        critic_hits if target_hits else 0.0,
    )
    stance_data["stance_target_hits"] = round(target_hits, 2)
    primary = categories[0] if categories else None
    return {
        "kind": kind,
        "label": label,
        "categories": categories,
        "primary_category": primary["name"] if primary else "",
        "category_confidence": primary["confidence"] if primary else 0.0,
        "delta_pmi": 0.0,
        "polarity": polarity,
        "polarity_score": round(polarity_score, 4),
        **stance_data,
        "positive_hits": round(positive_hits, 2),
        "negative_hits": round(negative_hits, 2),
        "matched_weight": round(matched_weight, 2),
    }


def write_annotation_outputs(
    out_dir: Path,
    annotations: dict[str, dict[str, Any]],
    prefix: str,
    source_paths: dict[str, Path],
) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    category_counts: Counter[str] = Counter()
    polarity_counts: Counter[str] = Counter()
    stance_counts: Counter[str] = Counter()
    strategic_counts: Counter[str] = Counter()
    for node_id, item in annotations.items():
        category_counts.update(cat["name"] for cat in item.get("categories", []))
        polarity = item.get("polarity", "neutral")
        strategic_group = STRATEGIC_GROUP_BY_POLARITY.get(polarity, "opportunity")
        polarity_counts[polarity] += 1
        stance_counts[item.get("stance", "sin_postura")] += 1
        strategic_counts[strategic_group] += 1
        rows.append({
            "node_id": node_id,
            "tipo_nodo": item.get("kind", ""),
            "etiqueta": item.get("label", ""),
            "categoria_principal": item.get("primary_category", ""),
            "confianza_categoria": item.get("category_confidence", 0.0),
            "categorias": " | ".join(cat["name"] for cat in item.get("categories", [])),
            "polaridad": polarity,
            "polaridad_score": item.get("polarity_score", 0.0),
            "ambito_estrategico": strategic_group,
            "postura": item.get("stance", "sin_postura"),
            "postura_etiqueta": item.get("stance_label", STANCE_LABELS["sin_postura"]),
            "postura_score": item.get("stance_score", 0.0),
            "evidencia_postura": item.get("stance_evidence", 0.0),
            "mensajes_con_postura": item.get("stance_classified_messages", 0),
            "coincidencias_positivas": item.get("positive_hits", 0),
            "coincidencias_negativas": item.get("negative_hits", 0),
            "peso_coincidente": item.get("matched_weight", 0),
        })
    pd.DataFrame(rows).to_csv(out_dir / f"{prefix}_anotaciones.csv", index=False)
    summary = {
        "n_nodos": len(annotations),
        "n_nodos_con_categoria": sum(bool(v.get("categories")) for v in annotations.values()),
        "n_nodos_con_polaridad": sum(v.get("polarity") != "neutral" for v in annotations.values()),
        "n_nodos_con_postura": sum(v.get("stance") != "sin_postura" for v in annotations.values()),
        "categorias": dict(category_counts.most_common()),
        "polaridades": dict(polarity_counts.most_common()),
        "posturas": dict(stance_counts.most_common()),
        "ambitos_estrategicos": dict(strategic_counts.most_common()),
        "metodo_ambito_estrategico": "clasificacion exclusiva por polaridad: negativa=riesgo, positiva=consolidacion, mixta/neutral=oportunidad",
        "metodo_postura": "referencia a Monica Villarreal/gobierno municipal + senales independientes de apoyo o critica",
        "fuentes": {key: str(value) for key, value in source_paths.items()},
    }
    (out_dir / f"{prefix}_resumen.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return summary


def compact_annotations_for_html(annotations: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Conserva en HTML solo nodos que sirven para localizar/colorar."""
    compact: dict[str, dict[str, Any]] = {}
    for node_id, item in annotations.items():
        categories = item.get("categories") or []
        polarity = item.get("polarity") or "neutral"
        stance = item.get("stance") or "sin_postura"
        network_topics = item.get("network_topics") or []
        if not categories and polarity == "neutral" and stance == "sin_postura" and not network_topics:
            continue
        compact[node_id] = {
            "categories": categories,
            "primary_category": item.get("primary_category") or "",
            "category_confidence": item.get("category_confidence") or 0.0,
            "polarity": polarity,
            "polarity_score": item.get("polarity_score") or 0.0,
            "strategic_group": STRATEGIC_GROUP_BY_POLARITY.get(
                polarity, "opportunity"
            ),
            "stance": stance,
            "stance_label": item.get("stance_label") or STANCE_LABELS["sin_postura"],
            "stance_score": item.get("stance_score") or 0.0,
            "network_topics": network_topics,
        }
    return compact


def inject_guided_layer(
    html_path: Path,
    annotations: dict[str, dict[str, Any]],
    category_colors: dict[str, str],
    title: str,
    panel_top_px: int = 12,
    network_topic_labels: dict[int, str] | None = None,
    mount_id: str | None = None,
    layered_filters: bool = False,
    exclusive_topic_strategies: bool = False,
) -> None:
    """Inyecta un panel no destructivo de localizacion y color en una red vis."""
    source = html_path.read_text(encoding="utf-8")
    annotations_json = json.dumps(compact_annotations_for_html(annotations), ensure_ascii=False).replace("</", "<\\/")
    colors_json = json.dumps(category_colors, ensure_ascii=False)
    polarity_json = json.dumps(POLARITY_COLORS, ensure_ascii=False)
    stance_json = json.dumps(STANCE_COLORS, ensure_ascii=False)
    category_buttons = "".join(
        f'<button class="guided-target" data-category="{html.escape(category)}">'
        f'<span style="background:{color}"></span>{html.escape(category)}</button>'
        for category, color in category_colors.items()
    )
    network_topic_buttons = "".join(
        f'<button class="guided-target" data-network-topic="{int(topic_id)}">'
        f'<span style="background:#777"></span>{html.escape(label)}</button>'
        for topic_id, label in (network_topic_labels or {}).items()
    )
    topic_filter_section = (
        f"<h4>Temas de la red</h4>{network_topic_buttons}"
        if network_topic_buttons else ""
    )
    topic_filter_markup = f"\n  {topic_filter_section}" if topic_filter_section else ""
    filter_help = (
        '<div class="guided-help">Suma opciones dentro de una capa y cruza las capas entre si.</div>'
        if layered_filters else ""
    )
    filter_help_markup = f"\n  {filter_help}" if filter_help else ""
    panel_position = (
        "position:static; width:auto; max-height:none; overflow:visible; padding:0 0 10px; "
        "background:transparent; border:0; border-bottom:1px solid #444; "
        "border-radius:0; box-shadow:none;"
        if mount_id else
        f"position:fixed; top:{panel_top_px}px; right:12px; width:290px; "
        f"max-height:calc(100vh - {panel_top_px + 12}px); overflow:auto; padding:12px; "
        "background:rgba(18,18,18,.96); border:1px solid #666; "
        "border-radius:6px; box-shadow:0 4px 18px #0008;"
    )
    mount_script = (
        f"const guidedMount = document.getElementById({json.dumps(mount_id)});\n"
        "  const guidedPanel = document.getElementById('guidedPanel');\n"
        "  if (guidedMount && guidedPanel) guidedMount.insertBefore(guidedPanel, guidedMount.firstChild);"
        if mount_id else ""
    )
    panel = f"""
<style>
#guidedPanel {{ {panel_position} z-index:2147483001; box-sizing:border-box;
  color:#eee; font:12px Arial,sans-serif; }}
#guidedPanel h3 {{ margin:0 0 8px; font-size:14px; }}
#guidedPanel h4 {{ margin:10px 0 5px; padding-top:7px; border-top:1px solid #444;
  color:#bbb; font-size:10px; text-transform:uppercase; }}
#guidedPanel button, #guidedPanel select {{ background:#303030; color:#eee;
  border:1px solid #666; border-radius:3px; padding:4px 6px; cursor:pointer;
  box-sizing:border-box; min-width:0; }}
#guidedPanel button:hover {{ background:#454545; }}
#guidedPanel button.guided-active {{ outline:2px solid #f5f5f5; background:#505050; }}
#guidedPanel .guided-target {{ width:100%; display:flex; align-items:center;
  gap:6px; margin:3px 0; text-align:left; }}
#guidedPanel .guided-target span {{ width:11px; height:11px; border-radius:50%; flex:none; }}
#guidedPanel .guided-row {{ display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:5px; margin:4px 0; }}
#guidedPanel .guided-row button {{ white-space:normal; overflow-wrap:anywhere; }}
#guidedPanel .guided-row button:only-child {{ grid-column:1 / -1; }}
#guidedPanel select, #guidedPanel input[type=range] {{ width:100%; }}
#guidedPanel .guided-help {{ color:#aaa; line-height:1.35; margin:0 0 8px; }}
#guidedPanel .guided-option-help {{ color:#999; font-size:10px; line-height:1.35; margin:4px 0 7px; }}
#guidedStats {{ margin-top:7px; color:#bbb; line-height:1.35; }}
</style>
<div id="guidedPanel">
  <h3>{html.escape(title)}</h3>{filter_help_markup}
  <label>Color de nodos
    <select id="guidedColorMode">
      <option value="original">Estructura original</option>
      <option value="category">Tema estructural</option>
      <option value="polarity">Polaridad</option>
      <option value="stance">Postura hacia Mónica/gobierno</option>
    </select>
  </label>
  <div class="guided-option-help">Elige cómo se colorean los puntos. Esto cambia la lectura visual, no los datos ni las conexiones.</div>{topic_filter_markup}
  <h4>Temas estructurales</h4>
  {category_buttons}
  <div class="guided-option-help">Pulsa uno o varios asuntos para ver únicamente los nodos cuyo vocabulario coincide con ellos.</div>
  <label>Confianza minima: <b id="guidedConfidenceValue">0.00</b></label>
  <input id="guidedConfidence" type="range" min="0" max="100" value="0">
  <div class="guided-option-help">Súbela para exigir coincidencias más claras; bájala para incluir asociaciones más amplias.</div>
  <h4>Polaridad lexica</h4>
  <div class="guided-row">
    <button data-polarity="positiva">Positiva</button>
    <button data-polarity="negativa">Negativa</button>
    <button data-polarity="mixta">Mixta</button>
  </div>
  <div class="guided-option-help">Permite aislar lenguaje favorable, desfavorable o combinado. Describe palabras usadas, no necesariamente la intención completa.</div>
  <h4>Postura hacia Mónica/gobierno</h4>
  <div class="guided-row">
    <button data-stance="apoyo_defensa">Apoyo/defensa</button>
    <button data-stance="critica_oposicion">Crítica/oposición</button>
    <button data-stance="mixta_disputa">Mixta/disputa</button>
  </div>
  <div class="guided-option-help">Mide la posición respecto a Mónica Villarreal o el gobierno municipal mediante referencias al actor y señales de respaldo o crítica. No reutiliza la polaridad.</div>
  <div class="guided-row"><button id="guidedReset">Restaurar vista</button></div>
  <div class="guided-option-help">Restaurar elimina todos los filtros y vuelve a mostrar la red completa.</div>
  <div id="guidedStats">Preparando capa guiada...</div>
</div>
<script>
(function() {{
  {mount_script}
  const GUIDED_META = {annotations_json};
  const CATEGORY_COLORS = {colors_json};
  const POLARITY_COLORS = {polarity_json};
  const STANCE_COLORS = {stance_json};
  const originals = {{}};
  const LAYERED_FILTERS = {str(layered_filters).lower()};
  const EXCLUSIVE_TOPIC_STRATEGIES = {str(exclusive_topic_strategies).lower()};
  const activeFilters = {{topic:new Set(), category:new Set(), polarity:new Set(), stance:new Set(), strategic:new Set()}};
  let strategicCombine = null;
  let strategicSeedIds = new Set();
  let strategicContextIds = new Set();
  let guidedFocus = null;
  let guidedNodes = null;
  let guidedEdges = null;
  let guidedNetwork = null;
  window.guidedStrategicState = {{seedIds:[], contextIds:[]}};
  function publishStrategicState() {{
    window.guidedStrategicState = {{
      seedIds: Array.from(strategicSeedIds),
      contextIds: Array.from(strategicContextIds),
    }};
  }}
  function confidence() {{
    return parseInt(document.getElementById('guidedConfidence').value || '0', 10) / 100;
  }}
  function resolveNetwork() {{
    guidedNetwork = (typeof network !== 'undefined') ? network : window.network;
    guidedNodes = (typeof nodes !== 'undefined') ? nodes : window.nodes;
    guidedEdges = (typeof edges !== 'undefined') ? edges : window.edges;
    if (!guidedNodes && guidedNetwork && guidedNetwork.body && guidedNetwork.body.data) {{
      guidedNodes = guidedNetwork.body.data.nodes;
    }}
    if (!guidedEdges && guidedNetwork && guidedNetwork.body && guidedNetwork.body.data) {{
      guidedEdges = guidedNetwork.body.data.edges;
    }}
    return !!(guidedNodes && guidedNetwork);
  }}
  function initGuided() {{
    if (!resolveNetwork()) {{
      setTimeout(initGuided, 100); return;
    }}
    guidedNodes.get().forEach(function(node) {{
      originals[node.id] = {{
        color: node.color,
        title: node.title,
        size: node.size,
        borderWidth: node.borderWidth,
        font: node.font
      }};
    }});
    document.querySelectorAll('[data-network-topic]').forEach(btn =>
      btn.addEventListener('click', () => toggleFilter('topic', btn.dataset.networkTopic, btn)));
    document.querySelectorAll('#guidedPanel [data-category]').forEach(btn =>
      btn.addEventListener('click', () => LAYERED_FILTERS ? toggleFilter('category', btn.dataset.category, btn) : locate('category', btn.dataset.category)));
    document.querySelectorAll('#guidedPanel [data-polarity]').forEach(btn =>
      btn.addEventListener('click', () => LAYERED_FILTERS ? toggleFilter('polarity', btn.dataset.polarity, btn) : locate('polarity', btn.dataset.polarity)));
    document.querySelectorAll('#guidedPanel [data-stance]').forEach(btn =>
      btn.addEventListener('click', () => LAYERED_FILTERS ? toggleFilter('stance', btn.dataset.stance, btn) : locate('stance', btn.dataset.stance)));
    document.getElementById('guidedColorMode').addEventListener('change', recolor);
    document.getElementById('guidedConfidence').addEventListener('input', function() {{
      document.getElementById('guidedConfidenceValue').textContent = confidence().toFixed(2);
      if (LAYERED_FILTERS) applyLayerFilters(); else recolor();
    }});
    document.getElementById('guidedReset').addEventListener('click', resetGuided);
    const categorized = Object.values(GUIDED_META).filter(m => (m.categories || []).length).length;
    const polarized = Object.values(GUIDED_META).filter(m => m.polarity && m.polarity !== 'neutral').length;
    const positioned = Object.values(GUIDED_META).filter(m => m.stance && m.stance !== 'sin_postura').length;
    document.getElementById('guidedStats').innerHTML = '<b>' + categorized +
      '</b> con tema · <b>' + polarized + '</b> con polaridad · <b>' + positioned + '</b> con postura';
  }}
  function categoryMatch(meta, category) {{
    return (meta.categories || []).some(c => c.name === category &&
      Number(c.confidence || 0) >= confidence());
  }}
  function matchesFocus(meta, focus) {{
    if (!focus || !meta) return false;
    if (focus.kind === 'category') return categoryMatch(meta, focus.value);
    if (focus.kind === 'polarity') return meta.polarity === focus.value;
    return meta.stance === focus.value;
  }}
  function hasActiveFilters() {{
    return Object.values(activeFilters).some(values => values.size > 0);
  }}
  function matchesLayerFilters(meta) {{
    if (!hasActiveFilters()) return true;
    if (!meta) return false;
    if (activeFilters.topic.size && !(meta.network_topics || []).some(value => activeFilters.topic.has(String(value)))) return false;
    if (activeFilters.category.size && !Array.from(activeFilters.category).some(value => categoryMatch(meta, value))) return false;
    if (activeFilters.strategic.size && !activeFilters.strategic.has(meta.strategic_group)) return false;
    if (strategicCombine === 'any') {{
      const polarityMatch = activeFilters.polarity.size && activeFilters.polarity.has(meta.polarity);
      const stanceMatch = activeFilters.stance.size && activeFilters.stance.has(meta.stance);
      if (!polarityMatch && !stanceMatch) return false;
    }} else {{
      if (activeFilters.polarity.size && !activeFilters.polarity.has(meta.polarity)) return false;
      if (activeFilters.stance.size && !activeFilters.stance.has(meta.stance)) return false;
    }}
    return true;
  }}
  function toggleFilter(kind, value, button) {{
    if (strategicCombine) {{
      strategicCombine = null;
      strategicSeedIds.clear();
      strategicContextIds.clear();
      Object.values(activeFilters).forEach(values => values.clear());
      publishStrategicState();
      window.dispatchEvent(new CustomEvent('guided-strategy-cleared'));
    }}
    const values = activeFilters[kind];
    if (values.has(value)) values.delete(value); else values.add(value);
    button.classList.toggle('guided-active', values.has(value));
    if (kind !== 'topic') document.getElementById('guidedColorMode').value = kind;
    applyLayerFilters();
  }}
  function buildStrategicNeighborhood() {{
    strategicSeedIds = new Set(
      Object.keys(GUIDED_META).filter(id => guidedNodes.get(id) && matchesLayerFilters(GUIDED_META[id]))
    );
    strategicContextIds = new Set(strategicSeedIds);
    function addContext(id) {{
      const node = guidedNodes.get(id);
      if (EXCLUSIVE_TOPIC_STRATEGIES && node && node.kind === 'tema' && !strategicSeedIds.has(id)) return;
      strategicContextIds.add(id);
    }}
    const networkEdges = guidedEdges ? guidedEdges.get() : [];
    networkEdges.forEach(edge => {{
      const from = String(edge.from);
      const to = String(edge.to);
      if (strategicSeedIds.has(from) || strategicSeedIds.has(to)) {{
        addContext(from);
        addContext(to);
      }}
    }});
    networkEdges.forEach(edge => {{
      if (edge.kind !== 'tema_posicion') return;
      const from = String(edge.from);
      const to = String(edge.to);
      if (strategicContextIds.has(from) || strategicContextIds.has(to)) {{
        addContext(from);
        addContext(to);
      }}
    }});
    publishStrategicState();
  }}
  function applyLayerFilters() {{
    if (strategicCombine) buildStrategicNeighborhood();
    window.guidedNodeAllowed = node => strategicCombine
      ? strategicContextIds.has(String(node.id))
      : matchesLayerFilters(GUIDED_META[String(node.id)]);
    if (typeof rebuild === 'function') rebuild();
    else guidedNodes.update(guidedNodes.get().map(node => ({{id:node.id, hidden:!window.guidedNodeAllowed(node)}})));
    recolor();
    const matching = Object.keys(GUIDED_META).filter(id => matchesLayerFilters(GUIDED_META[id]) && guidedNodes.get(id) && !guidedNodes.get(id).hidden);
    const visible = guidedNodes.get().filter(node => !node.hidden);
    guidedNetwork.unselectAll();
    guidedNetwork.selectNodes(matching.slice(0, 1000));
    if (strategicCombine) {{
      document.getElementById('guidedStats').innerHTML = '<b>' + matching.length +
        '</b> coincidencias · <b>' + Math.max(0, visible.length - matching.length) +
        '</b> nodos de contexto · <b>' + visible.length + '</b> visibles';
    }} else {{
      document.getElementById('guidedStats').innerHTML = hasActiveFilters()
        ? '<b>' + matching.length + '</b> nodos en el subconjunto activo'
        : 'Sin filtros de capa activos';
    }}
  }}
  function setActiveButton(kind, value) {{
    document.querySelectorAll('[data-network-topic], #guidedPanel button').forEach(btn => btn.classList.remove('guided-active'));
    let selector = '[data-stance="' + value + '"]';
    if (kind === 'category') selector = '[data-category="' + value + '"]';
    else if (kind === 'polarity') selector = '[data-polarity="' + value + '"]';
    const btn = document.querySelector('#guidedPanel ' + selector);
    if (btn) btn.classList.add('guided-active');
  }}
  function mutedColor() {{
    return {{background:'#151515', border:'#333'}};
  }}
  function focusColor(meta, focus) {{
    if (focus.kind === 'category') return {{background: CATEGORY_COLORS[focus.value] || '#777', border:'#ffffff'}};
    if (focus.kind === 'polarity') return {{background: POLARITY_COLORS[meta.polarity] || '#777', border:'#ffffff'}};
    return {{background: STANCE_COLORS[meta.stance] || '#777', border:'#ffffff'}};
  }}
  function locate(kind, value) {{
    guidedFocus = {{kind: kind, value: value}};
    document.getElementById('guidedColorMode').value = kind;
    setActiveButton(kind, value);
    recolor();
    const ids = Object.keys(GUIDED_META).filter(id => {{
      const meta = GUIDED_META[id];
      return matchesFocus(meta, guidedFocus);
    }}).filter(id => guidedNodes.get(id) && !guidedNodes.get(id).hidden);
    guidedNetwork.unselectAll();
    const selected = ids.slice(0, 1000);
    const fitted = ids.slice(0, 250);
    guidedNetwork.selectNodes(selected);
    if (fitted.length) guidedNetwork.fit({{nodes: fitted, animation: {{duration: 500}}}});
    document.getElementById('guidedStats').innerHTML = '<b>' + ids.length +
      '</b> nodos localizados en ' + value;
  }}
  function recolor() {{
    const mode = document.getElementById('guidedColorMode').value;
    const updates = [];
    guidedNodes.get().forEach(node => {{
      const meta = GUIDED_META[node.id];
      const original = originals[node.id] || {{}};
      let color = original.color || node.color;
      let borderWidth = original.borderWidth || 1;
      let size = original.size;
      let font = node.font || original.font;
      const isFocus = matchesFocus(meta, guidedFocus);
      const isStrategicSeed = strategicCombine && strategicSeedIds.has(String(node.id));
      const isStrategicContext = strategicCombine && strategicContextIds.has(String(node.id)) && !isStrategicSeed;
      if (isStrategicSeed) {{
        borderWidth = Math.max(Number(borderWidth || 1), 4);
      }} else if (isStrategicContext) {{
        color = mutedColor();
      }} else if (guidedFocus) {{
        if (isFocus) {{
          color = focusColor(meta, guidedFocus);
          borderWidth = 5;
          if (typeof size === 'number') size = Math.max(size + 4, Math.round(size * 1.35));
        }} else {{
          color = mutedColor();
        }}
      }} else if (meta && mode === 'category' && meta.primary_category &&
          Number(meta.category_confidence || 0) >= confidence()) {{
        color = {{background: CATEGORY_COLORS[meta.primary_category], border:'#f5f5f5'}};
      }} else if (meta && mode === 'polarity') {{
        color = {{background: POLARITY_COLORS[meta.polarity] || '#777', border:'#f5f5f5'}};
      }} else if (meta && mode === 'stance') {{
        color = {{background: STANCE_COLORS[meta.stance] || '#777', border:'#f5f5f5'}};
      }}
      updates.push({{id:node.id, color:color, borderWidth:borderWidth, size:size, font:font}});
    }});
    guidedNodes.update(updates);
  }}
  function resetGuided() {{
    guidedFocus = null;
    Object.values(activeFilters).forEach(values => values.clear());
    strategicCombine = null;
    strategicSeedIds.clear();
    strategicContextIds.clear();
    publishStrategicState();
    window.guidedNodeAllowed = null;
    window.dispatchEvent(new CustomEvent('guided-strategy-cleared'));
    document.querySelectorAll('[data-network-topic], #guidedPanel button').forEach(btn => btn.classList.remove('guided-active'));
    document.getElementById('guidedColorMode').value = 'original';
    document.getElementById('guidedConfidence').value = '0';
    document.getElementById('guidedConfidenceValue').textContent = '0.00';
    if (typeof rebuild === 'function') rebuild();
    guidedNetwork.unselectAll(); recolor(); guidedNetwork.fit({{animation:true}});
  }}
  window.guidedApplyStrategicPreset = function(preset) {{
    if (!resolveNetwork()) return;
    guidedFocus = null;
    Object.values(activeFilters).forEach(values => values.clear());
    (preset.strategic || []).forEach(value => activeFilters.strategic.add(value));
    (preset.polarity || []).forEach(value => activeFilters.polarity.add(value));
    (preset.stance || []).forEach(value => activeFilters.stance.add(value));
    strategicCombine = preset.combine || 'all';
    document.querySelectorAll('[data-network-topic], #guidedPanel button').forEach(button => button.classList.remove('guided-active'));
    document.getElementById('guidedColorMode').value = 'original';
    activeFilters.polarity.forEach(value => {{
      const button = document.querySelector('#guidedPanel [data-polarity="' + value + '"]');
      if (button) button.classList.add('guided-active');
    }});
    activeFilters.stance.forEach(value => {{
      const button = document.querySelector('#guidedPanel [data-stance="' + value + '"]');
      if (button) button.classList.add('guided-active');
    }});
    applyLayerFilters();
  }};
  setTimeout(initGuided, 0);
}})();
</script>
"""
    if "</body>" not in source:
        raise ValueError(f"HTML sin cierre body: {html_path}")
    html_path.write_text(source.replace("</body>", panel + "\n</body>", 1), encoding="utf-8")
