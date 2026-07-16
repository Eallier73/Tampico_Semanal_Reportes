#!/usr/bin/env python3
"""Utilidades compartidas para las redes SNA guiadas por diccionarios."""

from __future__ import annotations

import html
import json
import math
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

CATEGORY_COLORS = {
    "Agua": "#1f77b4",
    "Alumbrado": "#f2c744",
    "Americo": "#9467bd",
    "Basura": "#2ca02c",
    "Corrupcion": "#d62728",
    "Delitos": "#8c564b",
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


def normalize(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value).strip().lower())
    return "".join(ch for ch in text if not unicodedata.combining(ch))


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

    topics = pd.read_excel(topic_dictionary)
    required = {"Categoria", "Palabra", "Delta_PMI", "Confianza"}
    if not required.issubset(topics.columns):
        raise ValueError(
            f"El Excel debe contener {sorted(required)}; contiene {list(topics.columns)}"
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
    stance = stance_from_score(polarity_score, int(in_positive) + int(in_negative))
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
        "stance": stance,
        "stance_label": STANCE_LABELS[stance],
        "stance_score": polarity_score,
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
    for word, raw_weight in weighted_words:
        weight = max(0.0, float(raw_weight))
        if weight <= 0:
            continue
        annotation = annotate_word(str(word), lexicons)
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
    if positive_hits and negative_hits:
        polarity = "mixta"
    elif positive_hits:
        polarity = "positiva"
    elif negative_hits:
        polarity = "negativa"
    else:
        polarity = "neutral"
    polarity_total = positive_hits + negative_hits
    polarity_score = (
        (positive_hits - negative_hits) / polarity_total if polarity_total else 0.0
    )
    stance = stance_from_score(polarity_score, polarity_total)
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
        "stance": stance,
        "stance_label": STANCE_LABELS[stance],
        "stance_score": round(polarity_score, 4),
        "positive_hits": round(positive_hits, 2),
        "negative_hits": round(negative_hits, 2),
        "matched_weight": round(matched_weight, 2),
    }


def stance_from_score(score: float, evidence_weight: float) -> str:
    """Mapea polaridad agregada a apoyo/defensa vs critica/oposicion."""
    if evidence_weight <= 0:
        return "sin_postura"
    if score >= 0.20:
        return "apoyo_defensa"
    if score <= -0.20:
        return "critica_oposicion"
    return "mixta_disputa"


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
    for node_id, item in annotations.items():
        category_counts.update(cat["name"] for cat in item.get("categories", []))
        polarity_counts[item.get("polarity", "neutral")] += 1
        stance_counts[item.get("stance", "sin_postura")] += 1
        rows.append({
            "node_id": node_id,
            "tipo_nodo": item.get("kind", ""),
            "etiqueta": item.get("label", ""),
            "categoria_principal": item.get("primary_category", ""),
            "confianza_categoria": item.get("category_confidence", 0.0),
            "categorias": " | ".join(cat["name"] for cat in item.get("categories", [])),
            "polaridad": item.get("polarity", "neutral"),
            "polaridad_score": item.get("polarity_score", 0.0),
            "postura": item.get("stance", "sin_postura"),
            "postura_etiqueta": item.get("stance_label", STANCE_LABELS["sin_postura"]),
            "postura_score": item.get("stance_score", 0.0),
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
        if not categories and polarity == "neutral":
            continue
        compact[node_id] = {
            "categories": categories,
            "primary_category": item.get("primary_category") or "",
            "category_confidence": item.get("category_confidence") or 0.0,
            "polarity": polarity,
            "polarity_score": item.get("polarity_score") or 0.0,
            "stance": item.get("stance") or "sin_postura",
            "stance_label": item.get("stance_label") or STANCE_LABELS["sin_postura"],
            "stance_score": item.get("stance_score") or item.get("polarity_score") or 0.0,
        }
    return compact


def inject_guided_layer(
    html_path: Path,
    annotations: dict[str, dict[str, Any]],
    category_colors: dict[str, str],
    title: str,
    panel_top_px: int = 12,
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
    panel = f"""
<style>
#guidedPanel {{ position:fixed; top:{panel_top_px}px; right:12px; z-index:2147483001;
  width:290px; max-height:calc(100vh - {panel_top_px + 12}px); overflow:auto; padding:12px;
  color:#eee; background:rgba(18,18,18,.96); border:1px solid #666;
  border-radius:6px; font:12px Arial,sans-serif; box-shadow:0 4px 18px #0008; }}
#guidedPanel h3 {{ margin:0 0 8px; font-size:14px; }}
#guidedPanel h4 {{ margin:10px 0 5px; padding-top:7px; border-top:1px solid #444;
  color:#bbb; font-size:10px; text-transform:uppercase; }}
#guidedPanel button, #guidedPanel select {{ background:#303030; color:#eee;
  border:1px solid #666; border-radius:3px; padding:4px 6px; cursor:pointer; }}
#guidedPanel button:hover {{ background:#454545; }}
#guidedPanel button.guided-active {{ outline:2px solid #f5f5f5; background:#505050; }}
#guidedPanel .guided-target {{ width:100%; display:flex; align-items:center;
  gap:6px; margin:3px 0; text-align:left; }}
#guidedPanel .guided-target span {{ width:11px; height:11px; border-radius:50%; flex:none; }}
#guidedPanel .guided-row {{ display:flex; gap:5px; margin:4px 0; }}
#guidedPanel .guided-row button {{ flex:1; }}
#guidedPanel select, #guidedPanel input[type=range] {{ width:100%; }}
#guidedStats {{ margin-top:7px; color:#bbb; line-height:1.35; }}
</style>
<div id="guidedPanel">
  <h3>{html.escape(title)}</h3>
  <label>Color de nodos
    <select id="guidedColorMode">
      <option value="original">Estructura original</option>
      <option value="category">Tema rastreado</option>
      <option value="polarity">Polaridad</option>
      <option value="stance">Postura discursiva</option>
    </select>
  </label>
  <h4>Temas rastreados</h4>
  {category_buttons}
  <label>Confianza minima: <b id="guidedConfidenceValue">0.00</b></label>
  <input id="guidedConfidence" type="range" min="0" max="100" value="0">
  <h4>Polaridad lexica</h4>
  <div class="guided-row">
    <button data-polarity="positiva">Positiva</button>
    <button data-polarity="negativa">Negativa</button>
    <button data-polarity="mixta">Mixta</button>
  </div>
  <h4>Postura discursiva</h4>
  <div class="guided-row">
    <button data-stance="apoyo_defensa">Apoyo/defensa</button>
    <button data-stance="critica_oposicion">Crítica/oposición</button>
    <button data-stance="mixta_disputa">Mixta/disputa</button>
  </div>
  <div class="guided-row"><button id="guidedReset">Restaurar vista</button></div>
  <div id="guidedStats">Preparando capa guiada...</div>
</div>
<script>
(function() {{
  const GUIDED_META = {annotations_json};
  const CATEGORY_COLORS = {colors_json};
  const POLARITY_COLORS = {polarity_json};
  const STANCE_COLORS = {stance_json};
  const originals = {{}};
  let guidedFocus = null;
  let guidedNodes = null;
  let guidedNetwork = null;
  function confidence() {{
    return parseInt(document.getElementById('guidedConfidence').value || '0', 10) / 100;
  }}
  function resolveNetwork() {{
    guidedNetwork = (typeof network !== 'undefined') ? network : window.network;
    guidedNodes = (typeof nodes !== 'undefined') ? nodes : window.nodes;
    if (!guidedNodes && guidedNetwork && guidedNetwork.body && guidedNetwork.body.data) {{
      guidedNodes = guidedNetwork.body.data.nodes;
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
    document.querySelectorAll('#guidedPanel [data-category]').forEach(btn =>
      btn.addEventListener('click', () => locate('category', btn.dataset.category)));
    document.querySelectorAll('#guidedPanel [data-polarity]').forEach(btn =>
      btn.addEventListener('click', () => locate('polarity', btn.dataset.polarity)));
    document.querySelectorAll('#guidedPanel [data-stance]').forEach(btn =>
      btn.addEventListener('click', () => locate('stance', btn.dataset.stance)));
    document.getElementById('guidedColorMode').addEventListener('change', recolor);
    document.getElementById('guidedConfidence').addEventListener('input', function() {{
      document.getElementById('guidedConfidenceValue').textContent = confidence().toFixed(2);
      recolor();
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
  function setActiveButton(kind, value) {{
    document.querySelectorAll('#guidedPanel button').forEach(btn => btn.classList.remove('guided-active'));
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
      let font = original.font;
      const isFocus = matchesFocus(meta, guidedFocus);
      if (guidedFocus) {{
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
    document.querySelectorAll('#guidedPanel button').forEach(btn => btn.classList.remove('guided-active'));
    document.getElementById('guidedColorMode').value = 'original';
    document.getElementById('guidedConfidence').value = '0';
    document.getElementById('guidedConfidenceValue').textContent = '0.00';
    guidedNetwork.unselectAll(); recolor(); guidedNetwork.fit({{animation:true}});
  }}
  setTimeout(initGuided, 0);
}})();
</script>
"""
    if "</body>" not in source:
        raise ValueError(f"HTML sin cierre body: {html_path}")
    html_path.write_text(source.replace("</body>", panel + "\n</body>", 1), encoding="utf-8")
