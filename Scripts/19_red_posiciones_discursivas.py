#!/usr/bin/env python3
"""
Red de posiciones discursivas por tema.

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
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.feature_extraction import DictVectorizer
from sklearn.feature_extraction.text import TfidfTransformer

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

TOKEN_RE = re.compile(r"[a-záéíóúüñ]{3,}", re.IGNORECASE)
DEFAULT_BASE = Path(__file__).resolve().parent.parent / "SNA" / "Resultados" / "historico"
DEFAULT_DATA = Path(__file__).resolve().parent.parent / "SNA" / "Datos" / "tampico_datos_tabulares_consolidados.csv"


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def load_topic_info(base: Path) -> dict[int, dict[str, Any]]:
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
                    if word:
                        words.append(word)
                info.setdefault(tid, {
                    "title": f"T{tid:02d}: " + ", ".join(words[:3]),
                    "summary": "Tema definido por sus palabras mas frecuentes.",
                    "words": [],
                })
                info[tid]["words"] = words[:40]

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
    mensajes = mensajes.merge(lemas, left_on="id", right_on="documento_id", how="left")
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
    return min(requested, max(2, n_accounts // 120), 4)


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
    out = rows.groupby("palabra")["conteo"].sum().sort_values(ascending=False).head(limit)
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
        }

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
    }


def hhi(values: list[float]) -> float:
    total = sum(values)
    if total <= 0:
        return 0.0
    shares = [v / total for v in values]
    return float(sum(s * s for s in shares))


def classify_name(words: list[str]) -> str:
    wset = set(words)
    core = ", ".join(words[:4])
    if wset & CRITIC_WORDS:
        return f"Critica/ataque: {core}"
    if wset & CLAIM_WORDS:
        return f"Reclamo/exigencia: {core}"
    if wset & SUPPORT_WORDS:
        return f"Apoyo/defensa: {core}"
    return f"Enfoque: {core}"


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
            name = classify_name([w.lower() for w in top_words])
            summary = make_summary(title, name, len(users_set), top_words, level)
            platforms = platform_breakdown(cuentas, users_set)

            positions.append({
                "posicion_id": pos_id,
                "tema_id": tema,
                "posicion_num": pos_num,
                "nombre": name,
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
        color = TEMA_COLORS[tema % len(TEMA_COLORS)]
        angle = idx * angle_step
        tx = round(math.cos(angle) * 300, 2)
        ty = round(math.sin(angle) * 250, 2)
        topic_coords[tema] = (tx, ty)
        node_id = f"T{tema:02d}"
        node = {
            "id": node_id,
            "label": f"T{tema:02d}",
            "title": info.get("title", f"T{tema:02d}"),
            "kind": "tema",
            "tema": tema,
            "shape": "dot",
            "color": {"background": color, "border": "#f2f2f2"},
            "font": {"size": 24, "color": "#ffffff", "face": "arial"},
            "size": scalar_size(float(topic_counts.loc[tema]), min_topic, max_topic, 34, 58),
            "x": tx,
            "y": ty,
        }
        nodes.append(node)
        meta[node_id] = {
            "kind": "tema",
            "title": info.get("title", f"T{tema:02d}"),
            "summary": info.get("summary", ""),
            "words": info.get("words", []),
            "n_cuentas": int(topic_counts.loc[tema]),
        }

    positions_per_theme = positions.groupby("tema_id")["posicion_id"].count().to_dict()
    for _, row in positions.iterrows():
        tema = int(row["tema_id"])
        color = TEMA_COLORS[tema % len(TEMA_COLORS)]
        pos_id = str(row["posicion_id"])
        border = LEVEL_COLORS.get(str(row["ibea_nivel"]), "#999")
        total_pos = max(1, int(positions_per_theme.get(tema, 1)))
        pos_angle = (2 * math.pi * (int(row["posicion_num"]) - 1) / total_pos) - (math.pi / 2)
        tx, ty = topic_coords.get(tema, (0.0, 0.0))
        px = round(tx + math.cos(pos_angle) * 105, 2)
        py = round(ty + math.sin(pos_angle) * 105, 2)
        position_coords[pos_id] = (px, py)
        nodes.append({
            "id": pos_id,
            "label": f"P{int(row['posicion_num'])}",
            "title": str(row["nombre"]),
            "kind": "posicion",
            "tema": tema,
            "ibea": str(row["ibea_nivel"]),
            "shape": "diamond",
            "color": {"background": color, "border": border, "highlight": {"background": color, "border": "#ffffff"}},
            "borderWidth": 3 if row["ibea_nivel"] == "alto" else 2,
            "font": {"size": 18, "color": "#ffffff", "face": "arial"},
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
            "resumen": str(row["resumen"]),
            "n_cuentas": int(row["n_cuentas"]),
            "n_msgs": int(row["n_msgs"]),
            "n_palabras_tema": int(row["n_palabras_tema"]),
            "top_words": str(row["top_words"]).split(", ") if str(row["top_words"]) else [],
            "top_subclusters": str(row["top_subclusters"]),
            "ibea_score": float(row["ibea_score"]),
            "ibea_nivel": str(row["ibea_nivel"]),
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

    word_node_ids: dict[str, str] = {}
    top_words = (
        position_words.sort_values(["posicion_id", "rank"])
        .groupby("posicion_id")
        .head(words_per_position)
    )
    max_word_count = max(1, int(top_words["conteo"].max())) if not top_words.empty else 1
    for _, row in top_words.iterrows():
        word = str(row["palabra"])
        if word not in word_node_ids:
            node_id = f"W{len(word_node_ids) + 1}"
            word_node_ids[word] = node_id
            pos_id = str(row["posicion_id"])
            px, py = position_coords.get(pos_id, (0.0, 0.0))
            rank = int(row["rank"])
            angle = (2 * math.pi * ((rank - 1) % max(1, words_per_position)) / max(1, words_per_position)) + math.pi
            radius = 110 + 6 * (rank % 3)
            nodes.append({
                "id": node_id,
                "label": word,
                "kind": "palabra",
                "tema": int(row["tema_id"]),
                "shape": "dot",
                "color": {"background": "#222222", "border": TEMA_COLORS[int(row["tema_id"]) % len(TEMA_COLORS)]},
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
        pos_id = str(row["posicion_id"])
        meta[word_node_ids[word]]["posiciones"].append({
            "posicion_id": pos_id,
            "tema_id": int(row["tema_id"]),
            "conteo": int(row["conteo"]),
            "rank": int(row["rank"]),
        })
        edge_id += 1
        edges.append({
            "id": f"e{edge_id}",
            "from": pos_id,
            "to": word_node_ids[word],
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
        for node in nodes if node.get("kind") == "posicion"
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
    topic_checks = "\n".join(
        f'<label class="topic-item" data-topic="{tid}" '
        f'data-volume="{topic_metrics[tid]["volume"]}" '
        f'data-centrality="{topic_metrics[tid]["centrality"]}" '
        f'data-connectivity="{topic_metrics[tid]["connectivity"]}">'
        f'<input type="checkbox" class="topic" data-topic="{tid}" checked> '
        f'<span class="sw" style="background:{TEMA_COLORS[tid % len(TEMA_COLORS)]}"></span>'
        f'T{tid:02d} · {html.escape(topic_info[tid].get("title", ""))}'
        f' <small class="topic-score"></small></label>'
        for tid in topics
    )

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
  #network {{ position:fixed; inset:0 360px 0 320px; background:#1d1d1d; }}
  #left, #right {{
    position:fixed; top:0; bottom:0; overflow:auto; z-index:5;
    background:#151515; border-color:#333; padding:12px; box-sizing:border-box;
  }}
  #left {{ left:0; width:320px; border-right:1px solid #333; }}
  #right {{ right:0; width:360px; border-left:1px solid #333; }}
  h1 {{ font-size:16px; margin:0 0 8px; }}
  h2 {{ font-size:12px; text-transform:uppercase; letter-spacing:.04em; color:#aaa; margin:14px 0 6px; }}
  label {{ display:block; font-size:12px; margin:5px 0; line-height:1.3; }}
  input[type="text"] {{ width:100%; box-sizing:border-box; padding:7px; background:#101010; color:#fff; border:1px solid #444; border-radius:4px; }}
  select {{ width:100%; box-sizing:border-box; padding:6px; background:#101010; color:#fff; border:1px solid #444; border-radius:4px; }}
  input[type="range"] {{ width:100%; }}
  button {{ background:#333; color:#fff; border:1px solid #555; border-radius:4px; padding:5px 8px; cursor:pointer; }}
  .grp {{ border:1px solid #333; border-radius:6px; padding:8px; margin-bottom:10px; background:#191919; }}
  .sw {{ display:inline-block; width:10px; height:10px; border-radius:2px; margin-right:5px; vertical-align:-1px; }}
  .topic-score {{ color:#888; }}
  .pill {{ display:inline-block; padding:2px 6px; margin:2px 3px 2px 0; border-radius:10px; background:#26384a; font-size:11px; }}
  .metric {{ display:grid; grid-template-columns:1fr auto; gap:6px; font-size:12px; border-bottom:1px solid #2b2b2b; padding:4px 0; }}
  .muted {{ color:#aaa; font-size:12px; line-height:1.35; }}
  .example {{ border-left:3px solid #555; padding:6px 8px; margin:8px 0; background:#101010; font-size:12px; line-height:1.35; }}
  .level-bajo {{ color:#cfcfcf; }}
  .level-medio {{ color:#ffcc33; }}
  .level-alto {{ color:#ff6b6b; }}
  #stats {{ position:fixed; left:332px; bottom:10px; z-index:8; background:#111; border:1px solid #444; border-radius:4px; padding:6px 8px; font-size:12px; }}
</style>
</head>
<body>
<aside id="left">
  <h1>Red {semana}: posiciones discursivas</h1>
  <div class="muted">Temas, posiciones, cuentas y palabras. El borde de cada posicion indica senal indiciaria: gris baja, amarillo media, rojo alta.</div>
  <div class="grp">
    <h2>Buscar</h2>
    <input id="search" type="text" placeholder="cuenta, palabra, posicion o tema...">
  </div>
  <div class="grp">
    <h2>Capas</h2>
    <label><input type="checkbox" id="showTema" checked> temas</label>
    <label><input type="checkbox" id="showPos" checked> posiciones</label>
    <label><input type="checkbox" id="showCuenta" checked> cuentas</label>
    <label><input type="checkbox" id="showPalabra" checked> palabras distintivas</label>
  </div>
  <div class="grp">
    <h2>Senal indiciaria</h2>
    <label><input type="checkbox" class="level" data-level="bajo" checked> baja</label>
    <label><input type="checkbox" class="level" data-level="medio" checked> media</label>
    <label><input type="checkbox" class="level" data-level="alto" checked> alta</label>
  </div>
  <div class="grp">
    <h2>Temas</h2>
    <label>Ordenar por</label>
    <select id="topicOrder">
      <option value="volume">Volumen (cuentas)</option>
      <option value="centrality">Centralidad (puentes)</option>
      <option value="connectivity">Conectividad (enlaces)</option>
      <option value="topic">Numero de tema</option>
    </select>
    <div id="topicList">{topic_checks}</div>
  </div>
  <div class="grp">
    <h2>Visual</h2>
    <label>Separacion <span id="springV">100%</span></label>
    <input id="spring" type="range" min="50" max="180" value="100">
    <button id="reset">Reorganizar</button>
    <button id="stopPhysics">Detener</button>
    <button id="fitNetwork">Encajar</button>
  </div>
</aside>
<main id="network"></main>
<aside id="right">
  <h1>Lectura</h1>
  <div id="detail" class="muted">Haz clic en un tema, posicion, cuenta o palabra para ver sus detalles.</div>
</aside>
<div id="stats"></div>
<script>
const RAW_NODES = {json.dumps(nodes, ensure_ascii=False)};
const RAW_EDGES = {json.dumps(edges, ensure_ascii=False)};
const META = {json.dumps(meta, ensure_ascii=False)};
const nodes = new vis.DataSet(RAW_NODES);
const edges = new vis.DataSet(RAW_EDGES);
const network = new vis.Network(document.getElementById('network'), {{nodes, edges}}, {{
  nodes: {{ borderWidth: 1, shadow: false }},
  edges: {{ smooth: {{type:'continuous'}}, font: {{size:0}} }},
  physics: {{
    enabled: false,
    solver: 'forceAtlas2Based',
    forceAtlas2Based: {{ gravitationalConstant: -95, centralGravity: 0.012, springLength: 170, springConstant: 0.06, damping: 0.45 }},
    stabilization: {{ enabled: false }}
  }},
  interaction: {{ hover:true, tooltipDelay:100, navigationButtons:true, keyboard:true }}
}});
window.network = network;

function sortTopicMenu() {{
  const mode = document.getElementById('topicOrder').value;
  const list = document.getElementById('topicList');
  const labels = Array.from(list.querySelectorAll('.topic-item'));
  labels.sort((a, b) => {{
    if (mode === 'topic') return Number(a.dataset.topic) - Number(b.dataset.topic);
    return Number(b.dataset[mode] || 0) - Number(a.dataset[mode] || 0) ||
      Number(a.dataset.topic) - Number(b.dataset.topic);
  }});
  labels.forEach(label => {{
    const score = label.querySelector('.topic-score');
    score.textContent = mode === 'topic' ? '' : `(${{label.dataset[mode] || 0}})`;
    list.appendChild(label);
  }});
}}
document.getElementById('topicOrder').addEventListener('change', sortTopicMenu);
sortTopicMenu();

function activeTopics() {{
  const s = {{}};
  document.querySelectorAll('.topic:checked').forEach(el => s[parseInt(el.dataset.topic)] = true);
  return s;
}}
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
function nodeVisible(n, topics, levels, flags) {{
  if (!flags[n.kind]) return false;
  if ((n.kind === 'tema' || n.kind === 'posicion') && n.tema !== undefined && !topics[n.tema]) return false;
  if (n.kind === 'posicion' && !levels[n.ibea]) return false;
  return true;
}}
function rebuild() {{
  const topics = activeTopics(), levels = activeLevels(), flags = layerFlags();
  const visible = {{}};
  const finalVisible = {{}};
  const connected = {{}};
  let counts = {{tema:0, posicion:0, cuenta:0, palabra:0, edges:0}};
  const nodeUpdates = [];
  const edgeUpdates = [];
  nodes.forEach(n => {{
    visible[n.id] = nodeVisible(n, topics, levels, flags);
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
    const v = !!finalVisible[e.from] && !!finalVisible[e.to];
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
    h += `<div class="metric"><span>cuentas mapeadas</span><b>${{m.n_cuentas}}</b></div>`;
    h += '<h2>Palabras guia</h2>' + (m.words || []).slice(0, 40).map(w => `<span class="pill">${{esc(w)}}</span>`).join('');
  }} else if (m.kind === 'posicion') {{
    h += `<h1>${{esc(id)}} · ${{esc(m.nombre)}}</h1>`;
    h += `<p>${{esc(m.resumen)}}</p>`;
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
  if (el.id !== 'search' && el.id !== 'spring') el.addEventListener('change', rebuild);
}});
let lastSeparation = 100;
document.getElementById('spring').addEventListener('input', e => {{
  const next = parseInt(e.target.value, 10);
  document.getElementById('springV').textContent = next + '%';
  const ratio = next / lastSeparation;
  const positions = network.getPositions();
  const ids = Object.keys(positions);
  if (ids.length && Number.isFinite(ratio) && ratio > 0) {{
    let cx = 0, cy = 0;
    ids.forEach(id => {{ cx += positions[id].x; cy += positions[id].y; }});
    cx /= ids.length; cy /= ids.length;
    ids.forEach(id => network.moveNode(
      id,
      cx + (positions[id].x - cx) * ratio,
      cy + (positions[id].y - cy) * ratio
    ));
  }}
  lastSeparation = next;
  network.setOptions({{physics: {{forceAtlas2Based: {{springLength: Math.round(60 + next * 0.8)}}}}}});
}});
document.getElementById('reset').addEventListener('click', () => {{
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
requestAnimationFrame(() => network.fit({{animation:false}}));
</script>
</body>
</html>
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-dir", type=Path, default=DEFAULT_BASE)
    ap.add_argument("--input-csv", type=Path, default=DEFAULT_DATA)
    ap.add_argument("--positions-per-topic", type=int, default=3)
    ap.add_argument("--min-account-words", type=int, default=5)
    ap.add_argument("--min-topic-words", type=int, default=3)
    ap.add_argument("--min-topic-pct", type=float, default=12.0)
    ap.add_argument("--max-vocab", type=int, default=90)
    ap.add_argument("--accounts-per-position", type=int, default=18)
    ap.add_argument("--words-per-position", type=int, default=12)
    args = ap.parse_args()

    base = args.base_dir
    out_dir = base / "clusters" / "red_posiciones"
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[1/5] Cargando insumos de {base}")
    mensajes, cuentas, palx, cxt = read_inputs(base, args.input_csv)
    topic_info = load_topic_info(base)

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
    positions.to_csv(out_dir / "posiciones_discursivas.csv", index=False)
    memberships.to_csv(out_dir / "cuentas_posiciones.csv", index=False)
    position_words.to_csv(out_dir / "palabras_posiciones.csv", index=False)
    examples.to_csv(out_dir / "ejemplos_posiciones.csv", index=False)

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
    html_out = build_html(nodes, edges, meta, topic_info, base, "Tampico historico")
    (out_dir / "red_tampico_posiciones.html").write_text(html_out, encoding="utf-8")

    metrics = {
        "corpus": "historico consolidado de Tampico",
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
    (out_dir / "metricas_posiciones.json").write_text(
        json.dumps(metrics, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print("[5/5] OK")
    for path in [
        out_dir / "red_tampico_posiciones.html",
        out_dir / "posiciones_discursivas.csv",
        out_dir / "cuentas_posiciones.csv",
        out_dir / "palabras_posiciones.csv",
        out_dir / "ejemplos_posiciones.csv",
        out_dir / "metricas_posiciones.json",
    ]:
        print(f"      - {path} ({path.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
