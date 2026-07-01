#!/usr/bin/env python3
"""
SNA guiado: variante independiente de la red completa multi-capa.

Superpone temas rastreados y polaridad lexica sin modificar las salidas de
12c_red_completa.py. Escribe exclusivamente en clusters/red_guiada/.

Carga el output de Fase 2 (LDA) y Fase 2.5 (subclusters Louvain), arma una
red unica con TODAS las palabras como nodos y tres tipos de aristas:

  - intra_sub      : dentro de un mismo Louvain subcluster
                     ventana=3 (ventana intra LDA),   umbral peso >= 5
  - intra_cluster  : mismo tema LDA, distinto subcluster
                     ventana=3,                      umbral peso >= 30
  - extra          : entre temas LDA distintos
                     ventana=20 (ventana extra LDA), umbral peso >= 500

Calcula metricas:
  - Por nodo: grado por tipo, participation_coefficient (Guimera-Amaral),
    z_within_module_degree, betweenness, pagerank, clustering_coefficient.
    Asigna rol: hub_endogamico | broker | periferico | conector_provincial.
  - Por subcluster (Louvain): n, densidad, grado medio, asortatividad de peso.
  - Por tema (LDA): fuerza_intra, fuerza_extra, ratio_endogamia,
    broker_index, hub_local, aislamiento, eigenvector_interno, veredicto.
  - Globales: modularidad, componentes conexas, diametro, clustering global,
    asortatividad.

Outputs en SNA/Resultados/historico/clusters/red_completa/:
  - nodos_metricas.csv
  - aristas_clasificadas.csv
  - metricas_subcluster.csv
  - metricas_tema.csv
  - metricas_rol_nodo.csv
  - brokers_top.csv
  - sospechosos_por_tema.csv
  - metricas_red.json
  - red_w24.html   (pyvis interactivo)

Uso:
  .venv/bin/python Scripts/12c_red_completa.py
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import networkx as nx
import pandas as pd

from sna_guiada_common import (
    DEFAULT_NEGATIVE_DICTIONARY,
    DEFAULT_POSITIVE_DICTIONARY,
    DEFAULT_TOPIC_DICTIONARY,
    annotate_words,
    inject_guided_layer,
    load_lexicons,
    write_annotation_outputs,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CLUSTERS = REPO_ROOT / "SNA" / "Resultados" / "historico" / "clusters"

# Paleta consistente con el resto del pipeline
CMAP_TAB10 = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
    "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
    "#aec7e8", "#ffbb78", "#98df8a", "#ff9896", "#c5b0d5",
    "#c49c94", "#f7b6d2", "#c7c7c7", "#dbdb8d", "#9edae5",
]

TOPIC_READINGS = {
    0: {
        "title": "Presidenta, CNTE y buscadoras",
        "summary": (
            "Conversación centrada en Sheinbaum, la presidenta y reclamos de "
            "madres buscadoras, CNTE y justicia; mezcla apoyo político, dolor "
            "familiar, desaparecidos y polarización partidista PRI-PAN-AMLO."
        ),
    },
    1: {
        "title": "Seguridad, familia y educación",
        "summary": (
            "Tema de malestar social sobre pobreza, robos, gritos y familia; "
            "conecta exigencias de seguridad, educación, policía, niñez, "
            "liderazgo político y sensación de abandono gubernamental."
        ),
    },
    2: {
        "title": "Estadio Azteca y negocio futbolero",
        "summary": (
            "Discusión sobre Salinas Pliego, Estadio Azteca y posible negocio "
            "deportivo; aparecen venta, patria, fútbol, compras, inauguración, "
            "sindicatos, saludos y críticas a empresarios o rateros."
        ),
    },
    3: {
        "title": "Justicia, protección y violencia cotidiana",
        "summary": (
            "Núcleo emocional de reclamos por protección, pruebas, desastre, "
            "libertad e inocencia; mezcla enojo cotidiano, deseos de regresar, "
            "limpiar, vengar y evitar asesinatos o abusos."
        ),
    },
    4: {
        "title": "Trump, derecha y violencia política",
        "summary": (
            "Conversación alrededor de Trump, Estados Unidos, Obrador, ley y "
            "violencia; incluye videos, derecha, Peña, traición, llamados de "
            "ayuda y discusión sobre libertad nacional."
        ),
    },
    5: {
        "title": "Maestros, comercio y corrupción",
        "summary": (
            "Tema de indignación con maestros, comerciantes y trabajadores "
            "afectados; aparecen corrupción, madres buscadoras, investigación, "
            "vandalismo, ataques, mentiras, destrucción y valoración de actores "
            "sociales valientes."
        ),
    },
    6: {
        "title": "Maestros, criminalización y represión",
        "summary": (
            "Conversación centrada en maestros, PRIAN, cárcel, criminales, "
            "cartel y represión; sugiere disputa sobre protestas escolares, "
            "porros, pagos, detenciones y cambios exigidos al sistema."
        ),
    },
    7: {
        "title": "Bienestar, recursos y reforma federal",
        "summary": (
            "Debate de defensa y entendimiento sobre recursos, extranjeros, "
            "bienestar, pensiones, reforma y nivel federal; expresa disputa por "
            "intereses, uso político, complicidad y atención institucional."
        ),
    },
    8: {
        "title": "México, pueblo y mundial",
        "summary": (
            "Tema identitario sobre México, pueblo y país frente al mundo y el "
            "Mundial; mezcla orgullo, vergüenza, evento FIFA, partido, paz, "
            "felicidad y merecimiento nacional."
        ),
    },
    9: {
        "title": "Ciudad, vida pública y situación social",
        "summary": (
            "Conversación amplia sobre personas, día a día, ciudad y realidad; "
            "incluye vida pública, medios, promesas, dinero, Zócalo, situación "
            "social, tristeza, preguntas y cierre de procesos."
        ),
    },
    10: {
        "title": "Gobierno, Morena, narco y corrupción",
        "summary": (
            "Tema político duro sobre gobierno, Morena, narco, impuestos y "
            "corrupción; relaciona presidente, ciudadanía, derechos, mexicanos, "
            "pérdida, protestas, autoridad y percepción de deterioro público."
        ),
    },
    11: {
        "title": "CDMX, Brugada y crimen organizado",
        "summary": (
            "Conversación local sobre CDMX, Clara Brugada, crimen organizado y "
            "mentiras; combina juicio negativo, actuación de gobierno, "
            "felicidad, voces ciudadanas, vallas, jefatura y violencia."
        ),
    },
    12: {
        "title": "Gente, dinero y manifestación",
        "summary": (
            "Tema transversal sobre gente, deberes, dinero y problemas; incluye "
            "dejar, seguir, creer, salir, miedo, culpa, casa, manifestación y "
            "expectativas incumplidas en la vida cotidiana."
        ),
    },
}

# Las lecturas anteriores pertenecen a CDMX. En Tampico los nombres y
# descripciones se generan desde los terminos de cada corrida.
TOPIC_READINGS = {}


def _hex_a_rgb(h: str) -> tuple[int, int, int]:
    h = h.lstrip("#")
    return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))  # type: ignore[return-value]


def _mezclar(c1: str, c2: str, t: float) -> str:
    """Mezcla dos hex por fraccion t (0=c1, 1=c2)."""
    r1, g1, b1 = _hex_a_rgb(c1)
    r2, g2, b2 = _hex_a_rgb(c2)
    r = int(r1 + t * (r2 - r1))
    g = int(g1 + t * (g2 - g1))
    b = int(b1 + t * (b2 - b1))
    return f"#{r:02x}{g:02x}{b:02x}"


def _limpiar_termino(raw: Any) -> str:
    termino = str(raw).strip()
    if "(" in termino:
        termino = termino.split("(")[0].strip()
    return termino


def _terminos_desde_texto(raw: Any, max_terms: int = 60) -> list[str]:
    return [t for t in (_limpiar_termino(p) for p in str(raw).split(",")) if t][:max_terms]


def build_topic_info(tdf: pd.DataFrame | None) -> dict[str, dict[str, Any]]:
    """Prepara las lecturas que se muestran en filtros y tarjetas."""
    topic_info: dict[str, dict[str, Any]] = {}
    terminos: dict[int, list[str]] = {}

    if tdf is not None and not tdf.empty:
        if {"tema_id", "top_20_terminos"}.issubset(tdf.columns):
            for _, row in tdf.iterrows():
                terminos[int(row["tema_id"])] = _terminos_desde_texto(row["top_20_terminos"])
        else:
            col_pal = "palabra" if "palabra" in tdf.columns else (
                "termino" if "termino" in tdf.columns else None
            )
            col_t = "tema_id" if "tema_id" in tdf.columns else (
                "topic" if "topic" in tdf.columns else None
            )
            col_peso = "peso" if "peso" in tdf.columns else None
            if col_pal and col_t:
                for tid, grp in tdf.groupby(col_t):
                    if col_peso:
                        grp = grp.sort_values(col_peso, ascending=False)
                    terminos[int(tid)] = [_limpiar_termino(v) for v in grp[col_pal].head(20)]

    for tid, reading in TOPIC_READINGS.items():
        words = terminos.get(tid, [])
        topic_info[str(tid)] = {
            "title": reading["title"],
            "summary": reading["summary"],
            "words": words,
        }

    for tid, words in terminos.items():
        principales = words[:8]
        topic_info.setdefault(str(tid), {
            "title": ", ".join(principales[:4]).capitalize() or f"Tema {tid}",
            "summary": (
                "Conversacion caracterizada por " + ", ".join(principales) + "."
                if principales else "Tema sin suficientes palabras caracteristicas."
            ),
            "words": words,
        })

    return topic_info


def add_topic_words_from_nodes(
    topic_info: dict[str, dict[str, Any]],
    nodos_df: pd.DataFrame,
    limit: int = 60,
) -> None:
    """Completa las listas de lectura con las palabras mas conectadas de cada tema."""
    if nodos_df.empty or not {"palabra", "tema_id", "grado_total", "pagerank"}.issubset(nodos_df.columns):
        return

    for tid, grp in nodos_df.sort_values(
        ["tema_id", "grado_total", "pagerank"],
        ascending=[True, False, False],
    ).groupby("tema_id"):
        key = str(int(tid))
        existing = list(topic_info.get(key, {}).get("words", []))
        seen = {str(w).lower() for w in existing}
        words = existing[:]
        for palabra in grp["palabra"].astype(str):
            k = palabra.lower()
            if k in seen:
                continue
            words.append(palabra)
            seen.add(k)
            if len(words) >= limit:
                break
        topic_info.setdefault(key, {
            "title": "Tema por palabras clave",
            "summary": "Tema definido por sus palabras más frecuentes y sus conexiones dentro de la red.",
        })
        topic_info[key]["words"] = words[:limit]


# ------------------------------------------------------------------
# Carga de datos
# ------------------------------------------------------------------

def cargar_datos(sna_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Retorna (lda_asignacion, temas_terminos, intra_full, extra_full).
    """
    lda_asig = pd.read_csv(sna_dir / "lda_asignacion.csv")
    temas = pd.read_csv(sna_dir / "temas_terminos.csv")

    # Intra y extra son directorios con archivos tema_XX.csv / tema_XX.csv
    intra_full = pd.concat(
        [
            pd.read_csv(p).assign(tema_id=int(p.stem.split("_")[1]))
            for p in (sna_dir / "intracluster").glob("tema_*.csv")
        ],
        ignore_index=True,
    ) if (sna_dir / "intracluster").exists() else pd.DataFrame(
        columns=["source", "target", "weight", "tema_id"]
    )

    extra_full = pd.concat(
        [
            pd.read_csv(p).assign(tema_id=int(p.stem.split("_")[1]))
            for p in (sna_dir / "extracluster").glob("tema_*.csv")
        ],
        ignore_index=True,
    ) if (sna_dir / "extracluster").exists() else pd.DataFrame(
        columns=["termino_interno", "termino_externo", "weight", "tema_id"]
    )

    return lda_asig, temas, intra_full, extra_full


def cargar_subclusters(sna_dir: Path) -> pd.DataFrame:
    """Carga subclusters_palabras.csv si existe; si no, devuelve DataFrame vacio."""
    path = sna_dir / "subclusters" / "subclusters_palabras.csv"
    if not path.exists():
        return pd.DataFrame(columns=["tema_id", "sub_id", "palabra", "peso_lda", "rank_in_sub"])
    return pd.read_csv(path)


# ------------------------------------------------------------------
# Asignacion de sub_id a palabras. -1 significa sin subcluster.
# ------------------------------------------------------------------

def _asignar_subids(
    lda_asig: pd.DataFrame, sub_palabras: pd.DataFrame
) -> dict[str, tuple[int, int]]:
    """Devuelve dict {palabra: (tema_id, sub_id)}."""
    out: dict[str, tuple[int, int]] = {}
    if not sub_palabras.empty:
        for _, row in sub_palabras.iterrows():
            out[str(row["palabra"])] = (int(row["tema_id"]), int(row["sub_id"]))
    # -1 no colisiona con el subcluster Louvain 0.
    for _, row in lda_asig.iterrows():
        pal = str(row["termino"])
        if pal not in out:
            out[pal] = (int(row["tema_id"]), -1)
    return out


# ------------------------------------------------------------------
# Construccion del grafo
# ------------------------------------------------------------------

def construir_grafo(
    lda_asig: pd.DataFrame,
    intra_full: pd.DataFrame,
    extra_full: pd.DataFrame,
    pal2subs: dict[str, tuple[int, int]],
    umbral_intra_sub: float,
    umbral_intra_cluster: float,
    umbral_extra: float,
) -> nx.Graph:
    """Construye el grafo no-dirigido con atributo `tipo` por arista."""
    G = nx.Graph()

    # Mapeo palabra -> tema_id (autoridad LDA)
    pal2tema = dict(zip(
        lda_asig["termino"].astype(str),
        lda_asig["tema_id"].astype(int),
    ))

    palabras_validas = set(pal2tema.keys())

    # ----- intra (LDA): source/target/weight/tema_id -----
    # intra_sub: ambos en mismo subcluster (pal2subs iguales)
    # intra_cluster: mismo tema, distinto sub
    aristas_intra_sub = 0
    aristas_intra_cluster = 0
    for _, row in intra_full.iterrows():
        a = str(row["source"])
        b = str(row["target"])
        w = float(row["weight"] or 0)
        if a not in palabras_validas or b not in palabras_validas:
            continue
        if a == b:
            continue
        ta, sa = pal2subs.get(a, (pal2tema[a], -1))
        tb, sb = pal2subs.get(b, (pal2tema[b], -1))
        if ta != pal2tema[a] or tb != pal2tema[b]:
            # Sub-cluster vino de otra corrida con tema_ids distintos:
            # caemos al modo solo-tema
            sa, sb = -1, -1

        if ta != tb:
            # cross-topic: no deberia estar en intra_full; defensivo
            continue

        if w >= umbral_intra_sub and sa == sb and sa >= 0:
            tipo = "intra_sub"
            aristas_intra_sub += 1
        elif w >= umbral_intra_cluster:
            tipo = "intra_cluster"
            aristas_intra_cluster += 1
        else:
            continue

        if G.has_edge(a, b):
            # si ya existe, mantenemos la de mayor peso y agregamos tipo
            if w > G[a][b]["weight"]:
                G[a][b]["weight"] = w
                G[a][b]["tipo"] = tipo
            # si hay multiples tipos, lo dejamos como el mas restrictivo
        else:
            G.add_edge(a, b, weight=w, tipo=tipo)

    # ----- extra: termino_interno/termino_externo/weight/tema_id (lado interno) -----
    aristas_extra = 0
    for _, row in extra_full.iterrows():
        a = str(row["termino_interno"])
        b = str(row["termino_externo"])
        w = float(row["weight"] or 0)
        if a not in palabras_validas or b not in palabras_validas:
            continue
        if a == b:
            continue
        if w < umbral_extra:
            continue
        ta = pal2tema[a]
        tb = pal2tema[b]
        if ta == tb:
            continue  # cross-topic estricto
        if G.has_edge(a, b):
            if w > G[a][b]["weight"]:
                G[a][b]["weight"] = w
                G[a][b]["tipo"] = "extra"
        else:
            G.add_edge(a, b, weight=w, tipo="extra")
        aristas_extra += 1

    # ----- Atributos de nodo -----
    for pal in G.nodes():
        tema_id, sub_id = pal2subs.get(pal, (pal2tema.get(pal, 0), -1))
        G.nodes[pal]["tema_id"] = tema_id
        G.nodes[pal]["sub_id"] = sub_id
        G.nodes[pal]["color_tema"] = CMAP_TAB10[tema_id % len(CMAP_TAB10)]
        peso_lda = float(
            lda_asig.loc[lda_asig["termino"] == pal, "peso"].iloc[0]
            if (lda_asig["termino"] == pal).any() else 0.0
        )
        G.nodes[pal]["peso_lda"] = peso_lda

    print(f"  aristas intra_sub:      {aristas_intra_sub}")
    print(f"  aristas intra_cluster:  {aristas_intra_cluster}")
    print(f"  aristas extra:          {aristas_extra}")
    return G


# ------------------------------------------------------------------
# Metricas
# ------------------------------------------------------------------

def _participation_coefficient(G: nx.Graph, partition: dict[str, int]) -> dict[str, float]:
    """
    Participation coefficient (Guimera & Amaral 2005).
    Para nodo i: P_i = 1 - sum_m (k_im/k_i)^2
    donde k_im = grado de i hacia modulo m.
    Rango [0, 1]. P cercano a 0 = endogamo, cercano a 1 = broker.
    """
    # grados totales
    grados = {n: G.degree(n) for n in G.nodes()}
    # mapeo nodo -> comunidad
    out: dict[str, float] = {}
    for n in G.nodes():
        ki = grados[n]
        if ki == 0:
            out[n] = 0.0
            continue
        # contar aristas hacia cada comunidad
        conteo: dict[int, int] = defaultdict(int)
        for v in G.neighbors(n):
            conteo[partition[v]] += 1
        # si todos los vecinos estan en la propia comunidad
        # suma = (ki_m / ki)^2 = 1 => P = 0
        suma = sum((c / ki) ** 2 for c in conteo.values())
        out[n] = 1.0 - suma
    return out


def _z_within_module_degree(
    G: nx.Graph, partition: dict[str, int]
) -> dict[str, float]:
    """
    z-score del grado intra-comunidad, normalizado.
    z_i = (k_i_m - mean_k_m) / std_k_m
    """
    mod_grados: dict[int, list[int]] = defaultdict(list)
    for n in G.nodes():
        m = partition[n]
        # contar aristas hacia la propia comunidad
        ki_m = sum(1 for v in G.neighbors(n) if partition[v] == m)
        mod_grados[m].append(ki_m)

    # media y std por modulo
    stats = {}
    for m, vals in mod_grados.items():
        if len(vals) < 2:
            stats[m] = (vals[0] if vals else 0.0, 0.0)
            continue
        mu = sum(vals) / len(vals)
        var = sum((v - mu) ** 2 for v in vals) / len(vals)
        stats[m] = (mu, math.sqrt(var) if var > 0 else 0.0)

    out: dict[str, float] = {}
    for n in G.nodes():
        m = partition[n]
        ki_m = sum(1 for v in G.neighbors(n) if partition[v] == m)
        mu, sd = stats[m]
        out[n] = (ki_m - mu) / sd if sd > 0 else 0.0
    return out


def _clasificar_rol(z: float, p: float) -> str:
    """Reglas Guimera-Amaral (z>2.5 hub, P<0.05 endogamia, etc.)."""
    # Umbrales sugeridos por literatura; los adaptamos al problema
    if z >= 2.5 and p < 0.30:
        return "hub_endogamico"
    if z >= 2.5 and p >= 0.30:
        return "conector_provincial"
    if z < 2.5 and p >= 0.50:
        return "broker"
    return "periferico"


def calcular_metricas(G: nx.Graph) -> dict[str, pd.DataFrame]:
    """Calcula todas las metricas y devuelve DataFrames."""

    nodos = list(G.nodes())
    print(f"  Calculando metricas para {len(nodos)} nodos y {G.number_of_edges()} aristas...")

    # Grado por tipo
    grado_total = {n: G.degree(n) for n in nodos}
    grado_intra_sub = defaultdict(int)
    grado_intra_cluster = defaultdict(int)
    grado_extra = defaultdict(int)
    peso_intra_sub = defaultdict(float)
    peso_intra_cluster = defaultdict(float)
    peso_extra = defaultdict(float)

    for u, v, data in G.edges(data=True):
        w = float(data.get("weight", 0))
        t = data.get("tipo", "intra_sub")
        # sumar a ambos extremos
        if t == "intra_sub":
            grado_intra_sub[u] += 1
            grado_intra_sub[v] += 1
            peso_intra_sub[u] += w
            peso_intra_sub[v] += w
        elif t == "intra_cluster":
            grado_intra_cluster[u] += 1
            grado_intra_cluster[v] += 1
            peso_intra_cluster[u] += w
            peso_intra_cluster[v] += w
        else:  # extra
            grado_extra[u] += 1
            grado_extra[v] += 1
            peso_extra[u] += w
            peso_extra[v] += w

    # Particion = subcluster (mismo Louvain). Para particion global usamos
    # (tema_id, sub_id) codificado como entero.
    partition: dict[str, int] = {}
    for n in nodos:
        t = G.nodes[n]["tema_id"]
        s = G.nodes[n]["sub_id"]
        partition[n] = t * 10000 + (s if s >= 0 else 9999)

    n_comunidades = len(set(partition.values()))
    print(f"  Comunidades (subclusters presentes): {n_comunidades}")

    # Pagerank
    try:
        pr = nx.pagerank(G, weight="weight")
    except Exception:
        pr = {n: 0.0 for n in nodos}

    # Clustering
    clustering = nx.clustering(G)

    # Betweenness (puede tardar). Limitar a subconjunto si >1000 nodos.
    if len(nodos) <= 1500:
        print("  Calculando betweenness centrality (puede tardar)...")
        nx.set_edge_attributes(
            G,
            {(u, v): 1.0 / max(float(d.get("weight", 0)), 1e-12) for u, v, d in G.edges(data=True)},
            "distance",
        )
        btw = nx.betweenness_centrality(G, weight="distance", normalized=True)
    else:
        print("  [SKIP] betweenness: demasiados nodos")
        btw = {n: 0.0 for n in nodos}

    # Participation + z
    print("  Calculando participation coefficient y z-within-degree...")
    p_coef = _participation_coefficient(G, partition)
    z_wmd = _z_within_module_degree(G, partition)

    # Roles
    roles = {n: _clasificar_rol(z_wmd[n], p_coef[n]) for n in nodos}

    # Eigenvector
    try:
        eig = nx.eigenvector_centrality_numpy(G, weight="weight")
    except Exception:
        eig = {n: 0.0 for n in nodos}

    # DataFrame de nodos
    nodos_df = pd.DataFrame({
        "palabra": nodos,
        "tema_id": [G.nodes[n]["tema_id"] for n in nodos],
        "sub_id": [G.nodes[n]["sub_id"] for n in nodos],
        "color_tema": [G.nodes[n]["color_tema"] for n in nodos],
        "peso_lda": [G.nodes[n]["peso_lda"] for n in nodos],
        "grado_total": [grado_total[n] for n in nodos],
        "grado_intra_sub": [grado_intra_sub[n] for n in nodos],
        "grado_intra_cluster": [grado_intra_cluster[n] for n in nodos],
        "grado_extra": [grado_extra[n] for n in nodos],
        "peso_intra_sub": [round(peso_intra_sub[n], 1) for n in nodos],
        "peso_intra_cluster": [round(peso_intra_cluster[n], 1) for n in nodos],
        "peso_extra": [round(peso_extra[n], 1) for n in nodos],
        "pagerank": [round(pr[n], 6) for n in nodos],
        "betweenness": [round(btw[n], 6) for n in nodos],
        "clustering_coef": [round(clustering[n], 4) for n in nodos],
        "eigenvector": [round(eig[n], 6) for n in nodos],
        "participation": [round(p_coef[n], 4) for n in nodos],
        "z_within_degree": [round(z_wmd[n], 4) for n in nodos],
        "rol": [roles[n] for n in nodos],
    })

    # Aristas clasificadas
    aristas = []
    for u, v, d in G.edges(data=True):
        aristas.append({
            "source": u,
            "target": v,
            "weight": float(d.get("weight", 0)),
            "tipo": d.get("tipo", "intra_sub"),
            "tema_source": G.nodes[u]["tema_id"],
            "tema_target": G.nodes[v]["tema_id"],
            "sub_source": G.nodes[u]["sub_id"],
            "sub_target": G.nodes[v]["sub_id"],
        })
    aristas_df = pd.DataFrame(aristas)

    # Metricas por subcluster
    subs_presentes = sorted(set(partition.values()))
    sub_rows = []
    for cod in subs_presentes:
        miembros = [n for n in nodos if partition[n] == cod]
        subG = G.subgraph(miembros)
        if subG.number_of_nodes() == 0:
            continue
        tema_id = G.nodes[miembros[0]]["tema_id"]
        sub_id = G.nodes[miembros[0]]["sub_id"]
        aristas_internas = subG.number_of_edges()
        densidad = nx.density(subG)
        # grado medio
        grados = [d for _, d in subG.degree()]
        g_medio = sum(grados) / len(grados) if grados else 0
        g_max = max(grados) if grados else 0
        # asortatividad de peso
        try:
            asort_peso = nx.degree_assortativity_coefficient(subG, weight="weight")
        except Exception:
            asort_peso = 0.0
        sub_rows.append({
            "tema_id": tema_id,
            "sub_id": sub_id,
            "comunidad_cod": cod,
            "n_palabras": len(miembros),
            "n_aristas": aristas_internas,
            "densidad": round(densidad, 4),
            "grado_medio": round(g_medio, 2),
            "grado_max": g_max,
            "asortatividad_peso": round(asort_peso, 3),
            "color": CMAP_TAB10[tema_id % len(CMAP_TAB10)],
        })
    subs_df = pd.DataFrame(sub_rows)

    # Metricas por tema
    temas_presentes = sorted({G.nodes[n]["tema_id"] for n in nodos})
    tema_rows = []
    for tid in temas_presentes:
        palabras_tema = [n for n in nodos if G.nodes[n]["tema_id"] == tid]
        # fuerza intra = suma pesos de aristas intra_sub + intra_cluster con ambos extremos en el tema
        fuerza_intra = 0.0
        fuerza_extra = 0.0
        for u, v, d in G.edges(data=True):
            if G.nodes[u]["tema_id"] == tid and G.nodes[v]["tema_id"] == tid:
                if d.get("tipo") != "extra":
                    fuerza_intra += float(d.get("weight", 0))
            elif G.nodes[u]["tema_id"] == tid or G.nodes[v]["tema_id"] == tid:
                if d.get("tipo") == "extra":
                    fuerza_extra += float(d.get("weight", 0))
        total = fuerza_intra + fuerza_extra
        ratio = fuerza_intra / total if total > 0 else 0.0

        # broker_index = mean(participation) de palabras del tema
        ps = [p_coef[n] for n in palabras_tema]
        broker_index = sum(ps) / len(ps) if ps else 0.0

        # hub_local = mean(z) de palabras del tema
        zs = [z_wmd[n] for n in palabras_tema]
        hub_local = sum(zs) / len(zs) if zs else 0.0

        # eigenvector_interno = mean(eigenvector) de palabras del tema
        es = [eig[n] for n in palabras_tema]
        eig_int = sum(es) / len(es) if es else 0.0

        # aislamiento: comparamos fuerza_extra con la maxima global
        tema_rows.append({
            "tema_id": tid,
            "n_palabras": len(palabras_tema),
            "fuerza_intra": round(fuerza_intra, 1),
            "fuerza_extra": round(fuerza_extra, 1),
            "fuerza_total": round(total, 1),
            "ratio_endogamia": round(ratio, 4),
            "broker_index": round(broker_index, 4),
            "hub_local": round(hub_local, 4),
            "eigenvector_interno": round(eig_int, 6),
        })
    temas_df = pd.DataFrame(tema_rows)

    # Aislamiento (post-process: relativo al max de fuerza_extra)
    if not temas_df.empty:
        max_extra = temas_df["fuerza_extra"].max() or 1
        temas_df["aislamiento"] = temas_df["fuerza_extra"].apply(
            lambda x: round(1 - (x / max_extra), 4)
        )
        # veredicto
        def _veredicto(row):
            r = row["ratio_endogamia"]
            h = row["hub_local"]
            a = row["aislamiento"]
            if r > 0.9 and h > 0:
                return "camara_de_eco"
            if r > 0.9 and h <= 0:
                return "burbuja_vacia"
            if r < 0.1:
                return "disperso_sin_sustancia"
            if a > 0.95:
                return "huerfano"
            if row["broker_index"] > 0.5 and 0.2 < r < 0.8:
                return "conector_legitimo"
            return "balanceado"
        temas_df["veredicto"] = temas_df.apply(_veredicto, axis=1)

    # Brokers top
    brokers_top = nodos_df.nlargest(30, "participation")[
        ["palabra", "tema_id", "sub_id", "participation", "z_within_degree",
         "grado_total", "rol"]
    ]

    # Metricas globales
    print("  Calculando metricas globales...")
    componentes = list(nx.connected_components(G))
    n_componentes = len(componentes)
    tamano_componentes = sorted([len(c) for c in componentes], reverse=True)

    try:
        modularidad = nx.algorithms.community.modularity(
            G,
            [{n for n in nodos if partition[n] == c} for c in set(partition.values())],
        )
    except Exception:
        modularidad = 0.0

    clustering_global = nx.average_clustering(G)
    try:
        diametro = nx.diameter(G.subgraph(max(componentes, key=len)))
    except Exception:
        diametro = 0

    try:
        camino_promedio = nx.average_shortest_path_length(G.subgraph(max(componentes, key=len)))
    except Exception:
        camino_promedio = 0.0

    try:
        asortatividad = nx.degree_assortativity_coefficient(G, weight="weight")
    except Exception:
        asortatividad = 0.0

    # Conteo por tipo
    conteo_tipos = aristas_df["tipo"].value_counts().to_dict()

    metricas_red = {
        "n_nodos": len(nodos),
        "n_aristas_total": int(len(aristas_df)),
        "aristas_por_tipo": conteo_tipos,
        "n_componentes_conexas": n_componentes,
        "tamano_componentes_top10": tamano_componentes[:10],
        "modularidad": round(modularidad, 4),
        "clustering_global": round(clustering_global, 4),
        "diametro_gigante": diametro,
        "camino_promedio_gigante": round(camino_promedio, 4),
        "asortatividad_peso": round(asortatividad, 4),
        "n_comunidades_sub": n_comunidades,
        "n_temas": len(temas_presentes),
    }

    return {
        "nodos": nodos_df,
        "aristas": aristas_df,
        "subs": subs_df,
        "temas": temas_df,
        "brokers_top": brokers_top,
        "metricas_red": metricas_red,
    }


# ------------------------------------------------------------------
# Render pyvis
# ------------------------------------------------------------------

def render_pyvis(
    G: nx.Graph,
    out_path: Path,
    nodos_df: pd.DataFrame,
    top_global: Sequence[Sequence[Any]],
    top_by_tema: Sequence[Sequence[Any]],
    top_by_sub: Sequence[Sequence[Any]],
    metricas_tema: pd.DataFrame,
    temas_sospechosos_set: set[int] | None = None,
    topic_info: dict[str, dict[str, Any]] | None = None,
) -> bool:
    try:
        from pyvis.network import Network  # noqa: PLC0415
    except ImportError as exc:
        print(f"[WARN] pyvis no instalado ({exc}); no se genera HTML.")
        return False

    print("  Generando HTML pyvis...")
    net = Network(
        height="100vh",
        width="100%",
        bgcolor="#222222",
        font_color="white",
        notebook=False,
        heading="",
        cdn_resources="in_line",
    )

    # Tamano de nodo por grado total: curva agresiva para separar centros y periferia.
    if nodos_df["grado_total"].max() > 0:
        gmin = nodos_df["grado_total"].min()
        gmax = nodos_df["grado_total"].max()
    else:
        gmin, gmax = 0, 1
    tam_map = {}
    for n in G.nodes():
        g = int(nodos_df.loc[nodos_df["palabra"] == n, "grado_total"].fillna(0).iloc[0])
        pos = (g - gmin) / max(1, gmax - gmin)
        tam_map[n] = 12 + int((pos ** 2.15) * 295)  # 12..307

    # Color por rol
    rol_color = {
        "hub_endogamico": "#ff7f0e",
        "conector_provincial": "#2ca02c",
        "broker": "#d62728",
        "periferico": "#7f7f7f",
    }
    # Construir lookup rapido
    rol_map = dict(zip(nodos_df["palabra"], nodos_df["rol"]))
    color_tema_map = dict(zip(nodos_df["palabra"], nodos_df["color_tema"]))

    for n in G.nodes():
        rol = rol_map.get(n, "periferico")
        # si es periferico, usa color del tema para que se distingan clusters
        color = rol_color.get(rol, color_tema_map.get(n, "#999999"))
        # tooltip enriquecido
        info = nodos_df[nodos_df["palabra"] == n].iloc[0]
        title = (
            f"<b>{n}</b><br>"
            f"rol: {info['rol']}<br>"
            f"tema: T{info['tema_id']:02d} / sub S{info['sub_id']:02d}<br>"
            f"grado: {info['grado_total']} "
            f"(intra_sub={info['grado_intra_sub']}, "
            f"intra_cl={info['grado_intra_cluster']}, "
            f"extra={info['grado_extra']})<br>"
            f"participation: {info['participation']}<br>"
            f"z_within: {info['z_within_degree']}<br>"
            f"pagerank: {info['pagerank']}"
        )
        # tamano de fuente por grado (mayor gradiente = mas visible)
        g_pos = (info["grado_total"] - gmin) / max(1, gmax - gmin)
        font_size = 10 + int((g_pos ** 2.05) * 105)  # 10..115

        net.add_node(
            n,
            label=n,
            title="",  # sin tooltip nativo (lo maneja el panel custom)
            color=color,
            size=tam_map[n],
            font={"size": font_size, "color": "#ffffff", "face": "arial"},
            group=f"T{info['tema_id']:02d}",
            # campos personalizados para los filtros JS
            rol=str(info["rol"]),
            tema=str(int(info["tema_id"])),
            participation=float(info["participation"]),
            z_within=float(info["z_within_degree"]),
            pagerank=float(info["pagerank"]),
            grado=int(info["grado_total"]),
            sospechoso=bool(temas_sospechosos_set and int(info["tema_id"]) in temas_sospechosos_set),
            hidden=bool(rol not in {"broker", "hub_endogamico"} or int(info["grado_total"]) < 25),
        )

    # Aristas: color por tipo, ancho por peso
    tipo_color = {
        "intra_sub": "#4daf4a",       # verde
        "intra_cluster": "#377eb8",   # azul
        "extra": "#e41a1c",           # rojo
    }
    # ancho normalizado por tipo
    for tipo in ["intra_sub", "intra_cluster", "extra"]:
        subset = [(u, v, d["weight"]) for u, v, d in G.edges(data=True) if d.get("tipo") == tipo]
        if not subset:
            continue
        wmax = max(w for _, _, w in subset)
        wmin = min(w for _, _, w in subset)
        for u, v, w in subset:
            t = (w - wmin) / max(1, wmax - wmin)
            width = 0.3 + t * 5.0
            title = f"{tipo} (w={w:.0f})"
            net.add_edge(
                u,
                v,
                color=tipo_color[tipo],
                width=width,
                title=title,
                hidden=tipo != "intra_sub",
            )

    # Opciones de visualizacion: fisica + filtros por grupo/tipo
    net.set_options("""
    {
      "physics": {
        "enabled": true,
        "solver": "forceAtlas2Based",
        "forceAtlas2Based": {
          "gravitationalConstant": -70,
          "centralGravity": 0.006,
          "springLength": 160,
          "springConstant": 0.08,
          "damping": 0.4
        },
        "stabilization": {"iterations": 200}
      },
      "interaction": {
        "hover": true,
        "tooltipDelay": 100,
        "navigationButtons": true,
        "keyboard": true
      },
      "nodes": {"borderWidth": 0.5, "borderWidthSelected": 2}
    }
    """)

    net.write_html(str(out_path), open_browser=False)
    # Inyectar CSS + panel de filtros por capa de aristas
    html = out_path.read_text(encoding="utf-8")
    # PyVis agrega Bootstrap desde CDN aunque vis-network vaya incrustado.
    # Esta visualizacion no usa esos componentes; quitarlos la hace autocontenida.
    bootstrap_cdn = """        <link
          href="https://cdn.jsdelivr.net/npm/bootstrap@5.0.0-beta3/dist/css/bootstrap.min.css"
          rel="stylesheet"
          integrity="sha384-eOJMYsd53ii+scO/bJGFsiCZc+5NDVN2yr8+0RDqr0Ql0h+rP48ckxlpbzKgwra6"
          crossorigin="anonymous"
        />
        <script
          src="https://cdn.jsdelivr.net/npm/bootstrap@5.0.0-beta3/dist/js/bootstrap.bundle.min.js"
          integrity="sha384-JEW9xMcG8R+pH31jmWH6WWP0WintQrMb4s7ZOdauHnUtxwoG2vI5DkLtS3qm9Ekf"
          crossorigin="anonymous"
        ></script>"""
    html = html.replace(bootstrap_cdn, "")

    # === Construir mapas NODE_META y EDGE_INFO desde Python ===
    # EDGE_INFO: edge_id -> { color } (las aristas de pyvis se numeran 1..N)
    edge_info: dict[str, dict[str, str]] = {}
    edge_idx = 0
    for tipo in ["intra_sub", "intra_cluster", "extra"]:
        subset = [(u, v, d["weight"]) for u, v, d in G.edges(data=True) if d.get("tipo") == tipo]
        if not subset:
            continue
        wmax = max(w for _, _, w in subset)
        wmin = min(w for _, _, w in subset)
        for u, v, w in subset:
            edge_idx += 1
            color = tipo_color[tipo]
            edge_info[str(edge_idx)] = {"color": color}
    edge_info_js = json.dumps(edge_info, ensure_ascii=False)

    # NODE_META: palabra -> {rol, tema, participation, z_within, pagerank, grado, sospechoso}
    node_meta: dict[str, dict[str, Any]] = {}
    sus_set = temas_sospechosos_set or set()
    for _, row in nodos_df.iterrows():
        p = str(row["palabra"])
        t = int(row["tema_id"])
        node_meta[p] = {
            "rol": str(row.get("rol", "periferico")),
            "tema": t,
            "participation": float(row.get("participation", 0)),
            "z_within": float(row.get("z_within_degree", 0)),
            "pagerank": float(row.get("pagerank", 0)),
            "grado": int(row.get("grado_total", 0)),
            "sospechoso": t in sus_set,
        }
    node_meta_js = json.dumps(node_meta, ensure_ascii=False)
    topic_info_js = json.dumps(topic_info or {}, ensure_ascii=False)

    css_fix = (
        "<style>\n"
        "  * { box-sizing:border-box; }\n"
        "  html, body { margin:0; padding:0; height:100%; width:100%; "
        "background:#222; overflow:hidden; font-family: sans-serif; }\n"
        "  body > center { display:none; }\n"
        "  body > .card {\n"
        "    position:fixed !important; inset:0 !important; width:100vw !important;\n"
        "    height:100vh !important; margin:0 !important; padding:0 !important;\n"
        "    border:0 !important; border-radius:0 !important; overflow:hidden !important;\n"
        "    background:#222 !important; display:block !important;\n"
        "  }\n"
        "  #mynetwork, #mynetwork.card-body {\n"
        "    position:fixed !important; inset:0 !important; width:100vw !important;\n"
        "    height:100vh !important; margin:0 !important; padding:0 !important;\n"
        "    border:0 !important; overflow:hidden !important; float:none !important;\n"
        "  }\n"
        "  #loadingBar, #bar, #border, #text, .outerBorder { display:none !important; }\n"
        "  #config, #options { display:none; }\n"
        "  .card { background:#333 !important; color:#fff !important; }\n"
        "  /* BARRA SUPERIOR CENTRADA con todos los controles */\n"
        "  #topBar {\n"
        "    position:static; width:100%; padding:0; margin:0 0 10px 0;\n"
        "    font-size:12px; color:#fff; overflow:visible; pointer-events:auto;\n"
        "  }\n"
        "  #topBarInner {\n"
        "    width:100%; display:block;\n"
        "  }\n"
        "  #topBar .grp {\n"
        "    background:rgba(20,20,20,0.96); padding:8px 10px;\n"
        "    border:1px solid #555; border-radius:6px;\n"
        "    width:100%; margin-bottom:8px; pointer-events:auto;\n"
        "  }\n"
        "  #topBar h4 { margin:0 0 6px 0; font-size:11px; color:#bbb;\n"
        "    text-transform:uppercase; letter-spacing:0.5px; }\n"
        "  #topBar label { display:block; margin:3px 0; cursor:pointer; }\n"
        "  #topBar input[type=checkbox] { vertical-align:middle; margin-right:5px; }\n"
        "  #topBar .swatch {\n"
        "    display:inline-block; width:12px; height:12px; vertical-align:middle;\n"
        "    margin-right:5px; border-radius:50%; border:1px solid #222;\n"
        "  }\n"
        "  #topBar .swatchL { width:18px; height:3px; vertical-align:middle;\n"
        "    margin-right:5px; border-radius:1px; }\n"
        "  #topBar input[type=range] { width:100%; }\n"
        "  #topBar .rangoVal { color:#ffeb3b; font-weight:bold; margin-left:4px; }\n"
        "  /* PANEL LATERAL DE BUSQUEDA */\n"
        "  #searchPanel {\n"
        "    position:fixed; top:12px; left:12px; z-index:2147483000;\n"
        "    background:rgba(30,30,30,0.92); color:#fff; padding:12px 14px;\n"
        "    border:1px solid #555; border-radius:6px; font-size:12px;\n"
        "    width:340px; max-height:calc(100vh - 24px); overflow-y:auto;\n"
        "  }\n"
        "  #searchPanel h4 { margin:10px 0 6px 0; font-size:12px; color:#ddd;\n"
        "    border-top:1px solid #444; padding-top:8px; }\n"
        "  #searchPanel h4:first-child { border-top:0; padding-top:0; margin-top:0; }\n"
        "  #searchInput {\n"
        "    width:100%; box-sizing:border-box; padding:5px 7px; border-radius:3px;\n"
        "    border:1px solid #555; background:#1a1a1a; color:#fff; font-size:12px;\n"
        "  }\n"
        "  #searchResults { margin:6px 0; max-height:160px; overflow-y:auto; }\n"
        "  #searchResults .match {\n"
        "    padding:4px 6px; cursor:pointer; border-radius:3px; color:#fff;\n"
        "  }\n"
        "  #searchResults .match:hover { background:#444; }\n"
        "  #searchResults .empty { color:#888; font-style:italic; padding:4px; }\n"
        "  .sec { margin-bottom:6px; }\n"
        "  .scroll { max-height:200px; overflow-y:auto; border:1px solid #333;\n"
        "    border-radius:3px; }\n"
        "  #searchPanel table {\n"
        "    width:100%; border-collapse:collapse; font-size:11px;\n"
        "  }\n"
        "  #searchPanel th, #searchPanel td {\n"
        "    padding:3px 6px; text-align:left; border-bottom:1px solid #2a2a2a;\n"
        "  }\n"
        "  #searchPanel th { background:#222; color:#aaa; position:sticky; top:0; }\n"
        "  #searchPanel tr[data-id] { cursor:pointer; }\n"
        "  #searchPanel tr[data-id]:hover { background:#333; }\n"
        "  #searchPanel td b { color:#ffeb3b; }\n"
        "  #temaChecks { max-height:430px; overflow-y:auto; border:1px solid #333;\n"
        "    border-radius:4px; }\n"
        "  .topic-filter-card { padding:6px 8px; border-bottom:1px solid #2a2a2a; }\n"
        "  .topic-filter-card:last-child { border-bottom:0; }\n"
        "  .topic-filter-title { display:block; margin:0 0 4px 0; cursor:pointer;\n"
        "    color:#f0f0f0; font-size:11px; line-height:1.35; }\n"
        "  .topic-filter-card p { margin:6px 0 4px 0; color:#ddd;\n"
        "    font-size:11px; line-height:1.35; }\n"
        "  .topic-words { color:#9ecbff; font-size:10px; line-height:1.35; }\n"
        "  .topic-word { display:inline-block; margin:1px 2px 1px 0; padding:1px 4px;\n"
        "    border-radius:3px; background:#1f2a33; color:#9ecbff; cursor:pointer; }\n"
        "  .topic-word:hover { background:#2e4558; color:#fff; }\n"
        "  .search-section-title { color:#aaa; font-size:10px; text-transform:uppercase;\n"
        "    letter-spacing:0.4px; margin:6px 0 3px; }\n"
        "  .topic-match { padding:6px; border-bottom:1px solid #2a2a2a; cursor:pointer; }\n"
        "  .topic-match:hover { background:#333; }\n"
        "  .topic-match .topic-title { color:#f0f0f0; font-size:11px; line-height:1.35; }\n"
        "  .topic-match p { margin:4px 0; color:#ccc; font-size:10px; line-height:1.35; }\n"
        "  /* STATS inferior */\n"
        "  #statsBar {\n"
        "    position:fixed; bottom:8px; right:12px; z-index:9998;\n"
        "    background:rgba(30,30,30,0.92); color:#aaa; padding:5px 10px;\n"
        "    border:1px solid #444; border-radius:4px; font-size:11px;\n"
        "  }\n"
        "  #statsBar b { color:#fff; }\n"
        "  /* TOOLTIP CUSTOM (caja explicativa para neofitos) */\n"
        "  #nodeTooltip {\n"
        "    position:fixed; z-index:10000; display:none;\n"
        "    background:#1a1a1a; color:#f0f0f0; border:1px solid #777;\n"
        "    border-radius:8px; padding:12px 14px; max-width:340px;\n"
        "    box-shadow:0 6px 24px rgba(0,0,0,0.6);\n"
        "    font-family: sans-serif; font-size:13px; line-height:1.5;\n"
        "    pointer-events:auto;\n"
        "  }\n"
        "  #nodeTooltip .tt-title {\n"
        "    font-size:18px; font-weight:bold; color:#ffeb3b;\n"
        "    margin:0 0 6px 0; padding-bottom:6px;\n"
        "    border-bottom:1px solid #444;\n"
        "  }\n"
        "  #nodeTooltip .tt-row {\n"
        "    display:flex; justify-content:space-between; gap:10px;\n"
        "    margin:3px 0;\n"
        "  }\n"
        "  #nodeTooltip .tt-row .lbl { color:#aaa; }\n"
        "  #nodeTooltip .tt-row .val { color:#fff; font-weight:bold; }\n"
        "  #nodeTooltip .tt-expl {\n"
        "    margin-top:8px; padding-top:8px; border-top:1px solid #333;\n"
        "    color:#ddd; font-size:12px; line-height:1.45;\n"
        "  }\n"
        "  #nodeTooltip .tt-badge {\n"
        "    display:inline-block; padding:2px 8px; border-radius:10px;\n"
        "    font-size:11px; font-weight:bold; color:#fff;\n"
        "  }\n"
        "  #nodeTooltip .tt-meter {\n"
        "    height:6px; background:#333; border-radius:3px; margin:2px 0;\n"
        "    overflow:hidden;\n"
        "  }\n"
        "  #nodeTooltip .tt-meter > div {\n"
        "    height:100%; border-radius:3px; transition: width .3s;\n"
        "  }\n"
        "</style>\n"
    )

    panel_html = (
        "<div id=\"searchPanel\">\n"
        "<div id=\"topBar\"><div id=\"topBarInner\">"
        "<div class=\"grp\"><h4>Conexiones (por color)</h4>"
            "    <label><input type=\"checkbox\" data-edge=\"#4daf4a\" checked>\n"
            "      <span class=\"swatchL\" style=\"background:#4daf4a\"></span>\n"
            "      Vecinas (verde)</label>\n"
            "    <label><input type=\"checkbox\" data-edge=\"#377eb8\">\n"
            "      <span class=\"swatchL\" style=\"background:#377eb8\"></span>\n"
            "      Mismo tema (azul)</label>\n"
            "    <label><input type=\"checkbox\" data-edge=\"#e41a1c\">\n"
            "      <span class=\"swatchL\" style=\"background:#e41a1c\"></span>\n"
            "      Entre temas (rojo)</label>\n"
            "    <div style=\"color:#bbb;font-size:10px;margin-top:6px\">\n"
            "      <b>verde</b> = palabras que aparecen una tras otra<br>\n"
            "      <b>azul</b> = enlaces dentro de un mismo tema<br>\n"
            "      <b>rojo</b> = enlaces hacia otros temas</div>\n"
            "  </div>\n"
            "  <div class=\"grp\">\n"
            "    <h4>Palabras (por tipo)</h4>\n"
            "    <label><input type=\"checkbox\" data-rol=\"broker\" checked>\n"
            "      <span class=\"swatch\" style=\"background:#d62728\"></span>\n"
            "      <b>Puentes</b> — unen varias conversaciones</label>\n"
            "    <label><input type=\"checkbox\" data-rol=\"hub_endogamico\" checked>\n"
            "      <span class=\"swatch\" style=\"background:#ff7f0e\"></span>\n"
            "      <b>Núcleo</b> — palabras clave de un solo tema</label>\n"
            "    <label><input type=\"checkbox\" data-rol=\"conector_provincial\">\n"
            "      <span class=\"swatch\" style=\"background:#2ca02c\"></span>\n"
            "      <b>Conexiones locales</b> — unen dos o tres temas</label>\n"
            "    <label><input type=\"checkbox\" data-rol=\"periferico\">\n"
            "      <span class=\"swatch\" style=\"background:#7f7f7f\"></span>\n"
            "      <b>Ruido</b> — palabras sueltas, poco centrales</label>\n"
            "    <hr style=\"border:0; border-top:1px solid #444; margin:6px 0;\">\n"
            "    <h4 style=\"margin-top:4px\">Por conversación</h4>\n"
            "    <div id=\"temaChecks\"></div>\n"
            "    <label><input type=\"checkbox\" id=\"allTemas\" checked>mostrar todas</label>\n"
            "  </div>\n"
            "  <div class=\"grp\" style=\"min-width:240px\">\n"
            "    <h4>Filtros de palabras</h4>\n"
            "    <label>Encerradas en su tema (mín):\n"
            "      <span class=\"rangoVal\" id=\"endoMinV\">0.00</span></label>\n"
            "    <input type=\"range\" id=\"endoMin\" min=\"0\" max=\"100\" value=\"0\">\n"
            "    <div style=\"color:#bbb;font-size:10px;margin-top:-2px;margin-bottom:6px\">\n"
            "      sube para ocultar palabras que casi no salen de su tema</div>\n"
            "    <label>Poco conectadas (mín enlaces):\n"
            "      <span class=\"rangoVal\" id=\"gradMinV\">25</span></label>\n"
            "    <input type=\"range\" id=\"gradMin\" min=\"0\" max=\"100\" value=\"25\">\n"
            "    <div style=\"color:#bbb;font-size:10px;margin-top:-2px;margin-bottom:6px\">\n"
            "      sube para ocultar palabras con pocas conexiones</div>\n"
            "    <label><input type=\"checkbox\" id=\"soloBrokers\">\n"
            "      solo palabras puente (las que más unen temas)</label>\n"
            "    <label><input type=\"checkbox\" id=\"soloHubs\">\n"
            "      solo palabras clave de un tema</label>\n"
            "    <label><input type=\"checkbox\" id=\"soloSospechosos\">\n"
            "      solo palabras de conversaciones sospechosas</label>\n"
            "  </div>\n"
            "  <div class=\"grp\">\n"
            "    <h4>Visual</h4>\n"
            "    <label>Tamaño de palabras:\n"
            "      <span class=\"rangoVal\" id=\"tamMinV\">100%</span></label>\n"
            "    <input type=\"range\" id=\"tamMin\" min=\"60\" max=\"180\" value=\"100\">\n"
            "    <label>Separación:\n"
            "      <span class=\"rangoVal\" id=\"springV\">160</span></label>\n"
            "    <input type=\"range\" id=\"spring\" min=\"20\" max=\"300\" value=\"160\">\n"
            "    <label>Atracción al centro:\n"
            "      <span class=\"rangoVal\" id=\"gravV\">0.01</span></label>\n"
            "    <input type=\"range\" id=\"grav\" min=\"0\" max=\"100\" value=\"1\">\n"
            "    <button id=\"resetPhysics\" style=\"margin-top:6px;background:#444;color:#fff;border:1px solid #666;padding:3px 8px;border-radius:3px;cursor:pointer;\">Reorganizar</button>\n"
            "  </div>\n"
            "</div></div>\n"
            "  <h4>Buscar conversación, tema o palabra</h4>\n"
            "  <input type=\"text\" id=\"searchInput\" placeholder=\"tema, conversación o palabra...\">\n"
            "  <div id=\"searchResults\"></div>\n"
            "</div>\n"
            "<div id=\"statsBar\"><b>palabras</b>: <span id=\"visNodos\">0</span> |\n"
            "  <b>conexiones</b>: <span id=\"visEdges\">0</span></div>\n"
            "<div id=\"nodeTooltip\"></div>\n"
            "<script>\n"
            "  /* Espera a que pyvis cree 'network' y expone todo a window */\n"
            "  function wireUp(){\n"
            "    if (typeof network === 'undefined') { setTimeout(wireUp, 100); return; }\n"
            "    window.network = network;\n"
            "    window.nodes = nodes;\n"
            "    window.edges = edges;\n"
            "    if (typeof NODE_META !== 'undefined') window.allNodeIds = Object.keys(NODE_META);\n"
            "  }\n"
            "  wireUp();\n"
            "  /* === ARISTAS === */\n"
            "  function getEdgeColor(e) {\n"
            "    if (!e.color) return null;\n"
            "    return (typeof e.color === 'object') ? e.color.color : e.color;\n"
            "  }\n"
            "  function updateEdgeFilter() {\n"
            "    var checks = document.querySelectorAll('#topBar input[data-edge]');\n"
            "    var enabled = {};\n"
            "    checks.forEach(function(c){ enabled[c.dataset.edge.toLowerCase()] = c.checked; });\n"
            "    var updates = edges.map(function(e){\n"
            "      var c = getEdgeColor(e);\n"
            "      if (!c) return {id: e.id, hidden: false};\n"
            "      return {id: e.id, hidden: enabled[c.toLowerCase()] === false};\n"
            "    });\n"
            "    edges.update(updates);\n"
            "  }\n"
            "  /* === FILTROS DE NODO === */\n"
            "  var rolMap = {};   // id -> rol\n"
            "  var temaMap = {};  // id -> tema_id\n"
            "  var suspMap = {};  // id -> bool (pertenece a tema sospechoso)\n"
            "  var partMap = {};  // id -> participation\n"
            "  var zMap = {};     // id -> z_within_degree\n"
            "  var gradMap = {};  // id -> grado_total\n"
            "  function applyNodeFilter() {\n"
            "    var rolEnabled = {};\n"
            "    document.querySelectorAll('#topBar input[data-rol]').forEach(function(c){\n"
            "      rolEnabled[c.dataset.rol] = c.checked;\n"
            "    });\n"
            "    var temaEnabled = {};\n"
            "    document.querySelectorAll('#temaChecks input[data-tema]').forEach(function(c){\n"
            "      temaEnabled[c.dataset.tema] = c.checked;\n"
            "    });\n"
            "    var endoMin = parseFloat(document.getElementById('endoMin').value) / 100;\n"
            "    var gradMin = parseInt(document.getElementById('gradMin').value, 10);\n"
            "    var soloBrokers = document.getElementById('soloBrokers').checked;\n"
            "    var soloHubs = document.getElementById('soloHubs').checked;\n"
            "    var soloSusp = document.getElementById('soloSospechosos').checked;\n"
            "    var visible = 0;\n"
            "    var updates = nodes.map(function(n){\n"
            "      var id = n.id;\n"
            "      var rol = rolMap[id] || 'periferico';\n"
            "      var tema = temaMap[id];\n"
            "      var part = partMap[id] || 0;\n"
            "      var z = zMap[id] || 0;\n"
            "      var gr = gradMap[id] || 0;\n"
            "      var vis = true;\n"
            "      if (rolEnabled[rol] === false) vis = false;\n"
            "      if (vis && temaEnabled[String(tema)] === false) vis = false;\n"
            "      if (vis && endoMin > 0 && part < endoMin) vis = false;\n"
            "      if (vis && gradMin > 0 && gr < gradMin) vis = false;\n"
            "      if (vis && soloBrokers && part < 0.5) vis = false;\n"
            "      if (vis && soloHubs && !(rol === 'hub_endogamico')) vis = false;\n"
            "      if (vis && soloSusp && !suspMap[id]) vis = false;\n"
            "      if (vis) visible++;\n"
            "      return {id: id, hidden: !vis};\n"
            "    });\n"
            "    nodes.update(updates);\n"
            "    document.getElementById('visNodos').textContent = visible;\n"
                    "  }\n"
                    "  /* === FILTROS (VERSION ROBUSTA, mapas inyectados desde Python) === */\n"
                    "  // NODE_META: { palabra: {rol, tema, participation, z_within, pagerank, grado, sospechoso} }\n"
                    "  var allNodeIds = [];\n"
                    "  function rebuildNodeVisibility() {\n"
                    "    var enabledRoles = {};\n"
                    "    document.querySelectorAll('#topBar input[data-rol]').forEach(function(c){\n"
                    "      enabledRoles[c.dataset.rol] = c.checked;\n"
                    "    });\n"
                    "    var enabledTemas = {};\n"
                    "    document.querySelectorAll('#temaChecks input[data-tema]').forEach(function(c){\n"
                    "      enabledTemas[c.dataset.tema] = c.checked;\n"
                    "    });\n"
                    "    var endoMin = parseFloat(document.getElementById('endoMin').value) / 100;\n"
                    "    var gradMin = parseInt(document.getElementById('gradMin').value, 10);\n"
                    "    var soloBrokers = document.getElementById('soloBrokers').checked;\n"
                    "    var soloHubs = document.getElementById('soloHubs').checked;\n"
                    "    var soloSusp = document.getElementById('soloSospechosos').checked;\n"
                    "    var updates = [];\n"
                    "    var visible = 0;\n"
                    "    allNodeIds.forEach(function(id){\n"
                    "      var m = NODE_META[id];\n"
                    "      var vis = true;\n"
                    "      if (enabledRoles[m.rol] === false) vis = false;\n"
                    "      if (vis && enabledTemas[String(m.tema)] === false) vis = false;\n"
                    "      if (vis && endoMin > 0 && m.participation < endoMin) vis = false;\n"
                    "      if (vis && gradMin > 0 && m.grado < gradMin) vis = false;\n"
                    "      if (vis && soloBrokers && m.participation < 0.5) vis = false;\n"
                    "      if (vis && soloHubs && m.rol !== 'hub_endogamico') vis = false;\n"
                    "      if (vis && soloSusp && !m.sospechoso) vis = false;\n"
                    "      if (vis) visible++;\n"
                    "      updates.push({id: id, hidden: !vis});\n"
                    "    });\n"
                    "    nodes.update(updates);\n"
                    "    document.getElementById('visNodos').textContent = visible;\n"
                    "  }\n"
                    "  function rebuildEdgeVisibility() {\n"
                    "    var checks = document.querySelectorAll('#topBar input[data-edge]');\n"
                    "    var enabled = {};\n"
                    "    checks.forEach(function(c){ enabled[c.dataset.edge.toLowerCase()] = c.checked; });\n"
                    "    var visible = 0;\n"
                    "    var updates = edges.map(function(e){\n"
                    "      var c = getEdgeColor(e);\n"
                    "      var hidden = c ? (enabled[c.toLowerCase()] === false) : false;\n"
                    "      if (!hidden) visible++;\n"
                    "      return {id: e.id, hidden: hidden};\n"
                    "    });\n"
                    "    edges.update(updates);\n"
                    "    document.getElementById('visEdges').textContent = visible;\n"
                    "  }\n"
                    "  function escapeHtml(v) {\n"
                    "    return String(v || '').replace(/[&<>\"']/g, function(ch){\n"
                    "      return {'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',\"'\":'&#39;'}[ch];\n"
                    "    });\n"
                    "  }\n"
                    "  function topicCode(t) {\n"
                    "    return 'T' + String(t).padStart(2, '0');\n"
                    "  }\n"
                    "  function topicWordsFromNodes(t, limit) {\n"
                    "    var words = [];\n"
                    "    allNodeIds.forEach(function(id){\n"
                    "      var m = NODE_META[id];\n"
                    "      if (m && String(m.tema) === String(t)) words.push({id:id, grado:m.grado || 0, pr:m.pagerank || 0});\n"
                    "    });\n"
                    "    words.sort(function(a,b){ return (b.grado - a.grado) || (b.pr - a.pr); });\n"
                    "    return words.slice(0, limit || 60).map(function(w){ return w.id; });\n"
                    "  }\n"
                    "  function wordChips(words) {\n"
                    "    return (words || []).map(function(w){\n"
                    "      return '<span class=\"topic-word\" data-id=\"'+escapeHtml(w)+'\">'+escapeHtml(w)+'</span>';\n"
                    "    }).join(' ');\n"
                    "  }\n"
                    "  function topicCardHtml(t) {\n"
                    "    var info = TOPIC_INFO[String(t)] || {};\n"
                    "    var code = topicCode(t);\n"
                    "    var title = info.title || 'Tema por palabras clave';\n"
                    "    var summary = info.summary || 'Tema definido por sus palabras más frecuentes y sus conexiones dentro de la red.';\n"
                    "    var words = Array.isArray(info.words) && info.words.length ? info.words.slice(0, 60) : topicWordsFromNodes(t, 60);\n"
                    "    return '<div class=\"topic-filter-card\">'\n"
                    "      + '<label class=\"topic-filter-title\"><input type=\"checkbox\" data-tema=\"'+t+'\" checked> '\n"
                    "      + '<b>'+code+'</b> · '+escapeHtml(title)+'</label>'\n"
                    "      + '<p>'+escapeHtml(summary)+'</p>'\n"
                    "      + '<div class=\"topic-words\">'+wordChips(words)+'</div>'\n"
                    "      + '</div>';\n"
                    "  }\n"
                    "  function setupTemaChecks() {\n"
                    "    var setT = {};\n"
                    "    allNodeIds.forEach(function(id){\n"
                    "      setT[NODE_META[id].tema] = true;\n"
                    "    });\n"
                    "    var html = '';\n"
                    "    Object.keys(setT).sort(function(a,b){return Number(a)-Number(b);}).forEach(function(t){\n"
                    "      html += topicCardHtml(t);\n"
                    "    });\n"
                    "    document.getElementById('temaChecks').innerHTML = html;\n"
                    "    document.querySelectorAll('#temaChecks input').forEach(function(c){\n"
                    "      c.addEventListener('change', function(){\n"
                    "        document.getElementById('allTemas').checked = false;\n"
                    "        rebuildNodeVisibility();\n"
                    "      });\n"
                    "    });\n"
                    "    document.querySelectorAll('#temaChecks .topic-word').forEach(function(w){\n"
                    "      w.addEventListener('click', function(e){ e.stopPropagation(); focusNode(w.dataset.id); });\n"
                    "    });\n"
                    "  }\n"
                    "  document.getElementById('allTemas').addEventListener('change', function(e){\n"
                    "    var ch = e.target.checked;\n"
                    "    document.querySelectorAll('#temaChecks input').forEach(function(c){ c.checked = ch; });\n"
                    "    rebuildNodeVisibility();\n"
                    "  });\n"
                    "  document.querySelectorAll('#topBar input[data-rol]').forEach(function(c){\n"
                    "    c.addEventListener('change', rebuildNodeVisibility);\n"
                    "  });\n"
                    "  document.querySelectorAll('#topBar input[data-edge]').forEach(function(c){\n"
                    "    c.addEventListener('change', rebuildEdgeVisibility);\n"
                    "  });\n"
                    "  document.getElementById('soloBrokers').addEventListener('change', rebuildNodeVisibility);\n"
                    "  document.getElementById('soloHubs').addEventListener('change', rebuildNodeVisibility);\n"
                    "  document.getElementById('soloSospechosos').addEventListener('change', rebuildNodeVisibility);\n"
        "  /* === TOOLTIP CUSTOM (caja explicativa para neofitos) === */\n"
        "  var tt = document.getElementById('nodeTooltip');\n"
        "  var ttNodeId = null;\n"
        "  function fmtPct(x){ return Math.round(x*100)+'%'; }\n"
        "  function fmtNum(x){ return Math.round(x); }\n"
        "  function explainRol(rol, part, z){\n"
        "    var expl = {\n"
        "      'broker': 'Esta palabra conecta varios temas distintos. Es como un \"puente\" de la conversación: aparece cuando se cruzan conversaciones diferentes.',\n"
        "      'hub_endogamico': 'Esta palabra es MUY mencionada dentro de su propio tema pero casi no sale de ahí. Puede ser una \"palabra clave\" del tema o una \"cámara de eco\".',\n"
        "      'conector_provincial': 'Conecta su tema con uno o dos vecinos cercanos, pero no llega a toda la red. Es un puente local.',\n"
        "      'periferico': 'Palabra poco central: aparece de vez en cuando sin estar en el centro de la conversación.'\n"
        "    };\n"
        "    var base = expl[rol] || expl['periferico'];\n"
        "    if (rol === 'broker' && part >= 0.7) {\n"
        "      base += ' Es un broker FUERTE: '+fmtPct(part)+' de sus enlaces cruzan a otros temas.';\n"
        "    } else if (rol === 'hub_endogamico' && part < 0.1) {\n"
        "      base += ' Casi no tiene enlaces hacia otros temas ('+fmtPct(part)+').';\n"
        "    }\n"
        "    if (z > 2) {\n"
        "      base += ' Esta palabra aparece MUCHÍSIMO dentro de su propio tema (z='+z.toFixed(1)+').';\n"
        "    }\n"
        "    return base;\n"
        "  }\n"
        "  function showTooltip(nodeId, x, y){\n"
        "    var m = NODE_META[nodeId];\n"
        "    if (!m) return;\n"
        "    var rol = m.rol || 'periferico';\n"
        "    var part = m.participation || 0;\n"
        "    var z = m.z_within || 0;\n"
        "    var gr = m.grado || 0;\n"
        "    var pr = m.pagerank || 0;\n"
        "    var tema = m.tema;\n"
        "    var rolColor = {\n"
        "      'broker': '#d62728',\n"
        "      'hub_endogamico': '#ff7f0e',\n"
        "      'conector_provincial': '#2ca02c',\n"
        "      'periferico': '#7f7f7f'\n"
        "    }[rol];\n"
        "    var rolNombre = {\n"
        "      'broker': 'Broker (puente entre temas)',\n"
        "      'hub_endogamico': 'Hub del tema',\n"
        "      'conector_provincial': 'Conector local',\n"
        "      'periferico': 'Periférica'\n"
        "    }[rol];\n"
        "    var endoLvl, endoMsg;\n"
        "    if (part >= 0.7) { endoLvl = 'alta'; endoMsg = 'participa en muchos temas'; }\n"
        "    else if (part >= 0.4) { endoLvl = 'media'; endoMsg = 'mezcla su tema con vecinos'; }\n"
        "    else if (part >= 0.1) { endoLvl = 'baja'; endoMsg = 'casi solo dentro de su tema'; }\n"
        "    else { endoLvl = 'muy baja'; endoMsg = 'recluida en su tema'; }\n"
        "    var pctGr = Math.min(100, Math.round(gr / 80 * 100));  // 80+ enlaces = 100%\n"
        "    var pctPr = Math.min(100, Math.round(pr * 5000));       // escalado\n"
        "    var html = ''\n"
        "      + '<div class=\"tt-title\">'+nodeId+'</div>'\n"
        "      + '<div><span class=\"tt-badge\" style=\"background:'+rolColor+'\">'+rolNombre+'</span> '\n"
        "      + '<span style=\"color:#888;font-size:11px\">en Tema T'+tema+'</span></div>'\n"
        "      + '<div class=\"tt-row\"><span class=\"lbl\">Veces enlazada (grado)</span><span class=\"val\">'+gr+'</span></div>'\n"
        "      + '<div class=\"tt-meter\"><div style=\"background:#377eb8;width:'+pctGr+'%\"></div></div>'\n"
        "      + '<div class=\"tt-row\"><span class=\"lbl\">Importancia general</span><span class=\"val\">'+fmtPct(pr)+'</span></div>'\n"
        "      + '<div class=\"tt-meter\"><div style=\"background:#ff7f0e;width:'+pctPr+'%\"></div></div>'\n"
        "      + '<div class=\"tt-row\"><span class=\"lbl\">Diversidad de temas</span><span class=\"val\">'+fmtPct(part)+'</span></div>'\n"
        "      + '<div class=\"tt-meter\"><div style=\"background:'+rolColor+';width:'+(part*100).toFixed(1)+'%\"></div></div>'\n"
        "      + '<div style=\"color:#bbb;font-size:11px;margin:4px 0;\">→ '+endoMsg+'</div>'\n"
        "      + '<div class=\"tt-expl\">'+explainRol(rol, part, z)+'</div>';\n"
        "    tt.innerHTML = html;\n"
        "    tt.style.display = 'block';\n"
        "    // Posicionar: a la derecha del mouse, sin salir de pantalla\n"
        "    var tw = 340, th = tt.offsetHeight || 220;\n"
        "    var px = x + 18, py = y + 18;\n"
        "    if (px + tw > window.innerWidth - 10) px = x - tw - 18;\n"
        "    if (py + th > window.innerHeight - 10) py = y - th - 18;\n"
        "    tt.style.left = px + 'px';\n"
        "    tt.style.top = py + 'px';\n"
        "  }\n"
        "  function hideTooltip(){ tt.style.display = 'none'; ttNodeId = null; }\n"
        "  function bindTooltip(){\n"
        "    network.on('hoverNode', function(p){\n"
        "      ttNodeId = p.node;\n"
        "      showTooltip(p.node, p.pointer.DOM.x, p.pointer.DOM.y);\n"
        "    });\n"
        "    network.on('blurNode', hideTooltip);\n"
        "    network.on('dragStart', hideTooltip);\n"
        "    network.on('dragging', function(p){\n"
        "      if (ttNodeId) showTooltip(ttNodeId, p.pointer.DOM.x, p.pointer.DOM.y);\n"
        "    });\n"
        "  }\n"
        "  /* === BUSQUEDA === */\n"
        "  function focusNode(nodeId) {\n"
        "    if (Object.prototype.hasOwnProperty.call(NODE_META, nodeId)) {\n"
        "      network.selectNodes([nodeId]);\n"
        "      network.focus(nodeId, {scale: 1.2, animation: {duration: 800, easingFunction: 'easeInOutQuad'}});\n"
        "    }\n"
        "  }\n"
        "  function focusTopic(t) {\n"
        "    var ids = topicWordsFromNodes(t, 18).filter(function(id){ return Object.prototype.hasOwnProperty.call(NODE_META, id); });\n"
        "    if (!ids.length) return;\n"
        "    network.selectNodes(ids);\n"
        "    network.fit({nodes: ids, animation: {duration: 800, easingFunction: 'easeInOutQuad'}});\n"
        "  }\n"
        "  function topicSearchText(t) {\n"
        "    var info = TOPIC_INFO[String(t)] || {};\n"
        "    var words = Array.isArray(info.words) ? info.words.join(' ') : '';\n"
        "    return (topicCode(t)+' '+(info.title || '')+' '+(info.summary || '')+' '+words).toLowerCase();\n"
        "  }\n"
        "  function setupSearch() {\n"
        "    var input = document.getElementById('searchInput');\n"
        "    var results = document.getElementById('searchResults');\n"
        "    input.addEventListener('input', function(){\n"
        "      var q = input.value.toLowerCase().trim();\n"
        "      if (q.length < 2) { results.innerHTML = ''; return; }\n"
        "      var topicMatches = Object.keys(TOPIC_INFO || {}).filter(function(t){\n"
        "        return topicSearchText(t).indexOf(q) !== -1;\n"
        "      }).sort(function(a,b){return Number(a)-Number(b);}).slice(0, 6);\n"
        "      var matches = allNodeIds.filter(function(id){\n"
        "        var m = NODE_META[id] || {};\n"
        "        var info = TOPIC_INFO[String(m.tema)] || {};\n"
        "        return (id+' '+topicCode(m.tema)+' '+(info.title || '')).toLowerCase().indexOf(q) !== -1;\n"
        "      }).sort(function(a,b){ return ((NODE_META[b] || {}).grado || 0) - ((NODE_META[a] || {}).grado || 0); }).slice(0, 15);\n"
        "      var html = '';\n"
        "      if (topicMatches.length) {\n"
        "        html += '<div class=\"search-section-title\">Conversaciones / temas</div>';\n"
        "        html += topicMatches.map(function(t){\n"
        "          var info = TOPIC_INFO[String(t)] || {};\n"
        "          var words = Array.isArray(info.words) ? info.words.slice(0, 18) : topicWordsFromNodes(t, 18);\n"
        "          return '<div class=\"topic-match\" data-tema=\"'+t+'\"><div class=\"topic-title\"><b>'+topicCode(t)+'</b> · '+escapeHtml(info.title || 'Tema por palabras clave')+'</div>'\n"
        "            + '<p>'+escapeHtml(info.summary || '')+'</p><div class=\"topic-words\">'+wordChips(words)+'</div></div>';\n"
        "        }).join('');\n"
        "      }\n"
        "      if (matches.length) {\n"
        "        html += '<div class=\"search-section-title\">Palabras en el grafo</div>';\n"
        "        html += matches.map(function(id){\n"
        "          var m = NODE_META[id] || {};\n"
        "          var info = TOPIC_INFO[String(m.tema)] || {};\n"
        "          return '<div class=\"match\" data-id=\"'+escapeHtml(id)+'\"><b>'+escapeHtml(id)+'</b><br><span style=\"color:#aaa\">'+topicCode(m.tema)+' · '+escapeHtml(info.title || '')+'</span></div>';\n"
        "        }).join('');\n"
        "      }\n"
        "      results.innerHTML = html || '<div class=\"empty\">sin coincidencias</div>';\n"
        "      results.querySelectorAll('.match').forEach(function(d){\n"
        "        d.addEventListener('click', function(){ focusNode(d.dataset.id); });\n"
        "      });\n"
        "      results.querySelectorAll('.topic-match').forEach(function(d){\n"
        "        d.addEventListener('click', function(){ focusTopic(d.dataset.tema); });\n"
        "      });\n"
        "      results.querySelectorAll('.topic-word').forEach(function(w){\n"
        "        w.addEventListener('click', function(e){ e.stopPropagation(); focusNode(w.dataset.id); });\n"
        "      });\n"
        "    });\n"
        "  }\n"
        "  /* === RANGOS (sin nodes.map) === */\n"
        "  function bindRange(id, targetId, mult, fmt) {\n"
        "    var el = document.getElementById(id);\n"
        "    var out = document.getElementById(targetId);\n"
        "    el.addEventListener('input', function(){\n"
        "      var v = parseFloat(el.value);\n"
        "      out.textContent = fmt ? fmt(v * mult) : (v * mult);\n"
        "      if (id === 'tamMin') updateNodeSizes();\n"
        "      else if (id === 'spring' || id === 'grav') updatePhysics();\n"
        "      else rebuildNodeVisibility();\n"
        "    });\n"
        "  }\n"
        "  function updateNodeSizes() {\n"
        "    var scale = parseInt(document.getElementById('tamMin').value, 10) / 100;\n"
        "    var grados = allNodeIds.map(function(id){ return (NODE_META[id] || {}).grado || 0; });\n"
        "    var gmin = Math.min.apply(null, grados);\n"
        "    var gmax = Math.max.apply(null, grados);\n"
        "    var updates = [];\n"
        "    allNodeIds.forEach(function(id){\n"
        "      var gr = (NODE_META[id] || {}).grado || 0;\n"
        "      var pos = (gr - gmin) / Math.max(1, gmax - gmin);\n"
        "      var nodeSize = (12 + Math.pow(pos, 2.15) * 295) * scale;\n"
        "      var fontSize = (10 + Math.pow(pos, 2.05) * 105) * scale;\n"
        "      updates.push({id: id, size: Math.max(8, Math.min(360, nodeSize)), font: {size: Math.max(8, Math.min(130, fontSize)), color: '#ffffff', face: 'arial'}});\n"
        "    });\n"
        "    nodes.update(updates);\n"
        "  }\n"
        "  function updatePhysics() {\n"
        "    var sp = parseInt(document.getElementById('spring').value, 10);\n"
        "    var gv = parseInt(document.getElementById('grav').value, 10) / 1000;\n"
        "    network.setOptions({\n"
        "      physics: {\n"
        "        forceAtlas2Based: {\n"
        "          springLength: sp,\n"
        "          centralGravity: gv,\n"
        "          gravitationalConstant: -50\n"
        "        }\n"
        "      }\n"
        "    });\n"
        "  }\n"
        "  function setupRanges() {\n"
        "    bindRange('endoMin', 'endoMinV', 0.01);\n"
        "    bindRange('gradMin', 'gradMinV', 1);\n"
        "    bindRange('tamMin', 'tamMinV', 1, function(v){ return Math.round(v)+'%'; });\n"
        "    bindRange('spring', 'springV', 1);\n"
        "    bindRange('grav', 'gravV', 0.0001);\n"
        "  }\n"
        "  /* === INIT === */\n"
        "  function syncPanelOffsets(){\n"
        "    var tb = document.getElementById('topBar');\n"
        "    var h = tb ? tb.offsetHeight : 120;\n"
        "    document.documentElement.style.setProperty('--topbar-height', h + 'px');\n"
        "  }\n"
        "  document.addEventListener('DOMContentLoaded', syncPanelOffsets);\n"
        "  window.addEventListener('resize', syncPanelOffsets);\n"
        "  window.addEventListener('load', function(){\n"
        "    allNodeIds = Object.keys(window.NODE_META || {});\n"
        "    syncPanelOffsets();\n"
        "    // Re-attach handlers (pueden haberse asignado versiones viejas)\n"
        "    document.querySelectorAll('#topBar input[data-rol]').forEach(function(c){\n"
        "      c.addEventListener('change', rebuildNodeVisibility);\n"
        "    });\n"
        "    document.querySelectorAll('#topBar input[data-edge]').forEach(function(c){\n"
        "      c.addEventListener('change', rebuildEdgeVisibility);\n"
        "    });\n"
        "    ['soloBrokers','soloHubs','soloSospechosos'].forEach(function(id){\n"
        "      var el = document.getElementById(id);\n"
        "      if (el) el.addEventListener('change', rebuildNodeVisibility);\n"
        "    });\n"
        "    var at = document.getElementById('allTemas');\n"
        "    if (at) at.addEventListener('change', function(e){\n"
        "      var ch = e.target.checked;\n"
        "      document.querySelectorAll('#temaChecks input').forEach(function(c){ c.checked = ch; });\n"
        "      rebuildNodeVisibility();\n"
        "    });\n"
        "    // Stats iniciales\n"
        "    document.getElementById('visNodos').textContent = allNodeIds.length;\n"
        "    document.getElementById('visEdges').textContent = edges.getIds().length;\n"
        "    setupTemaChecks();\n"
        "    syncPanelOffsets();\n"
        "    setupSearch();\n"
        "    setupRanges();\n"
        "    updateNodeSizes();\n"
        "    bindTooltip();\n"
        "    initTables();\n"
        "    // Re-aplicar visibilidad inicial\n"
        "    rebuildNodeVisibility();\n"
        "    rebuildEdgeVisibility();\n"
        "    // Reset física\n"
        "    document.getElementById('resetPhysics').addEventListener('click', function(){\n"
        "      network.setOptions({physics: {enabled: true, stabilization: {iterations: 200}}});\n"
        "      network.stabilize(200);\n"
        "    });\n"
        "  });\n"
        "</script>\n"
    )
    if "</head>" in html:
        html = html.replace("</head>", css_fix + "</head>", 1)
    if "<body>" in html:
        html = html.replace("<body>", "<body>\n" + panel_html, 1)

    # Inyectar tablas pobladas (top global, por tema, por sub) justo antes
    data_script = (
        "<script>\n"
        "  var TOP_GLOBAL = " + json.dumps(top_global, ensure_ascii=False) + ";\n"
        "  var TOP_TEMA = " + json.dumps(top_by_tema, ensure_ascii=False) + ";\n"
        "  var TOP_SUB = " + json.dumps(top_by_sub, ensure_ascii=False) + ";\n"
        "  var TOPIC_INFO = " + topic_info_js + ";\n"
        "  var NODE_META = " + node_meta_js + ";\n"
        "  var EDGE_INFO = " + edge_info_js + ";\n"
        "  function fillTable(tblId, rows) {\n"
        "    var tbl = document.getElementById(tblId);\n"
        "    if (!tbl) return;\n"
        "    var html = '<tr><th>#</th><th>palabra</th><th>contexto</th></tr>';\n"
        "    rows.forEach(function(r, i){\n"
        "      html += '<tr data-id=\"'+r[0]+'\"><td>'+(i+1)+'</td>'+\n"
        "              '<td><b>'+r[0]+'</b></td>'+\n"
        "              '<td>'+r[1]+'</td></tr>';\n"
        "    });\n"
        "    tbl.innerHTML = html;\n"
        "    tbl.querySelectorAll('tr[data-id]').forEach(function(tr){\n"
        "      tr.addEventListener('click', function(){\n"
        "        focusNode(tr.dataset.id);\n"
        "      });\n"
        "    });\n"
        "  }\n"
        "  function initTables(){\n"
        "    fillTable('topGlobal', TOP_GLOBAL);\n"
        "    fillTable('topByTema', TOP_TEMA);\n"
        "    fillTable('topBySub', TOP_SUB);\n"
        "  }\n"
        "</script>\n"
    )
    # Llamar initTables() en el load existente
    html = html.replace(
        "setupSearch();\n    // Las tablas se llenan desde Python antes de inyectar este script",
        "setupSearch();\n    initTables();",
        1,
    )
    if "</body>" in html:
        html = html.replace("</body>", data_script + "</body>", 1)

    out_path.write_text(html, encoding="utf-8")
    print(f"  HTML escrito en {out_path} (CSS + panel + tablas inyectadas)")
    return True


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--clusters-dir", type=Path, default=DEFAULT_CLUSTERS)
    parser.add_argument("--umbral-intra-sub", type=float)
    parser.add_argument("--umbral-intra-cluster", type=float)
    parser.add_argument("--umbral-extra", type=float)
    parser.add_argument("--diccionario-temas", type=Path, default=DEFAULT_TOPIC_DICTIONARY)
    parser.add_argument("--diccionario-positivo", type=Path, default=DEFAULT_POSITIVE_DICTIONARY)
    parser.add_argument("--diccionario-negativo", type=Path, default=DEFAULT_NEGATIVE_DICTIONARY)
    args = parser.parse_args()

    sna_dir = args.clusters_dir

    if not sna_dir.exists():
        print(f"No existe {sna_dir}")
        return 2

    out_dir = sna_dir / "red_guiada"
    out_dir.mkdir(parents=True, exist_ok=True)

    diagnostico = sna_dir / "diagnostico_umbrales.json"
    sugeridos: dict[str, Any] = {}
    if diagnostico.exists():
        sugeridos = json.loads(diagnostico.read_text(encoding="utf-8")).get(
            "umbrales_sugeridos", {}
        )
    args.umbral_intra_sub = args.umbral_intra_sub or sugeridos.get("umbral_intra_sub")
    args.umbral_intra_cluster = args.umbral_intra_cluster or sugeridos.get("umbral_intra_cluster")
    args.umbral_extra = args.umbral_extra or sugeridos.get("umbral_extra")
    if any(v is None for v in (
        args.umbral_intra_sub, args.umbral_intra_cluster, args.umbral_extra
    )):
        raise SystemExit(
            "Ejecuta primero 12c_diagnostico_umbrales.py o especifica los tres umbrales."
        )

    print("[red_completa] corpus historico de Tampico")
    print(
        "[red_completa] umbrales: "
        f"intra_sub={args.umbral_intra_sub}, "
        f"intra_cluster={args.umbral_intra_cluster}, extra={args.umbral_extra}"
    )
    print("[red_completa] cargando datos...")
    lda_asig, temas, intra_full, extra_full = cargar_datos(sna_dir)
    sub_palabras = cargar_subclusters(sna_dir)
    pal2subs = _asignar_subids(lda_asig, sub_palabras)
    print(f"  Palabras LDA: {len(lda_asig)}, con sub_id: {len(sub_palabras) if not sub_palabras.empty else 0}")

    print("[red_completa] construyendo grafo...")
    G = construir_grafo(
        lda_asig,
        intra_full,
        extra_full,
        pal2subs,
        umbral_intra_sub=args.umbral_intra_sub,
        umbral_intra_cluster=args.umbral_intra_cluster,
        umbral_extra=args.umbral_extra,
    )
    print(f"  Nodos: {G.number_of_nodes()}, Aristas: {G.number_of_edges()}")

    # Quitar nodos aislados (no tendran metricas utiles)
    aislados = list(nx.isolates(G))
    if aislados:
        G.remove_nodes_from(aislados)
        print(f"  Removidos {len(aislados)} nodos sin aristas")

    print("[red_completa] calculando metricas...")
    res = calcular_metricas(G)

    # Guardar
    res["nodos"].to_csv(out_dir / "nodos_metricas.csv", index=False)
    res["aristas"].to_csv(out_dir / "aristas_clasificadas.csv", index=False)
    res["subs"].to_csv(out_dir / "metricas_subcluster.csv", index=False)
    res["temas"].to_csv(out_dir / "metricas_tema.csv", index=False)
    res["brokers_top"].to_csv(out_dir / "brokers_top.csv", index=False)
    res["nodos"][["palabra", "tema_id", "sub_id", "rol", "participation",
                   "z_within_degree", "grado_total"]].to_csv(
        out_dir / "metricas_rol_nodo.csv", index=False
    )

    # sospechosos: solo los veredictos no "balanceado"
    if not res["temas"].empty and "veredicto" in res["temas"].columns:
        susp = res["temas"][res["temas"]["veredicto"] != "balanceado"].copy()
    else:
        susp = pd.DataFrame()
    susp.to_csv(out_dir / "sospechosos_por_tema.csv", index=False)

    (out_dir / "metricas_red.json").write_text(
        json.dumps(res["metricas_red"], indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    # Set de temas sospechosos (no-balanceado) para marcar sus nodos
    if "veredicto" in res["temas"].columns:
        temas_sospechosos_set = set(
            int(t) for t in res["temas"].loc[
                res["temas"]["veredicto"] != "balanceado", "tema_id"
            ]
        )
    else:
        temas_sospechosos_set = set()

    # Render pyvis
    # Construir tablas para inyectar en el HTML
    # Top 30 global: por pagerank
    top_global_df = res["nodos"].nlargest(30, "pagerank")[["palabra", "tema_id", "rol"]]
    top_global: list[list[str]] = [
        [str(r["palabra"]), f"T{int(r['tema_id']):02d} · {r['rol']}"]
        for _, r in top_global_df.iterrows()
    ]

    # Top 10 por tema (LDA): el CSV puede venir en dos formatos:
    #   a) plano con cols termino/peso
    #   b) compacto: tema_id,n_terminos,top_20_terminos (string separado por coma)
    top_by_tema: list[list[str]] = []
    topic_info: dict[str, dict[str, Any]] = {}
    temas_path = sna_dir / "temas_terminos.csv"
    if temas_path.exists():
        tdf = pd.read_csv(temas_path)
        topic_info = build_topic_info(tdf)
        if {"tema_id", "top_20_terminos"}.issubset(tdf.columns):
            for _, r in tdf.iterrows():
                tid = int(r["tema_id"])
                # terminos vienen como "palabra(peso)" o "palabra"
                palabras = str(r["top_20_terminos"]).split(",")
                for p in palabras[:10]:
                    # limpiar peso pegado: "madre(0.052)" -> "madre"
                    limpio = _limpiar_termino(p)
                    top_by_tema.append([limpio, f"T{tid:02d}"])
        else:
            col_pal = "palabra" if "palabra" in tdf.columns else (
                "termino" if "termino" in tdf.columns else None
            )
            col_t = "tema_id" if "tema_id" in tdf.columns else (
                "topic" if "topic" in tdf.columns else None
            )
            col_peso = "peso" if "peso" in tdf.columns else None
            if col_pal and col_t:
                for tid in sorted(tdf[col_t].unique()):
                    if col_peso:
                        top10 = tdf[tdf[col_t] == tid].nlargest(10, col_peso)[col_pal].tolist()
                    else:
                        top10 = tdf[tdf[col_t] == tid][col_pal].tolist()[:10]
                    for p in top10:
                        top_by_tema.append([str(p), f"T{int(tid):02d}"])
    else:
        topic_info = build_topic_info(None)
    add_topic_words_from_nodes(topic_info, res["nodos"], limit=60)

    # Top 10 por subcluster (Louvain)
    top_by_sub: list[list[str]] = []
    sub_path = sna_dir / "subclusters" / "subclusters_palabras.csv"
    if sub_path.exists():
        sdf = pd.read_csv(sub_path)
        lecturas_path = sna_dir / "subclusters" / "subclusters_lectura.csv"
        nombres_sub = {}
        if lecturas_path.exists():
            lecturas = pd.read_csv(lecturas_path)
            nombres_sub = {
                (int(r.tema_id), int(r.sub_id)): str(r.nombre_subtema)
                for r in lecturas.itertuples()
            }
        if {"palabra", "tema_id", "sub_id"}.issubset(sdf.columns):
            for (tid, sid), grp in sdf.groupby(["tema_id", "sub_id"]):
                col_sort = "peso_lda" if "peso_lda" in grp.columns else (
                    "rank_in_sub" if "rank_in_sub" in grp.columns else None
                )
                if col_sort == "rank_in_sub":
                    top10 = grp.sort_values(col_sort, ascending=True).head(10)
                elif col_sort:
                    top10 = grp.nlargest(10, col_sort)
                else:
                    top10 = grp.head(10)
                contexto = f"T{int(tid):02d}/S{int(sid):02d}"
                nombre_sub = nombres_sub.get((int(tid), int(sid)), "")
                if nombre_sub:
                    contexto += f" · {nombre_sub}"
                for _, r in top10.iterrows():
                    top_by_sub.append([str(r["palabra"]), contexto])

    html_path = out_dir / "red_tampico_historico_guiada.html"
    render_pyvis(
            G,
            html_path,
            res["nodos"],
            top_global,
            top_by_tema,
            top_by_sub,
            res["temas"],
            temas_sospechosos_set,
            topic_info,
        )

    print("  Aplicando temas rastreados y polaridad...")
    lexicons = load_lexicons(
        args.diccionario_temas,
        args.diccionario_positivo,
        args.diccionario_negativo,
    )
    annotations = annotate_words(res["nodos"]["palabra"].astype(str), lexicons)
    inject_guided_layer(
        html_path,
        annotations,
        lexicons.category_colors,
        "Temas rastreados y polaridad",
    )
    write_annotation_outputs(
        out_dir,
        annotations,
        "palabras_guiadas",
        {
            "temas": args.diccionario_temas,
            "positivas": args.diccionario_positivo,
            "negativas": args.diccionario_negativo,
        },
    )

    # Resumen en consola
    print("\n[red_completa] OK")
    print(f"  Nodos: {G.number_of_nodes()}")
    print(f"  Aristas: {G.number_of_edges()}")
    print(f"  Moduralidad: {res['metricas_red']['modularidad']}")
    print(f"  Clustering global: {res['metricas_red']['clustering_global']}")
    print(f"  Componentes conexas: {res['metricas_red']['n_componentes_conexas']}")
    print(f"  Salidas en: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
