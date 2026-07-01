#!/usr/bin/env python3
"""
SNA Fase 2.5: Sub-clustering dentro de cada tema LDA.

Para cada tema del LDA (clusters/intracluster/tema_XX.csv), carga las
aristas internas (coocurrencia ventana=3), construye un grafo no-dirigido
con networkx, y detecta comunidades (sub-clusters) usando Louvain
(python-louvain / `community`).

Salidas en SNA/Resultados/historico/clusters/subclusters/:
  - subclusters_resumen.csv: tema_id, n_subclusters, n_palabras_total
  - subcluster_temaXX_subYY.csv: palabras + peso del subcluster
  - subclusters_paleta.csv: id_global, tema_id, sub_id, hex_color
  - reporte_subclusters.md: lectura politica corta por tema

Uso:
  .venv/bin/python Scripts/12b_subclusters_louvain.py
"""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CLUSTERS = REPO_ROOT / "SNA" / "Resultados" / "historico" / "clusters"


def _cargar_intra(tema_path: Path) -> pd.DataFrame:
    """Lee intracluster/tema_XX.csv (columnas: source, target, weight)."""
    if not tema_path.exists():
        return pd.DataFrame(columns=["source", "target", "weight"])
    return pd.read_csv(tema_path)


def _subclusters_louvain(
    df_intra: pd.DataFrame,
    palabras_tema: set[str],
    resolution: float,
):
    """Aplica Louvain al grafo de coocurrencia intracluster."""
    import networkx as nx  # noqa: PLC0415
    try:
        import community as community_louvain  # noqa: PLC0415
    except ImportError as exc:
        raise SystemExit(
            "Falta python-louvain. Instala con: pip install python-louvain"
        ) from exc

    if df_intra.empty:
        return [], {}

    G = nx.Graph()
    for _, row in df_intra.iterrows():
        a = str(row["source"]).strip()
        b = str(row["target"]).strip()
        w = float(row.get("weight", 1) or 1)
        if not a or not b:
            continue
        if a == b:
            continue
        # Solo nodos que pertenecen al tema (por seguridad)
        if a in palabras_tema and b in palabras_tema:
            if G.has_edge(a, b):
                G[a][b]["weight"] += w
            else:
                G.add_edge(a, b, weight=w)

    if G.number_of_edges() == 0:
        return [], {}

    # Filtrar nodos aislados (sin aristas)
    G.remove_nodes_from(list(nx.isolates(G)))
    if G.number_of_edges() == 0:
        return [], {}

    # Louvain sobre el grafo pesado. resolution controla granularidad.
    particion = community_louvain.best_partition(
        G, weight="weight", random_state=42, resolution=resolution
    )

    # Agrupar por comunidad
    comunidades: dict[int, list[str]] = {}
    for nodo, cid in particion.items():
        comunidades.setdefault(cid, []).append(nodo)

    # Stats: tamano y densidad
    sub_info = []
    for cid, nodos in comunidades.items():
        subG = G.subgraph(nodos)
        sub_info.append({
            "sub_id": cid,
            "n_palabras": len(nodos),
            "n_aristas": subG.number_of_edges(),
            "densidad": nx.density(subG),
            "peso_total": float(sum(d.get("weight", 0) for _, _, d in subG.edges(data=True))),
            "palabras": sorted(nodos, key=lambda w: -G.degree(w, weight="weight")),
        })

    # Ordenar sub-clusters por peso_total DESC
    sub_info.sort(key=lambda x: -x["peso_total"])
    # Renumerar 0..N-1 para que el sub_id externo sea estable
    for i, info in enumerate(sub_info):
        info["sub_id"] = i

    return sub_info, {"n_nodos": G.number_of_nodes(), "n_aristas": G.number_of_edges()}


def _leer_palabras_tema(corpus_csv: Path, tema_id: int) -> set[str]:
    """Carga las palabras asignadas al tema desde lda_asignacion.csv."""
    df = pd.read_csv(corpus_csv)
    return set(df.loc[df["tema_id"] == tema_id, "termino"].astype(str).tolist())


def _tabla_top_pesos(corpus_csv: Path) -> pd.DataFrame:
    """Carga lda_asignacion.csv (tema_id, termino, peso)."""
    return pd.read_csv(corpus_csv)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--clusters-dir", type=Path, default=DEFAULT_CLUSTERS)
    parser.add_argument(
        "--resolution", type=float, default=1.4,
        help="Resolution de Louvain (mas alto = mas sub-clusters)",
    )
    parser.add_argument(
        "--min-sub-size", type=int, default=3,
        help="Sub-clusters con menos palabras se descartan como ruido",
    )
    parser.add_argument(
        "--max-words-per-subcluster", type=int, default=0,
        help="Palabras guardadas por subcluster; 0 conserva todas",
    )
    args = parser.parse_args()

    base = args.clusters_dir
    intra_dir = base / "intracluster"
    if not intra_dir.exists():
        print(f"No existe {intra_dir}")
        return 2

    lda_asig = base / "lda_asignacion.csv"
    if not lda_asig.exists():
        print(f"No existe {lda_asig}")
        return 3

    out_dir = base / "subclusters"
    out_dir.mkdir(parents=True, exist_ok=True)

    df_pesos = _tabla_top_pesos(lda_asig)
    # pesos por (tema, termino)
    pesos_idx = df_pesos.set_index(["tema_id", "termino"])["peso"].to_dict()

    resumen_rows = []
    paleta_rows = []
    detalle_rows = []
    lectura_rows = []

    # Identificar todos los temas presentes en lda_asignacion.csv
    temas = sorted(df_pesos["tema_id"].unique().tolist())
    print(f"[subclusters] {len(temas)} temas en lda_asignacion.csv")
    print(f"[subclusters] resolution={args.resolution} min_sub_size={args.min_sub_size}")

    for tema_id in temas:
        palabras = _leer_palabras_tema(lda_asig, tema_id)
        intra_path = intra_dir / f"tema_{tema_id:02d}.csv"
        df_intra = _cargar_intra(intra_path)

        sub_info, meta = _subclusters_louvain(df_intra, palabras, args.resolution)

        # Filtrar sub-chicos
        sub_info = [s for s in sub_info if s["n_palabras"] >= args.min_sub_size]

        print(
            f"  Tema {tema_id}: {meta.get('n_nodos', 0)} nodos, "
            f"{meta.get('n_aristas', 0)} aristas -> "
            f"{len(sub_info)} sub-clusters"
        )

        resumen_rows.append({
            "tema_id": tema_id,
            "n_palabras": len(palabras),
            "n_nodos_grafo": meta.get("n_nodos", 0),
            "n_aristas_grafo": meta.get("n_aristas", 0),
            "n_subclusters": len(sub_info),
        })

        # Paleta consistente con CMAP_TAB10 del resto del pipeline
        CMAP_TAB10 = [
            "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
            "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
            "#aec7e8", "#ffbb78", "#98df8a", "#ff9896", "#c5b0d5",
        ]
        # Color base del tema = indice tema_id, sub-color = gradiente del base
        color_tema = CMAP_TAB10[tema_id % len(CMAP_TAB10)]

        for sub in sub_info:
            sid = sub["sub_id"]
            palabras_lectura = sub["palabras"][:12]
            nombre_subtema = ", ".join(palabras_lectura[:4]).capitalize()
            resumen_subtema = (
                f"Subtema T{tema_id:02d}.S{sid:02d} centrado en "
                f"{', '.join(palabras_lectura[:6])}; conecta tambien "
                f"{', '.join(palabras_lectura[6:12])}."
            ).replace("; conecta tambien .", ".")
            # Sub-color: mezclar tema con blanco segun peso
            shade = 0.4 + 0.5 * (sid / max(1, len(sub_info)))
            sub_color = color_tema  # placeholder: mantenemos color del tema
            paleta_rows.append({
                "id_global": f"T{tema_id:02d}_S{sid:02d}",
                "tema_id": tema_id,
                "sub_id": sid,
                "hex_color": sub_color,
                "n_palabras": sub["n_palabras"],
                "n_aristas": sub["n_aristas"],
                "densidad": round(sub["densidad"], 3),
                "peso_total": round(sub["peso_total"], 1),
                "nombre_subtema": nombre_subtema,
            })
            lectura_rows.append({
                "id_global": f"T{tema_id:02d}_S{sid:02d}",
                "tema_id": tema_id,
                "sub_id": sid,
                "nombre_subtema": nombre_subtema,
                "resumen_subtema": resumen_subtema,
                "top_terminos": ", ".join(palabras_lectura),
                "n_palabras": sub["n_palabras"],
                "n_aristas": sub["n_aristas"],
                "densidad": round(sub["densidad"], 3),
                "peso_total": round(sub["peso_total"], 1),
            })

            # detalle: palabras + peso LDA
            palabras_detalle = sub["palabras"]
            if args.max_words_per_subcluster > 0:
                palabras_detalle = palabras_detalle[:args.max_words_per_subcluster]
            for rank, pal in enumerate(palabras_detalle):
                w_lda = pesos_idx.get((tema_id, pal), 0.0)
                detalle_rows.append({
                    "tema_id": tema_id,
                    "sub_id": sid,
                    "palabra": pal,
                    "peso_lda": round(float(w_lda), 4),
                    "grado_in_sub": None,  # se calcula abajo si queremos
                    "rank_in_sub": rank,
                })

    # Guardar outputs
    pd.DataFrame(resumen_rows).to_csv(out_dir / "subclusters_resumen.csv", index=False)
    pd.DataFrame(paleta_rows).to_csv(out_dir / "subclusters_paleta.csv", index=False)
    pd.DataFrame(detalle_rows).to_csv(out_dir / "subclusters_palabras.csv", index=False)
    pd.DataFrame(lectura_rows).to_csv(out_dir / "subclusters_lectura.csv", index=False)

    # reporte markdown corto
    md = ["# Subclusters Louvain por tema LDA", ""]
    md.append(f"- Generado: {datetime.now().isoformat(timespec='seconds')}")
    md.append("- Corpus: historico consolidado de Tampico")
    md.append(f"- Resolution: {args.resolution}")
    md.append(f"- Min sub-cluster size: {args.min_sub_size}")
    md.append("")
    md.append("| Tema | n_palabras | n_nodos | n_aristas | n_subclusters |")
    md.append("|---:|---:|---:|---:|---:|")
    for r in resumen_rows:
        md.append(
            f"| T{r['tema_id']:02d} | {r['n_palabras']} | {r['n_nodos_grafo']} | "
            f"{r['n_aristas_grafo']} | {r['n_subclusters']} |"
        )
    md.append("")
    md.append("## Lectura rapida por tema")
    md.append("")
    for r in resumen_rows:
        tid = r["tema_id"]
        subs = [p for p in paleta_rows if p["tema_id"] == tid]
        if not subs:
            continue
        md.append(f"### T{tid:02d} ({r['n_palabras']} palabras, {r['n_subclusters']} sub-clusters)")
        for s in subs:
            pals_sub = [
                d["palabra"] for d in detalle_rows
                if d["tema_id"] == tid and d["sub_id"] == s["sub_id"]
            ]
            pals_top = pals_sub[:12]
            md.append(
                f"- **T{tid:02d}.S{s['sub_id']:02d} - {s['nombre_subtema']}** "
                f"({s['n_palabras']} pal, {s['n_aristas']} aristas, "
                f"densidad {s['densidad']:.2f}): "
                f"{', '.join(pals_top)}"
            )
        md.append("")

    (out_dir / "reporte_subclusters.md").write_text("\n".join(md), encoding="utf-8")
    print(f"\n[subclusters] guardado en: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
