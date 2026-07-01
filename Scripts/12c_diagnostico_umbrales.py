#!/usr/bin/env python3
"""Diagnostica pesos por capa y propone umbrales para la red de Tampico."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CLUSTERS = REPO_ROOT / "SNA" / "Resultados" / "historico" / "clusters"
PERCENTILES = (0.50, 0.75, 0.90, 0.95, 0.99)


def cargar_capa(path: Path, columnas: list[str]) -> pd.DataFrame:
    archivos = sorted(path.glob("tema_*.csv"))
    if not archivos:
        return pd.DataFrame(columns=columnas + ["tema_id"])
    return pd.concat(
        [pd.read_csv(p).assign(tema_id=int(p.stem.split("_")[1])) for p in archivos],
        ignore_index=True,
    )


def resumen_pesos(df: pd.DataFrame) -> dict[str, float | int]:
    if df.empty:
        return {"n_aristas": 0}
    pesos = pd.to_numeric(df["weight"], errors="coerce").dropna()
    data: dict[str, float | int] = {
        "n_aristas": int(len(pesos)),
        "min": float(pesos.min()),
        "media": float(pesos.mean()),
        "max": float(pesos.max()),
    }
    for q in PERCENTILES:
        data[f"p{int(q * 100):02d}"] = float(pesos.quantile(q))
    return data


def clasificar_intra(intra: pd.DataFrame, subs: pd.DataFrame) -> pd.DataFrame:
    pal2sub = {
        str(r.palabra): (int(r.tema_id), int(r.sub_id))
        for r in subs.itertuples()
    }

    def capa(row: pd.Series) -> str:
        tema = int(row["tema_id"])
        ta, sa = pal2sub.get(str(row["source"]), (tema, -1))
        tb, sb = pal2sub.get(str(row["target"]), (tema, -1))
        if ta != tema or tb != tema or sa < 0 or sb < 0:
            return "sin_subcluster"
        return "intra_sub" if sa == sb else "intra_cluster"

    result = intra.copy()
    if not result.empty:
        result["capa"] = result.apply(capa, axis=1)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clusters-dir", type=Path, default=DEFAULT_CLUSTERS)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    clusters = args.clusters_dir
    output = args.output or clusters / "diagnostico_umbrales.json"
    intra = cargar_capa(clusters / "intracluster", ["source", "target", "weight"])
    extra = cargar_capa(
        clusters / "extracluster",
        ["termino_interno", "termino_externo", "weight"],
    )
    sub_path = clusters / "subclusters" / "subclusters_palabras.csv"
    if not sub_path.exists():
        raise SystemExit(f"Falta ejecutar subclusters: {sub_path}")
    intra = clasificar_intra(intra, pd.read_csv(sub_path))

    por_capa = {
        capa: resumen_pesos(grupo)
        for capa, grupo in intra.groupby("capa")
    }
    por_capa["extra"] = resumen_pesos(extra)
    sugeridos = {
        "umbral_intra_sub": por_capa.get("intra_sub", {}).get("p75"),
        "umbral_intra_cluster": por_capa.get("intra_cluster", {}).get("p75"),
        "umbral_extra": por_capa["extra"].get("p75"),
        "criterio": "percentil 75 de cada capa",
    }
    resultado = {
        "corpus": "historico consolidado de Tampico",
        "clusters_dir": str(clusters),
        "capas": por_capa,
        "umbrales_sugeridos": sugeridos,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(resultado, indent=2, ensure_ascii=False), encoding="utf-8")

    print(json.dumps(resultado, indent=2, ensure_ascii=False))
    print(f"Diagnostico guardado en: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
