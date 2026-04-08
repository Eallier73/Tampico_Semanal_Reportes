#!/usr/bin/env python3
"""
Consolida los .txt de cada extractor en dos archivos de análisis:

  material_institucional.txt  <- posts oficiales (Twitter, Facebook, YouTube scripts)
  material_comentarios.txt    <- reacciones ciudadanas (Twitter, Facebook, YouTube comentarios, Medios)

Uso:
  python 6_consolidador_datos.py --since 2026-03-30 --before 2026-04-05

  El script infiere la semana ISO desde --since y busca los archivos en las
  carpetas de cada red (relativas a la raíz del repo o a --base-dir).
  La salida va a Datos/{semana_tag}/.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# Importar build_report_tag desde el mismo directorio
sys.path.insert(0, str(REPO_ROOT / "Scripts"))
from output_naming import build_report_tag


# ---------------------------------------------------------------------------
# Definición de fuentes
# ---------------------------------------------------------------------------

def _sources(since: str, base_dir: Path) -> dict[str, list[Path]]:
    """
    Devuelve dos listas de paths (pueden no existir):
      "institucional": posts oficiales
      "comentarios":   reacciones ciudadanas
    """
    tw  = build_report_tag(since, "Twitter")
    fb  = build_report_tag(since, "Facebook")
    yt  = build_report_tag(since, "Youtube")
    med = build_report_tag(since, "Medios")

    institucional = [
        base_dir / "Twitter" / tw  / f"{tw}_post_institucionales.txt",
        base_dir / "Facebook" / fb / f"{fb}_posts.txt",
        base_dir / "Youtube"  / yt / f"{yt}_scripts.txt",
    ]

    comentarios = [
        base_dir / "Twitter"  / tw  / f"{tw}_comentarios.txt",
        base_dir / "Facebook" / fb  / f"{fb}_comentarios.txt",
        base_dir / "Youtube"  / yt  / f"{yt}_comentarios.txt",
        base_dir / "Medios"   / med / f"noticias_tampico_{med}.txt",
    ]

    return {"institucional": institucional, "comentarios": comentarios}


# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------

def consolidar(paths: list[Path]) -> tuple[list[str], list[str]]:
    """Lee y limpia líneas de cada archivo. Devuelve (líneas, advertencias)."""
    lines: list[str] = []
    warnings: list[str] = []

    for path in paths:
        if not path.exists():
            warnings.append(f"  ⚠️  No encontrado: {path}")
            continue
        with open(path, encoding="utf-8") as f:
            raw = f.readlines()
        kept = [l.rstrip("\n") for l in raw if l.strip()]
        lines.extend(kept)
        print(f"  ✅ {path.name}: {len(kept)} líneas")

    return lines, warnings


def escribir(lines: list[str], dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
        if lines:
            f.write("\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def valid_date(value: str) -> str:
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Fecha inválida '{value}', usa YYYY-MM-DD") from exc
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consolida .txt de todos los extractores en material_institucional.txt y material_comentarios.txt"
    )
    parser.add_argument("--since", required=True, type=valid_date,
                        help="Fecha inicio YYYY-MM-DD (define la semana ISO)")
    parser.add_argument("--before", required=True, type=valid_date,
                        help="Fecha fin YYYY-MM-DD (heredado del orquestador)")
    parser.add_argument("--base-dir", default=str(REPO_ROOT),
                        help=f"Raíz del repositorio (default: {REPO_ROOT})")
    parser.add_argument("--output-dir", default=None,
                        help="Carpeta base de salida (default: <base-dir>/Datos)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_dir = Path(args.base_dir)
    output_base = Path(args.output_dir) if args.output_dir else base_dir / "Datos"

    datos_tag = build_report_tag(args.since, "Datos")
    output_dir = output_base / datos_tag

    print("\n" + "=" * 70)
    print("📦 CONSOLIDADOR DE DATOS SEMANALES")
    print("=" * 70)
    print(f"Semana : {datos_tag}")
    print(f"Salida : {output_dir}")

    sources = _sources(args.since, base_dir)

    # ── Material institucional ──
    print("\n── Material institucional ──")
    inst_lines, inst_warn = consolidar(sources["institucional"])
    for w in inst_warn:
        print(w)

    inst_path = output_dir / "material_institucional.txt"
    escribir(inst_lines, inst_path)
    print(f"\n  📄 {inst_path.name}: {len(inst_lines)} líneas totales")

    # ── Material comentarios ──
    print("\n── Material comentarios ──")
    com_lines, com_warn = consolidar(sources["comentarios"])
    for w in com_warn:
        print(w)

    com_path = output_dir / "material_comentarios.txt"
    escribir(com_lines, com_path)
    print(f"\n  📄 {com_path.name}: {len(com_lines)} líneas totales")

    print("\n" + "=" * 70)
    print("✅ CONSOLIDACIÓN COMPLETADA")
    print("=" * 70)
    print(f"  {inst_path}")
    print(f"  {com_path}")


if __name__ == "__main__":
    main()
