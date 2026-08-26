#!/usr/bin/env python3
"""Ejecuta de punta a punta el analisis SNA historico de Tampico."""

from __future__ import annotations

import argparse
import shlex
import signal
import subprocess
import sys
from pathlib import Path

from output_naming import (
    build_range_label,
    build_range_report_tag,
    validate_date_range,
    write_range_contract,
)


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = REPO_ROOT / "Scripts"


def build_steps(
    *,
    since: str,
    before: str,
    sin_radar: bool = False,
) -> tuple[list[tuple[str, list[str]]], Path, Path]:
    python = sys.executable
    range_label = build_range_label(since, before)
    range_tag = build_range_report_tag(since, before, "SNA")
    data_dir = REPO_ROOT / "SNA" / "Datos" / range_tag
    results_dir = REPO_ROOT / "SNA" / "Resultados" / range_tag
    input_csv = data_dir / f"tampico_datos_tabulares_{range_label}.csv"
    clusters_dir = results_dir / "clusters"
    accounts_dir = results_dir / "cuentas_clusters"
    scope_short = f"del rango [{since}, {before})"
    network_scope = f"Tampico · [{since}, {before})"
    corpus_label = f"rango local exacto [{since}, {before}) de Tampico"
    steps: list[tuple[str, list[str]]] = [
        (
            "Consolidar rango social exacto",
            [
                python,
                str(SCRIPTS_DIR / "11_consolidar_historico_sna.py"),
                "--since", since,
                "--before", before,
                "--output", str(input_csv),
            ],
        ),
        (
            "Modelar temas con LDA",
            [
                python,
                str(SCRIPTS_DIR / "12_lda_sna.py"),
                "--input-csv", str(input_csv),
                "--output-dir", str(clusters_dir),
                "--k-min", "25",
                "--k-max", "35",
                "--selection-mode", "coherence",
            ],
        ),
        (
            "Evaluar y rotular calidad temática",
            [
                python,
                str(SCRIPTS_DIR / "sna_topic_quality.py"),
                "--clusters-dir", str(clusters_dir),
            ],
        ),
        (
            "Crear subclusters Louvain",
            [
                python,
                str(SCRIPTS_DIR / "12b_subclusters_louvain.py"),
                "--clusters-dir", str(clusters_dir),
                "--resolution", "1.4",
                "--min-sub-size", "3",
            ],
        ),
        (
            "Calcular umbrales de red",
            [
                python,
                str(SCRIPTS_DIR / "12c_diagnostico_umbrales.py"),
                "--clusters-dir", str(clusters_dir),
            ],
        ),
        (
            "Crear red completa",
            [
                python,
                str(SCRIPTS_DIR / "12c_red_completa.py"),
                "--clusters-dir", str(clusters_dir),
                "--output-filename", f"red_tampico_{range_label}.html",
                "--scope-label", scope_short,
            ],
        ),
        (
            "Mapear cuentas a clusters",
            [
                python,
                str(SCRIPTS_DIR / "18_cuentas_clusters.py"),
                "--clusters-dir", str(clusters_dir),
                "--output-dir", str(accounts_dir),
            ],
        ),
        (
            "Crear red de cuentas",
            [
                python,
                str(SCRIPTS_DIR / "12d_red_cuentas.py"),
                "--base-dir", str(results_dir),
                "--output-filename", f"red_tampico_cuentas_{range_label}.html",
                "--scope-label", scope_short,
                "--corpus-label", corpus_label,
            ],
        ),
        (
            "Crear red de posiciones discursivas",
            [
                python,
                str(SCRIPTS_DIR / "19_red_posiciones_discursivas.py"),
                "--base-dir", str(results_dir),
                "--input-csv", str(input_csv),
                "--output-filename", f"red_tampico_posiciones_{range_label}.html",
                "--scope-label", network_scope,
                "--corpus-label", corpus_label,
            ],
        ),
        (
            "Crear red completa guiada",
            [
                python,
                str(SCRIPTS_DIR / "12c_red_completa_guiada.py"),
                "--clusters-dir", str(clusters_dir),
                "--output-filename", f"red_tampico_{range_label}_guiada.html",
                "--scope-label", scope_short,
            ],
        ),
        (
            "Crear red de cuentas guiada",
            [
                python,
                str(SCRIPTS_DIR / "12d_red_cuentas_guiada.py"),
                "--base-dir", str(results_dir),
                "--output-filename", f"red_tampico_cuentas_{range_label}_guiada.html",
                "--scope-label", scope_short,
                "--corpus-label", corpus_label,
            ],
        ),
        (
            "Crear red de posiciones guiada",
            [
                python,
                str(SCRIPTS_DIR / "19_red_posiciones_guiada.py"),
                "--base-dir", str(results_dir),
                "--input-csv", str(input_csv),
                "--output-filename", f"red_tampico_posiciones_{range_label}_guiada.html",
                "--scope-label", network_scope,
                "--corpus-label", corpus_label,
                "--positions-per-topic", "5",
                "--words-per-position", "60",
            ],
        ),
    ]
    if sin_radar:
        steps[0][1].append("--sin-radar")
    return steps, data_dir, results_dir


def render_command(command: list[str]) -> str:
    return " ".join(shlex.quote(part) for part in command)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", required=True, help="Límite inicial inclusivo YYYY-MM-DD")
    parser.add_argument("--before", required=True, help="Límite final exclusivo YYYY-MM-DD")
    parser.add_argument(
        "--sin-radar",
        action="store_true",
        help="Genera el corpus solo con las fuentes del repositorio de Tampico",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Muestra la secuencia sin ejecutar los scripts",
    )
    args = parser.parse_args()

    try:
        validate_date_range(args.since, args.before)
    except ValueError as exc:
        parser.error(str(exc))

    steps, data_dir, results_dir = build_steps(
        since=args.since,
        before=args.before,
        sin_radar=args.sin_radar,
    )
    print("=" * 70, flush=True)
    print("GENERACION DEL ANALISIS SNA DE RANGO EXACTO DE TAMPICO", flush=True)
    print(f"Contrato: [{args.since}, {args.before})", flush=True)
    print(f"Pasos: {len(steps)}", flush=True)
    print("=" * 70, flush=True)

    if not args.dry_run:
        write_range_contract(data_dir, args.since, args.before, "SNA")
        write_range_contract(results_dir, args.since, args.before, "SNA")

    running_process: subprocess.Popen[bytes] | None = None

    def stop_current_step(signum: int, _frame: object) -> None:
        if running_process is not None and running_process.poll() is None:
            running_process.terminate()
            try:
                running_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                running_process.kill()
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, stop_current_step)

    for index, (label, command) in enumerate(steps, 1):
        print(f"\n[{index}/{len(steps)}] {label}", flush=True)
        print(f"Comando: {render_command(command)}", flush=True)
        if args.dry_run:
            continue
        running_process = subprocess.Popen(command, cwd=REPO_ROOT)
        return_code = running_process.wait()
        running_process = None
        if return_code != 0:
            print(
                f"\nERROR: '{label}' termino con codigo {return_code}.",
                file=sys.stderr,
                flush=True,
            )
            return return_code

    if args.dry_run:
        print("\nDry run finalizado.", flush=True)
    else:
        print(
            f"\nAnalisis SNA finalizado. Resultados en {results_dir}.",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
