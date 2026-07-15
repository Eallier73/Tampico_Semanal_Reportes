#!/usr/bin/env python3
"""Ejecuta de punta a punta el analisis SNA historico de Tampico."""

from __future__ import annotations

import argparse
import shlex
import signal
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = REPO_ROOT / "Scripts"


def build_steps(*, sin_radar: bool = False) -> list[tuple[str, list[str]]]:
    python = sys.executable
    steps: list[tuple[str, list[str]]] = [
        (
            "Consolidar historico social",
            [python, str(SCRIPTS_DIR / "11_consolidar_historico_sna.py")],
        ),
        (
            "Modelar temas con LDA",
            [
                python,
                str(SCRIPTS_DIR / "12_lda_sna.py"),
                "--k-min", "25",
                "--k-max", "35",
                "--selection-mode", "informative",
            ],
        ),
        (
            "Crear subclusters Louvain",
            [
                python,
                str(SCRIPTS_DIR / "12b_subclusters_louvain.py"),
                "--resolution", "1.4",
                "--min-sub-size", "3",
            ],
        ),
        (
            "Calcular umbrales de red",
            [python, str(SCRIPTS_DIR / "12c_diagnostico_umbrales.py")],
        ),
        (
            "Crear red completa",
            [python, str(SCRIPTS_DIR / "12c_red_completa.py")],
        ),
        (
            "Mapear cuentas a clusters",
            [python, str(SCRIPTS_DIR / "18_cuentas_clusters.py")],
        ),
        (
            "Crear red de cuentas",
            [python, str(SCRIPTS_DIR / "12d_red_cuentas.py")],
        ),
        (
            "Crear red de posiciones discursivas",
            [python, str(SCRIPTS_DIR / "19_red_posiciones_discursivas.py")],
        ),
        (
            "Crear red completa guiada",
            [python, str(SCRIPTS_DIR / "12c_red_completa_guiada.py")],
        ),
        (
            "Crear red de cuentas guiada",
            [python, str(SCRIPTS_DIR / "12d_red_cuentas_guiada.py")],
        ),
        (
            "Crear red de posiciones guiada",
            [python, str(SCRIPTS_DIR / "19_red_posiciones_guiada.py")],
        ),
    ]
    if sin_radar:
        steps[0][1].append("--sin-radar")
    return steps


def render_command(command: list[str]) -> str:
    return " ".join(shlex.quote(part) for part in command)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
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

    steps = build_steps(sin_radar=args.sin_radar)
    print("=" * 70, flush=True)
    print("GENERACION DEL ANALISIS SNA HISTORICO DE TAMPICO", flush=True)
    print(f"Pasos: {len(steps)}", flush=True)
    print("=" * 70, flush=True)

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
            "\nAnalisis SNA finalizado. Resultados en SNA/Resultados/historico/.",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
