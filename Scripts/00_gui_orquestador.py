#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import threading
import tkinter as tk
from datetime import datetime, timedelta
from pathlib import Path
from tkinter import messagebox, scrolledtext, ttk

SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from download_history import latest_downloads_by_pipeline, read_download_history
from output_naming import build_range_label, build_range_report_tag, write_range_contract
from sna_recent_ranges import resolve_recent_scope


def manual_load_dotenv(path: Path) -> bool:
    try:
        if not path.exists():
            return False
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                os.environ[key.strip()] = value.strip().strip("'").strip('"')
        return True
    except Exception:
        return False


REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = REPO_ROOT / ".env.local"

try:
    from dotenv import load_dotenv

    if ENV_FILE.exists():
        load_dotenv(str(ENV_FILE))
except ImportError:
    manual_load_dotenv(ENV_FILE)


def load_orquestador_module():
    module_path = Path(__file__).resolve().parent / "00_orquestador_general.py"
    scripts_dir = str(module_path.parent)
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    spec = importlib.util.spec_from_file_location("tampico_orquestador_general", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"No se pudo cargar el orquestador desde {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


ORQ = load_orquestador_module()
PIPELINES = ORQ.PIPELINES
PIPELINES_BY_CODE = ORQ.PIPELINES_BY_CODE
_TODAY = datetime.now().date()
_CURRENT_RANGE_START = _TODAY - timedelta(days=_TODAY.weekday())
DEFAULT_GLOBAL_SINCE = _CURRENT_RANGE_START.isoformat()
DEFAULT_GLOBAL_BEFORE = (_CURRENT_RANGE_START + timedelta(days=7)).isoformat()

def build_sna_run(
    scope: str,
    *,
    repo_root: Path = REPO_ROOT,
    now: datetime | None = None,
) -> dict[str, object]:
    """Construye la cadena SNA y mantiene aislados corpus, resultados y HTML."""
    sna_data_dir = repo_root / "SNA" / "Datos"
    sna_results_root = repo_root / "SNA" / "Resultados"
    exact_since: str | None = None
    exact_before: str | None = None
    selected_ranges: list[dict[str, object]] = []
    if scope == "historico":
        label = "material histórico (fuentes locales + RAdAR)"
        input_csv = sna_data_dir / "tampico_datos_tabulares_consolidados.csv"
        results_dir = sna_results_root / "historico"
        consolidate_args = ["--output", str(input_csv)]
        scope_short = "histórico"
        network_scope = "Tampico histórico"
        accounts_scope = "histórica"
        corpus_label = "histórico consolidado de Tampico con RAdAR"
        filename_scope = "historico"
        log_name = "ultima_ejecucion.log"
    elif scope in {"ultimos_2_rangos", "ultimo_rango"}:
        count = 2 if scope == "ultimos_2_rangos" else 1
        recent = resolve_recent_scope(repo_root, count)
        exact_since = recent.since.isoformat()
        exact_before = recent.before.isoformat()
        range_label = build_range_label(exact_since, exact_before)
        range_tag = build_range_report_tag(exact_since, exact_before, "SNA")
        execution_time = now or datetime.now()
        execution_id = execution_time.strftime("ejecucion_%Y%m%dT%H%M%S_%f")
        input_dir = sna_data_dir / range_tag / execution_id
        input_csv = input_dir / f"tampico_datos_tabulares_{range_label}.csv"
        results_dir = sna_results_root / range_tag / execution_id
        consolidate_args = [
            "--since", exact_since,
            "--before", exact_before,
            "--output", str(input_csv),
        ]
        selected_ranges = [
            {
                "since": item.since.isoformat(),
                "before": item.before.isoformat(),
                "identity": item.identity,
                "sources": list(item.sources),
                "inferred_from_rows": item.inferred_from_rows,
            }
            for item in recent.selected_ranges
        ]
        selection_label = "2 rangos más recientes" if count == 2 else "rango más reciente"
        label = f"{selection_label} · cobertura [{exact_since}, {exact_before})"
        scope_short = f"de [{exact_since}, {exact_before})"
        network_scope = f"Tampico · [{exact_since}, {exact_before})"
        accounts_scope = scope_short
        corpus_label = f"fuentes locales de Tampico en [{exact_since}, {exact_before})"
        filename_scope = range_label
        log_name = f"ejecucion_sna_{range_label}.log"
    else:
        raise ValueError(f"Alcance SNA desconocido: {scope}")

    clusters_dir = results_dir / "clusters"
    accounts_dir = results_dir / "cuentas_clusters"
    complete_name = f"red_tampico_{filename_scope}.html"
    accounts_name = f"red_tampico_cuentas_{filename_scope}.html"
    positions_name = f"red_tampico_posiciones_{filename_scope}.html"
    guided_complete_name = f"red_tampico_{filename_scope}_guiada.html"
    guided_accounts_name = f"red_tampico_cuentas_{filename_scope}_guiada.html"
    guided_positions_name = f"red_tampico_posiciones_{filename_scope}_guiada.html"

    # Conserva los nombres históricos actuales para no romper marcadores o
    # vínculos ya usados, pero los alcances recientes siempre tienen nombres propios.
    if scope == "historico":
        accounts_name = "red_tampico_cuentas.html"
        positions_name = "red_tampico_posiciones.html"
        guided_complete_name = "red_tampico_historico_guiada.html"
        guided_accounts_name = "red_tampico_cuentas_guiada.html"
        guided_positions_name = "red_tampico_posiciones_guiada.html"

    steps = [
        ("Consolidar corpus SNA", "11_consolidar_historico_sna.py", consolidate_args),
        (
            "Modelar temas LDA",
            "12_lda_sna.py",
            [
                "--input-csv", str(input_csv),
                "--output-dir", str(clusters_dir),
                "--k-min", "25", "--k-max", "35",
                "--selection-mode", "coherence",
            ],
        ),
        (
            "Evaluar calidad temática",
            "sna_topic_quality.py",
            ["--clusters-dir", str(clusters_dir)],
        ),
        (
            "Calcular subclusters Louvain",
            "12b_subclusters_louvain.py",
            [
                "--clusters-dir", str(clusters_dir),
                "--resolution", "1.4", "--min-sub-size", "3",
            ],
        ),
        (
            "Diagnosticar umbrales",
            "12c_diagnostico_umbrales.py",
            ["--clusters-dir", str(clusters_dir)],
        ),
        (
            "Generar red completa",
            "12c_red_completa.py",
            [
                "--clusters-dir", str(clusters_dir),
                "--output-filename", complete_name,
                "--scope-label", scope_short,
            ],
        ),
        (
            "Mapear cuentas a clusters",
            "18_cuentas_clusters.py",
            [
                "--clusters-dir", str(clusters_dir),
                "--output-dir", str(accounts_dir),
            ],
        ),
        (
            "Generar red de cuentas",
            "12d_red_cuentas.py",
            [
                "--base-dir", str(results_dir),
                "--output-filename", accounts_name,
                "--scope-label", accounts_scope,
                "--corpus-label", corpus_label,
            ],
        ),
        (
            "Generar red de posiciones discursivas",
            "19_red_posiciones_discursivas.py",
            [
                "--base-dir", str(results_dir),
                "--input-csv", str(input_csv),
                "--output-filename", positions_name,
                "--scope-label", network_scope,
                "--corpus-label", corpus_label,
            ],
        ),
        (
            "Generar red completa guiada",
            "12c_red_completa_guiada.py",
            [
                "--clusters-dir", str(clusters_dir),
                "--output-filename", guided_complete_name,
                "--scope-label", scope_short,
            ],
        ),
        (
            "Generar red de cuentas guiada",
            "12d_red_cuentas_guiada.py",
            [
                "--base-dir", str(results_dir),
                "--output-filename", guided_accounts_name,
                "--scope-label", accounts_scope,
                "--corpus-label", corpus_label,
            ],
        ),
        (
            "Generar red de posiciones guiada",
            "19_red_posiciones_guiada.py",
            [
                "--base-dir", str(results_dir),
                "--input-csv", str(input_csv),
                "--output-filename", guided_positions_name,
                "--scope-label", network_scope,
                "--corpus-label", corpus_label,
                "--positions-per-topic", "5",
                "--words-per-position", "60",
            ],
        ),
    ]
    guided_dir = clusters_dir / "red_guiada"
    return {
        "scope": scope,
        "label": label,
        "input_csv": input_csv,
        "results_dir": results_dir,
        "run_log": results_dir / log_name,
        "steps": steps,
        "final_outputs": [
            guided_dir / guided_complete_name,
            guided_dir / guided_accounts_name,
            guided_dir / guided_positions_name,
        ],
        "since": exact_since,
        "before": exact_before,
        "selected_ranges": selected_ranges,
    }


def write_sna_run_manifest(run: dict[str, object]) -> Path | None:
    """Registra el alcance reciente sin modificar los CSV fuente."""
    since = run.get("since")
    before = run.get("before")
    if not isinstance(since, str) or not isinstance(before, str):
        return None
    results_dir = Path(run["results_dir"])
    write_range_contract(results_dir, since, before, "SNA")
    path = results_dir / "manifiesto_ejecucion_sna.json"
    payload = {
        "manifest_version": 1,
        "scope": run.get("scope"),
        "label": run.get("label"),
        "since": since,
        "before": before,
        "interval": "[since,before)",
        "selected_ranges": run.get("selected_ranges", []),
        "input_csv": str(run["input_csv"]),
        "results_dir": str(results_dir),
    }
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return path


def validate_date(value: str) -> str:
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d").date().isoformat()
    except ValueError as exc:
        raise ValueError(f"Fecha invalida '{value}', usa YYYY-MM-DD") from exc


def parse_date_range(since: str, before: str) -> tuple[str, str]:
    parsed_since = validate_date(since)
    parsed_before = validate_date(before)
    if parsed_since >= parsed_before:
        raise ValueError(
            "La fecha final exclusiva debe ser posterior a la fecha inicial."
        )
    return parsed_since, parsed_before


def ensure_pipeline_before(selected, dependency_code: str, target_code: str):
    dependency = next((item for item in selected if item.code == dependency_code), None)
    target_index = next((index for index, item in enumerate(selected) if item.code == target_code), None)
    dependency_index = next((index for index, item in enumerate(selected) if item.code == dependency_code), None)
    if dependency is None or target_index is None or dependency_index is None or dependency_index < target_index:
        return selected
    selected.pop(dependency_index)
    target_index = next(index for index, item in enumerate(selected) if item.code == target_code)
    selected.insert(target_index, dependency)
    return selected


def ensure_pipeline_after(selected, target_code: str, dependency_codes: list[str]):
    target = next((item for item in selected if item.code == target_code), None)
    if target is None:
        return selected

    target_index = next(index for index, item in enumerate(selected) if item.code == target_code)
    required_indexes = [
        index for index, item in enumerate(selected)
        if item.code in dependency_codes
    ]
    if not required_indexes or target_index > max(required_indexes):
        return selected

    selected.pop(target_index)
    insert_at = max(index for index, item in enumerate(selected) if item.code in dependency_codes) + 1
    selected.insert(insert_at, target)
    return selected


class OrquestadorGUI:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Orquestador Pipelines Tampico")
        self.root.geometry("940x900")

        self.running_process: subprocess.Popen[str] | None = None
        self.stop_requested = False
        self.venv_python = self.detect_venv()

        self.setup_ui()

    def detect_venv(self) -> str:
        for folder in (".venv", "venv"):
            for candidate in (
                REPO_ROOT / folder / "bin" / "python3",
                REPO_ROOT / folder / "bin" / "python",
                REPO_ROOT / folder / "Scripts" / "python.exe",
            ):
                if candidate.exists():
                    return str(candidate)
        return sys.executable

    def setup_ui(self) -> None:
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        venv_frame = ttk.LabelFrame(main_frame, text="Entorno de Ejecucion", padding="10")
        venv_frame.pack(fill=tk.X, pady=5)

        self.use_venv_var = tk.BooleanVar(value=(self.venv_python != sys.executable))
        ttk.Checkbutton(
            venv_frame,
            text="Usar Entorno Virtual (.venv/venv)",
            variable=self.use_venv_var,
        ).grid(row=0, column=0, sticky=tk.W)

        self.venv_status_var = tk.StringVar(value=f"Ruta: {self.venv_python}")
        ttk.Label(
            venv_frame,
            textvariable=self.venv_status_var,
            foreground="gray",
        ).grid(row=1, column=0, sticky=tk.W, padx=20)

        credential_status = self.build_credential_status()
        self.credential_status_var = tk.StringVar(value=credential_status)
        ttk.Label(
            venv_frame,
            textvariable=self.credential_status_var,
            foreground="gray",
        ).grid(row=2, column=0, sticky=tk.W, padx=20, pady=(4, 0))

        date_frame = ttk.LabelFrame(
            main_frame,
            text="Rango exacto [since, before)",
            padding="10",
        )
        date_frame.pack(fill=tk.X, pady=5)

        ttk.Label(date_frame, text="Desde inclusivo (YYYY-MM-DD):").grid(
            row=0,
            column=0,
            sticky=tk.W,
            padx=5,
            pady=5,
        )
        self.since_var = tk.StringVar(value=DEFAULT_GLOBAL_SINCE)
        ttk.Entry(date_frame, textvariable=self.since_var, width=15).grid(
            row=0,
            column=1,
            sticky=tk.W,
            padx=5,
        )

        ttk.Label(date_frame, text="Antes de, exclusivo (YYYY-MM-DD):").grid(
            row=0,
            column=2,
            sticky=tk.W,
            padx=5,
        )
        self.before_var = tk.StringVar(value=DEFAULT_GLOBAL_BEFORE)
        ttk.Entry(date_frame, textvariable=self.before_var, width=15).grid(
            row=0,
            column=3,
            sticky=tk.W,
            padx=5,
        )
        ttk.Label(
            date_frame,
            text="Se incluye Desde y se excluye Antes de.",
            foreground="gray",
        ).grid(row=1, column=0, columnspan=4, sticky=tk.W, padx=5)

        history_frame = ttk.LabelFrame(
            main_frame,
            text="Últimas descargas por fuente",
            padding="8",
        )
        history_frame.pack(fill=tk.X, pady=5)
        history_columns = ("fuente", "rango", "estado", "finalizo", "carpeta")
        self.download_history_tree = ttk.Treeview(
            history_frame,
            columns=history_columns,
            show="headings",
            height=5,
        )
        headings = {
            "fuente": ("Fuente", 145),
            "rango": ("Rango exacto", 225),
            "estado": ("Estado", 85),
            "finalizo": ("Finalizó", 135),
            "carpeta": ("Carpeta", 260),
        }
        for column, (label, width) in headings.items():
            self.download_history_tree.heading(column, text=label)
            self.download_history_tree.column(column, width=width, anchor=tk.W)
        self.download_history_tree.pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Button(
            history_frame,
            text="Actualizar",
            command=self.refresh_download_history,
        ).pack(side=tk.RIGHT, padx=(8, 0))
        self.refresh_download_history()

        options_frame = ttk.Frame(main_frame, padding="5")
        options_frame.pack(fill=tk.X)

        self.mode_var = tk.StringVar(value="all_networks")
        ttk.Radiobutton(
            options_frame,
            text="Modo Generico (Defaults)",
            variable=self.mode_var,
            value="all_networks",
        ).pack(side=tk.LEFT, padx=10)
        ttk.Radiobutton(
            options_frame,
            text="Modo Especifico (requiere terminal)",
            variable=self.mode_var,
            value="per_network",
        ).pack(side=tk.LEFT, padx=10)

        self.continue_error_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            options_frame,
            text="Continuar en error",
            variable=self.continue_error_var,
        ).pack(side=tk.LEFT, padx=10)

        sna_frame = ttk.LabelFrame(main_frame, text="Análisis SNA", padding="8")
        sna_frame.pack(fill=tk.X, pady=5)

        ttk.Label(
            sna_frame,
            text=(
                "Histórico incorpora las fuentes locales y RAdAR. Los alcances "
                "recientes resuelven fechas reales y usan solo fuentes locales."
            ),
            wraplength=800,
        ).grid(row=0, column=0, columnspan=2, sticky=tk.W, padx=5)

        self.sna_history_button = ttk.Button(
            sna_frame,
            text="EJECUTAR SNA MATERIAL HISTÓRICO",
            command=lambda: self.start_sna_execution("historico"),
        )
        self.sna_history_button.grid(
            row=1, column=0, sticky=tk.EW, padx=5, pady=(7, 3)
        )

        self.sna_recent_button = ttk.Button(
            sna_frame,
            text="EJECUTAR SNA 2 RANGOS RECIENTES",
            command=lambda: self.start_sna_execution("ultimos_2_rangos"),
        )
        self.sna_recent_button.grid(
            row=1, column=1, sticky=tk.EW, padx=5, pady=(7, 3)
        )

        self.sna_last_week_button = ttk.Button(
            sna_frame,
            text="EJECUTAR SNA ÚLTIMO RANGO",
            command=lambda: self.start_sna_execution("ultimo_rango"),
        )
        self.sna_last_week_button.grid(
            row=2, column=0, columnspan=2, sticky=tk.EW, padx=5, pady=3
        )
        sna_frame.columnconfigure(0, weight=1)
        sna_frame.columnconfigure(1, weight=1)

        ttk.Label(
            sna_frame,
            text=(
                "Cada rango conserva su CSV; cada ejecución crea una subcarpeta "
                "con timestamp y no sobrescribe resultados anteriores."
            ),
            foreground="gray",
            font=("Helvetica", 8),
        ).grid(row=3, column=0, columnspan=2, sticky=tk.W, padx=5)

        control_frame = ttk.Frame(main_frame, padding="10")
        control_frame.pack(fill=tk.X)

        self.play_button = ttk.Button(control_frame, text="EJECUTAR", command=self.start_execution)
        self.play_button.pack(side=tk.LEFT, padx=5, expand=True, fill=tk.X)

        self.stop_button = ttk.Button(control_frame, text="DETENER", command=self.stop_execution, state=tk.DISABLED)
        self.stop_button.pack(side=tk.LEFT, padx=5, expand=True, fill=tk.X)

        pipeline_frame = ttk.LabelFrame(main_frame, text="Seleccion de Pipelines", padding="10")
        pipeline_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        self.pipeline_vars: dict[str, tk.BooleanVar] = {}
        canvas = tk.Canvas(pipeline_frame)
        scrollbar = ttk.Scrollbar(pipeline_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        scrollable_frame.bind(
            "<Configure>",
            lambda event: canvas.configure(scrollregion=canvas.bbox("all")),
        )
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        for pipe in PIPELINES:
            var = tk.BooleanVar(value=False)
            self.pipeline_vars[pipe.code] = var
            ttk.Checkbutton(
                scrollable_frame,
                text=f"{pipe.code}) {pipe.label}",
                variable=var,
            ).pack(anchor=tk.W, pady=2)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        log_frame = ttk.LabelFrame(main_frame, text="Consola de Salida", padding="5")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        self.log_area = scrolledtext.ScrolledText(
            log_frame,
            height=15,
            state=tk.DISABLED,
            bg="black",
            fg="lightgreen",
            font=("Courier", 10),
        )
        self.log_area.pack(fill=tk.BOTH, expand=True)

    def build_credential_status(self) -> str:
        tracked = ["YOUTUBE_API_KEY", "APIFY_TOKEN", "CLAUDE_API_KEY"]
        present = [name for name in tracked if os.getenv(name, "").strip()]
        missing = [name for name in tracked if name not in present]
        if present and not missing:
            return f"Credenciales detectadas: {', '.join(present)}"
        if present:
            return (
                f"Credenciales detectadas: {', '.join(present)} | "
                f"Faltan: {', '.join(missing)}"
            )
        return "No se detectaron credenciales en .env.local o en el entorno"

    def refresh_download_history(self) -> None:
        for item in self.download_history_tree.get_children():
            self.download_history_tree.delete(item)
        records = latest_downloads_by_pipeline(
            read_download_history(limit=500)
        )
        if not records:
            self.download_history_tree.insert(
                "",
                tk.END,
                values=("—", "Sin descargas registradas", "—", "—", "—"),
            )
            return
        for record in records:
            finished = str(record.get("finished_at") or "")
            try:
                parsed = datetime.fromisoformat(finished.replace("Z", "+00:00"))
                finished = parsed.astimezone().strftime("%Y-%m-%d %H:%M")
            except ValueError:
                pass
            output_dir = str(record.get("output_dir") or "")
            folder = Path(output_dir).name if output_dir else "—"
            self.download_history_tree.insert(
                "",
                tk.END,
                values=(
                    record.get("pipeline_label") or record.get("pipeline_key") or "—",
                    f"{record.get('since')} → {record.get('before')} (excl.)",
                    record.get("status") or "—",
                    finished or "—",
                    folder,
                ),
            )

    def log(self, message: str) -> None:
        def _append() -> None:
            self.log_area.config(state=tk.NORMAL)
            self.log_area.insert(tk.END, message + "\n")
            self.log_area.see(tk.END)
            self.log_area.config(state=tk.DISABLED)

        self.root.after(0, _append)

    def clear_log(self) -> None:
        def _clear() -> None:
            self.log_area.config(state=tk.NORMAL)
            self.log_area.delete(1.0, tk.END)
            self.log_area.config(state=tk.DISABLED)

        self.root.after(0, _clear)

    def get_selected_pipelines(self):
        selected = [PIPELINES_BY_CODE[code] for code, var in self.pipeline_vars.items() if var.get()]
        pipeline_order = [pipe.code for pipe in PIPELINES]
        selected.sort(key=lambda item: pipeline_order.index(item.code))
        return selected

    def validate_dependencies(self, selected):
        selected_codes = {spec.code for spec in selected}
        if "5" in selected_codes and "4" not in selected_codes:
            self.log("Agregando Facebook Posts (4) como dependencia de Comentarios (5)")
            insert_at = next((index for index, item in enumerate(selected) if item.code == "5"), 0)
            selected.insert(insert_at, PIPELINES_BY_CODE["4"])

        selected = ensure_pipeline_before(selected, "4", "5")

        required_by_consolidador = {"7": "Claude", "8": "Influencia", "9": "Guiados"}
        for dep_code, dep_label in required_by_consolidador.items():
            selected_codes = {spec.code for spec in selected}
            if dep_code in selected_codes and "6" not in selected_codes:
                self.log(f"Agregando Consolidador (6) como dependencia de {dep_label} ({dep_code})")
                selected.insert(0, PIPELINES_BY_CODE["6"])
            selected = ensure_pipeline_before(selected, "6", dep_code)

        selected = ensure_pipeline_after(selected, "10", ["1", "2", "4"])
        selected = ensure_pipeline_after(
            selected, "6", ["1", "2", "3", "4", "5", "12", "13"]
        )
        selected = ensure_pipeline_after(
            selected, "11", ["1", "2", "3", "4", "5", "12", "13"]
        )

        unique_selected = []
        seen = set()
        for spec in selected:
            if spec.code not in seen:
                unique_selected.append(spec)
                seen.add(spec.code)
        return unique_selected

    def start_execution(self) -> None:
        if self.running_process is not None:
            messagebox.showwarning("En ejecución", "Ya hay un proceso en ejecución.")
            return

        selected = self.get_selected_pipelines()
        if not selected:
            messagebox.showwarning("Atencion", "Selecciona al menos un pipeline para ejecutar.")
            return

        if self.mode_var.get() == "per_network":
            messagebox.showwarning(
                "Modo no soportado en GUI",
                "La GUI no captura prompts interactivos de terminal para credenciales o parametros detallados. Usa Modo Generico o ejecuta 00_orquestador_general.py en terminal.",
            )
            return

        try:
            since, before = parse_date_range(self.since_var.get().strip(), self.before_var.get().strip())
        except ValueError as exc:
            messagebox.showerror("Error de Fechas", str(exc))
            return

        self.clear_log()
        self.log(f"Iniciando ejecucion: {since} al {before}")
        selected = self.validate_dependencies(selected)
        self.log(f"Pipelines a ejecutar: {', '.join(spec.label for spec in selected)}")

        self.play_button.config(state=tk.DISABLED)
        self.set_sna_buttons_state(tk.DISABLED)
        self.stop_button.config(state=tk.NORMAL)
        self.stop_requested = False

        thread = threading.Thread(target=self.run_pipelines, args=(selected, since, before), daemon=True)
        thread.start()

    def build_python_exec(self) -> str:
        if self.use_venv_var.get() and self.venv_python:
            return self.venv_python
        return sys.executable

    def set_sna_buttons_state(self, state: str) -> None:
        self.sna_history_button.config(state=state)
        self.sna_recent_button.config(state=state)
        self.sna_last_week_button.config(state=state)

    def start_sna_execution(self, scope: str) -> None:
        if self.running_process is not None:
            messagebox.showwarning("En ejecución", "Ya hay un proceso en ejecución.")
            return

        try:
            run = build_sna_run(scope)
        except (OSError, RuntimeError, ValueError) as exc:
            messagebox.showerror("No se pudo resolver el alcance SNA", str(exc))
            return
        steps = run["steps"]
        self.clear_log()
        self.log(f"Iniciando SNA: {run['label']}")
        self.log("Etapas: " + ", ".join(label for label, _, _ in steps))

        self.play_button.config(state=tk.DISABLED)
        self.set_sna_buttons_state(tk.DISABLED)
        self.stop_button.config(state=tk.NORMAL)
        self.stop_requested = False

        thread = threading.Thread(
            target=self.run_sna_pipelines,
            args=(run,),
            daemon=True,
        )
        thread.start()

    def stop_execution(self) -> None:
        if self.running_process:
            self.stop_requested = True
            self.running_process.terminate()
            self.log("Solicitud de detencion enviada...")

    def run_pipelines(self, selected, since: str, before: str) -> None:
        use_defaults = self.mode_var.get() == "all_networks"
        facebook_posts_csv = ""

        for spec in selected:
            if self.stop_requested:
                break

            self.log(f"\n--- Ejecutando: {spec.label} ---")
            cmd: list[str] | None = None
            started_at: str | None = None
            download_recorded = False
            try:
                cmd, env_vars = ORQ.build_pipeline(
                    spec,
                    since,
                    before,
                    use_defaults=use_defaults,
                    facebook_posts_csv=facebook_posts_csv,
                )

                if self.use_venv_var.get() and self.venv_python and cmd and cmd[0] == sys.executable:
                    cmd[0] = self.venv_python

                self.log(f"Comando: {ORQ.render_command(cmd)}")

                env = os.environ.copy()
                env.update(env_vars)

                started_at = ORQ.utc_now()
                self.running_process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    env=env,
                    cwd=str(REPO_ROOT),
                    bufsize=1,
                    universal_newlines=True,
                )

                assert self.running_process.stdout is not None
                for line in self.running_process.stdout:
                    self.log(line.rstrip())

                self.running_process.wait()
                return_code = self.running_process.returncode
                download_status = (
                    "completada"
                    if return_code == 0
                    else "detenida"
                    if self.stop_requested
                    else "fallida"
                )
                ORQ.record_download_result(
                    spec,
                    since,
                    before,
                    cmd,
                    started_at=started_at,
                    status=download_status,
                    return_code=return_code,
                )
                download_recorded = True
                self.root.after(0, self.refresh_download_history)

                if return_code == 0:
                    self.log(f"{spec.label} finalizado con exito.")
                    if spec.code == "4":
                        output_dir_arg = ORQ._extract_flag_value(cmd, "--output-dir") or str(REPO_ROOT / "Facebook")
                        report_tag = ORQ.build_range_report_tag(
                            since,
                            before,
                            "Facebook",
                        )
                        facebook_posts_csv = str(Path(output_dir_arg) / report_tag / f"{report_tag}_posts.csv")
                        if os.path.exists(facebook_posts_csv):
                            self.log(f"Detectado CSV de posts: {facebook_posts_csv}")
                        else:
                            self.log(f"CSV esperado no encontrado: {facebook_posts_csv}")
                            facebook_posts_csv = ""
                    continue

                if self.stop_requested:
                    self.log("Proceso detenido por el usuario.")
                    break

                self.log(f"Error en {spec.label} (Codigo {return_code})")
                if not self.continue_error_var.get():
                    self.log("Abortando ejecucion.")
                    break

            except Exception as exc:
                if cmd is not None and started_at is not None and not download_recorded:
                    ORQ.record_download_result(
                        spec,
                        since,
                        before,
                        cmd,
                        started_at=started_at,
                        status="detenida" if self.stop_requested else "fallida",
                        return_code=None,
                    )
                    self.root.after(0, self.refresh_download_history)
                self.log(f"Error inesperado ejecutando {spec.label}: {exc}")
                if not self.continue_error_var.get():
                    break

        self.log("\nProceso terminado.")
        self.root.after(0, self.finish_ui)

    def run_sna_pipelines(self, run: dict[str, object]) -> None:
        python_exec = self.build_python_exec()
        steps = run["steps"]
        results_dir = Path(run["results_dir"])
        run_log = Path(run["run_log"])
        final_outputs = [Path(path) for path in run["final_outputs"]]
        had_error = False
        success = False
        results_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = write_sna_run_manifest(run)

        with run_log.open("w", encoding="utf-8", buffering=1) as log_handle:
            def sna_log(message: str) -> None:
                self.log(message)
                log_handle.write(message + "\n")

            try:
                sna_log(f"Inicio: {datetime.now().isoformat(timespec='seconds')}")
                sna_log(f"Intérprete: {python_exec}")
                sna_log(f"Alcance: {run['label']}")
                selected_ranges = run.get("selected_ranges") or []
                for selected in selected_ranges:
                    sna_log(
                        "Lote fuente: "
                        f"{selected['identity']} "
                        f"[{selected['since']}, {selected['before']})"
                    )
                if manifest_path is not None:
                    sna_log(f"Manifiesto: {manifest_path}")

                for label, script_name, args in steps:
                    if self.stop_requested:
                        had_error = True
                        break

                    cmd = [python_exec, str(SCRIPTS_DIR / script_name), *args]
                    sna_log(f"\n--- Ejecutando SNA: {label} ---")
                    sna_log(f"Comando: {ORQ.render_command(cmd)}")

                    try:
                        self.running_process = subprocess.Popen(
                            cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            text=True,
                            cwd=str(REPO_ROOT),
                            env={**os.environ, "PYTHONUNBUFFERED": "1"},
                            bufsize=1,
                            universal_newlines=True,
                        )
                        assert self.running_process.stdout is not None
                        for line in self.running_process.stdout:
                            sna_log(line.rstrip())

                        self.running_process.wait()
                        return_code = self.running_process.returncode
                        if return_code == 0:
                            sna_log(f"{label} finalizado con éxito.")
                            continue

                        had_error = True
                        if self.stop_requested:
                            sna_log("Proceso SNA detenido por el usuario.")
                            break
                        sna_log(f"Error en {label} (código {return_code}).")
                        if not self.continue_error_var.get():
                            sna_log("Abortando ejecución SNA.")
                            break
                    except Exception as exc:
                        had_error = True
                        sna_log(f"Error inesperado en {label}: {exc}")
                        if not self.continue_error_var.get():
                            break

                if not had_error and not self.stop_requested:
                    missing_outputs = [path for path in final_outputs if not path.exists()]
                    if missing_outputs:
                        had_error = True
                        sna_log("Faltan resultados finales:")
                        for path in missing_outputs:
                            sna_log(f"  - {path}")
                    else:
                        success = True
                        sna_log("Resultados SNA generados:")
                        for path in final_outputs:
                            sna_log(f"  - {path}")

                if success:
                    sna_log("\nSNA finalizado correctamente.")
                elif not self.stop_requested:
                    sna_log("\nSNA incompleto: no se generaron los tres HTML finales.")
                sna_log(f"Bitácora: {run_log}")
            finally:
                self.root.after(0, self.finish_sna_ui, success, run)

    def finish_ui(self) -> None:
        self.play_button.config(state=tk.NORMAL)
        self.set_sna_buttons_state(tk.NORMAL)
        self.stop_button.config(state=tk.DISABLED)
        self.running_process = None
        if not self.stop_requested:
            messagebox.showinfo("Finalizado", "La ejecucion de los pipelines ha concluido.")

    def finish_sna_ui(self, success: bool, run: dict[str, object]) -> None:
        self.play_button.config(state=tk.NORMAL)
        self.set_sna_buttons_state(tk.NORMAL)
        self.stop_button.config(state=tk.DISABLED)
        self.running_process = None
        if self.stop_requested:
            return
        if success:
            messagebox.showinfo(
                "SNA finalizado",
                f"Se generó el análisis de {run['label']} en:\n{run['results_dir']}",
            )
        else:
            messagebox.showerror(
                "SNA incompleto",
                "La cadena se detuvo antes de generar los resultados finales. "
                f"Revisa la bitácora:\n{run['run_log']}",
            )


if __name__ == "__main__":
    root = tk.Tk()
    app = OrquestadorGUI(root)
    root.mainloop()
