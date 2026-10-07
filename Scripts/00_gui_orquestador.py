#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import hashlib
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

from download_history import (
    latest_downloads_by_pipeline,
    latest_pipeline_records,
    pipeline_completed_for_range,
    read_download_history,
    read_pipeline_history,
)
from output_naming import (
    build_range_label,
    build_range_report_tag,
    validate_range_contract_file,
    write_range_contract,
)
from sna_recent_ranges import MaterialRange, discover_material_ranges, resolve_recent_scope


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
MATERIAL_DEPENDENT_PIPELINE_CODES = frozenset({"7", "8", "9"})
_TODAY = datetime.now().date()
_CURRENT_RANGE_START = _TODAY - timedelta(days=_TODAY.weekday())
DEFAULT_GLOBAL_SINCE = _CURRENT_RANGE_START.isoformat()
DEFAULT_GLOBAL_BEFORE = (_CURRENT_RANGE_START + timedelta(days=7)).isoformat()

MATERIAL_FILENAMES = (
    "material_institucional.txt",
    "material_comentarios.txt",
)


def require_material_folder_for_range(
    since: str,
    before: str,
    *,
    repo_root: Path = REPO_ROOT,
) -> Path:
    """Exige material consolidado con contrato idéntico al rango escrito."""
    folder = repo_root / "Datos" / build_range_report_tag(since, before, "Datos")
    if not folder.is_dir():
        raise RuntimeError(
            "No existe la carpeta de material para el rango escrito: "
            f"{folder}"
        )

    contract_ok, contract_detail = validate_range_contract_file(
        folder,
        since,
        before,
        "Datos",
    )
    if not contract_ok:
        raise RuntimeError(
            "La carpeta de material no declara exactamente el rango escrito: "
            f"{contract_detail}"
        )

    missing = [name for name in MATERIAL_FILENAMES if not (folder / name).is_file()]
    if missing:
        raise RuntimeError(
            f"La carpeta {folder.name} no contiene: {', '.join(missing)}"
        )

    if not any((folder / name).stat().st_size > 0 for name in MATERIAL_FILENAMES):
        raise RuntimeError(f"Los archivos de material están vacíos en {folder}")
    return folder


def probe_material_folder_for_selected(
    selected_codes: set[str],
    since: str,
    before: str,
    *,
    repo_root: Path = REPO_ROOT,
) -> tuple[Path | None, str]:
    """Detecta material reutilizable sin bloquear una ejecución que puede crearlo."""
    if not selected_codes & MATERIAL_DEPENDENT_PIPELINE_CODES:
        return None, ""
    try:
        return (
            require_material_folder_for_range(
                since,
                before,
                repo_root=repo_root,
            ),
            "",
        )
    except (OSError, RuntimeError, ValueError) as exc:
        return None, str(exc)


def build_sna_run(
    scope: str,
    *,
    repo_root: Path = REPO_ROOT,
    now: datetime | None = None,
    since: str | None = None,
    before: str | None = None,
    selected_material_ranges: list[MaterialRange] | tuple[MaterialRange, ...] | None = None,
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
    elif scope == "rangos_seleccionados":
        unique_ranges = {
            item.material_folder.name: item
            for item in (selected_material_ranges or [])
        }
        chosen = sorted(
            unique_ranges.values(),
            key=lambda item: (item.since, item.before, item.material_folder.name),
        )
        if not chosen:
            raise ValueError("Selecciona al menos un rango para el análisis SNA")

        exact_since = min(item.since for item in chosen).isoformat()
        exact_before = max(item.before for item in chosen).isoformat()
        selection_key = "|".join(
            f"{item.material_folder.name}:{item.since}:{item.before}"
            for item in chosen
        )
        selection_id = hashlib.sha1(selection_key.encode("utf-8")).hexdigest()[:10]
        filename_scope = f"seleccion_{len(chosen)}_rangos_{selection_id}"
        execution_time = now or datetime.now()
        execution_id = execution_time.strftime("ejecucion_%Y%m%dT%H%M%S_%f")
        input_dir = sna_data_dir / "selecciones" / filename_scope / execution_id
        input_csv = input_dir / f"tampico_datos_tabulares_{filename_scope}.csv"
        results_dir = sna_results_root / "selecciones" / filename_scope / execution_id
        consolidate_args = ["--output", str(input_csv)]
        for item in chosen:
            consolidate_args.extend(
                ["--include-range", item.since.isoformat(), item.before.isoformat()]
            )
        selected_ranges = [
            {
                "since": item.since.isoformat(),
                "before": item.before.isoformat(),
                "identity": item.identity,
                "sources": list(item.sources),
                "inferred_from_rows": item.inferred_from_rows,
                "material_folder": str(item.material_folder),
            }
            for item in chosen
        ]
        label = f"selección de {len(chosen)} rango(s) · unión exacta"
        scope_short = f"de {len(chosen)} rangos seleccionados"
        network_scope = f"Tampico · selección de {len(chosen)} rangos"
        accounts_scope = scope_short
        corpus_label = f"unión exacta de {len(chosen)} rangos locales de Tampico"
        log_name = f"ejecucion_sna_{filename_scope}.log"
    elif scope in {"ultimos_2_rangos", "ultimo_rango", "rango_escrito"}:
        material_folder: Path | None = None
        if scope == "rango_escrito":
            if since is None or before is None:
                raise ValueError("El rango escrito requiere since y before")
            exact_since, exact_before = parse_date_range(since, before)
            material_folder = require_material_folder_for_range(
                exact_since,
                exact_before,
                repo_root=repo_root,
            )
            selected_ranges = [
                {
                    "since": exact_since,
                    "before": exact_before,
                    "identity": build_range_label(exact_since, exact_before),
                    "sources": ["Datos"],
                    "inferred_from_rows": False,
                    "material_folder": str(material_folder),
                }
            ]
            selection_label = f"rango escrito · material {material_folder.name}"
        else:
            count = 2 if scope == "ultimos_2_rangos" else 1
            recent = resolve_recent_scope(repo_root, count)
            exact_since = recent.since.isoformat()
            exact_before = recent.before.isoformat()
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
            selection_label = (
                "2 rangos más recientes" if count == 2 else "rango más reciente"
            )
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
        "selection_mode": (
            "union_exacta" if scope == "rangos_seleccionados" else "cobertura_continua"
        ),
    }


def write_sna_run_manifest(run: dict[str, object]) -> Path | None:
    """Registra el alcance reciente sin modificar los CSV fuente."""
    since = run.get("since")
    before = run.get("before")
    if not isinstance(since, str) or not isinstance(before, str):
        return None
    results_dir = Path(run["results_dir"])
    if run.get("selection_mode") != "union_exacta":
        write_range_contract(results_dir, since, before, "SNA")
    path = results_dir / "manifiesto_ejecucion_sna.json"
    payload = {
        "manifest_version": 1,
        "scope": run.get("scope"),
        "label": run.get("label"),
        "since": since,
        "before": before,
        "interval": "[since,before)",
        "selection_mode": run.get("selection_mode"),
        "selected_ranges": run.get("selected_ranges", []),
        "input_csv": str(run["input_csv"]),
        "results_dir": str(results_dir),
        "status": "iniciada",
        "started_at": ORQ.utc_now(),
        "finished_at": "",
        "steps": [
            {
                "index": index,
                "label": label,
                "script": script_name,
                "status": "pendiente",
                "return_code": None,
                "message": "",
            }
            for index, (label, script_name, _) in enumerate(run["steps"], 1)
        ],
    }
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return path


def update_sna_run_manifest(
    path: Path | None,
    *,
    step_index: int | None = None,
    step_status: str | None = None,
    return_code: int | None = None,
    message: str = "",
    run_status: str | None = None,
) -> None:
    if path is None or not path.exists():
        return
    payload = json.loads(path.read_text(encoding="utf-8"))
    if step_index is not None and step_status is not None:
        steps = payload.get("steps", [])
        if 1 <= step_index <= len(steps):
            step = steps[step_index - 1]
            step["status"] = step_status
            step["return_code"] = return_code
            step["message"] = message
            step["updated_at"] = ORQ.utc_now()
    if run_status is not None:
        payload["status"] = run_status
        if run_status in {"completada", "fallida", "detenida"}:
            payload["finished_at"] = ORQ.utc_now()
            if run_status != "completada":
                for step in payload.get("steps", []):
                    if step.get("status") == "pendiente":
                        step["status"] = "omitida"
                        step["message"] = message or "ejecucion_sna_incompleta"
                        step["updated_at"] = ORQ.utc_now()
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temp_path.replace(path)


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
        self.root.geometry("1040x900")
        self.root.minsize(760, 600)
        self.root.resizable(True, True)

        self.running_process: subprocess.Popen[str] | None = None
        self.stop_requested = False
        self.pipeline_had_error = False
        self.venv_python = self.detect_venv()
        self.active_run_id = ""
        self.active_run_log: Path | None = None
        self._log_lock = threading.Lock()
        self.sna_material_ranges: list[MaterialRange] = []

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

        self.main_vertical_pane = tk.PanedWindow(
            main_frame,
            orient=tk.VERTICAL,
            sashrelief=tk.RAISED,
            sashwidth=8,
            showhandle=True,
            borderwidth=0,
        )
        self.main_vertical_pane.pack(fill=tk.BOTH, expand=True, pady=5)

        operations_frame = ttk.Frame(self.main_vertical_pane)
        self.operations_vertical_pane = tk.PanedWindow(
            operations_frame,
            orient=tk.VERTICAL,
            sashrelief=tk.RAISED,
            sashwidth=8,
            showhandle=True,
            borderwidth=0,
        )

        history_frame = ttk.LabelFrame(
            self.operations_vertical_pane,
            text="Últimos estados por stage",
            padding="8",
        )
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
        history_actions = ttk.Frame(history_frame)
        history_actions.pack(side=tk.RIGHT, fill=tk.Y, padx=(8, 0))
        ttk.Button(
            history_actions,
            text="Actualizar",
            command=self.refresh_download_history,
        ).pack(side=tk.TOP)
        history_scrollbar = ttk.Scrollbar(
            history_frame,
            orient=tk.VERTICAL,
            command=self.download_history_tree.yview,
        )
        self.download_history_tree.configure(yscrollcommand=history_scrollbar.set)
        history_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.download_history_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.operations_vertical_pane.add(
            history_frame,
            minsize=90,
            stretch="always",
        )
        self.refresh_download_history()

        options_frame = ttk.Frame(operations_frame, padding="5")
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

        self.continue_error_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            options_frame,
            text="Continuar stages independientes",
            variable=self.continue_error_var,
        ).pack(side=tk.LEFT, padx=10)

        self.resume_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            options_frame,
            text="Reanudar completados",
            variable=self.resume_var,
        ).pack(side=tk.LEFT, padx=10)

        self.include_chucho_nader_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            options_frame,
            text="Descargar información de Chucho Nader",
            variable=self.include_chucho_nader_var,
        ).pack(side=tk.LEFT, padx=10)

        sna_frame = ttk.LabelFrame(
            self.operations_vertical_pane,
            text="Análisis SNA",
            padding="8",
        )

        ttk.Label(
            sna_frame,
            text=(
                "Histórico incorpora las fuentes locales y RAdAR. También puedes "
                "marcar cualquier combinación de los rangos que tienen material."
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

        ttk.Label(
            sna_frame,
            text="Rangos disponibles (cada clic marca o desmarca):",
        ).grid(row=2, column=0, columnspan=2, sticky=tk.W, padx=5, pady=(7, 2))

        selector_frame = ttk.Frame(sna_frame)
        selector_frame.grid(row=3, column=0, columnspan=2, sticky=tk.NSEW, padx=5)
        self.sna_range_listbox = tk.Listbox(
            selector_frame,
            selectmode=tk.MULTIPLE,
            exportselection=False,
            height=7,
            activestyle="dotbox",
        )
        sna_range_scrollbar = ttk.Scrollbar(
            selector_frame,
            orient=tk.VERTICAL,
            command=self.sna_range_listbox.yview,
        )
        self.sna_range_listbox.configure(yscrollcommand=sna_range_scrollbar.set)
        self.sna_range_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sna_range_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.sna_range_listbox.bind(
            "<<ListboxSelect>>",
            lambda _event: self.update_sna_selection_status(),
        )

        selector_controls = ttk.Frame(sna_frame)
        selector_controls.grid(row=4, column=0, columnspan=2, sticky=tk.EW, padx=5, pady=3)
        ttk.Button(
            selector_controls,
            text="Actualizar rangos",
            command=self.refresh_sna_range_selector,
        ).pack(side=tk.LEFT)
        self.sna_selection_status_var = tk.StringVar(value="0 rangos seleccionados")
        ttk.Label(
            selector_controls,
            textvariable=self.sna_selection_status_var,
            foreground="gray",
        ).pack(side=tk.RIGHT)

        self.sna_selected_ranges_button = ttk.Button(
            sna_frame,
            text="EJECUTAR SNA CON LOS RANGOS SELECCIONADOS",
            command=lambda: self.start_sna_execution("rangos_seleccionados"),
        )
        self.sna_selected_ranges_button.grid(
            row=5, column=0, columnspan=2, sticky=tk.EW, padx=5, pady=3
        )
        sna_frame.columnconfigure(0, weight=1)
        sna_frame.columnconfigure(1, weight=1)
        sna_frame.rowconfigure(3, weight=1)
        selector_frame.columnconfigure(0, weight=1)

        self.refresh_sna_range_selector()

        ttk.Label(
            sna_frame,
            text=(
                "Cada rango conserva su CSV; cada ejecución crea una subcarpeta "
                "con timestamp y no sobrescribe resultados anteriores."
            ),
            foreground="gray",
            font=("Helvetica", 8),
        ).grid(row=6, column=0, columnspan=2, sticky=tk.W, padx=5)

        self.operations_vertical_pane.add(
            sna_frame,
            minsize=220,
            stretch="always",
        )

        control_frame = ttk.Frame(operations_frame, padding="10")
        control_frame.pack(side=tk.BOTTOM, fill=tk.X)

        self.play_button = ttk.Button(control_frame, text="EJECUTAR", command=self.start_execution)
        self.play_button.pack(side=tk.LEFT, padx=5, expand=True, fill=tk.X)

        self.stop_button = ttk.Button(control_frame, text="DETENER", command=self.stop_execution, state=tk.DISABLED)
        self.stop_button.pack(side=tk.LEFT, padx=5, expand=True, fill=tk.X)

        self.operations_vertical_pane.pack(fill=tk.BOTH, expand=True)
        self.main_vertical_pane.add(
            operations_frame,
            minsize=320,
            stretch="always",
        )

        workspace_pane = tk.PanedWindow(
            self.main_vertical_pane,
            orient=tk.HORIZONTAL,
            sashrelief=tk.RAISED,
            sashwidth=8,
            showhandle=True,
            borderwidth=0,
        )

        pipeline_frame = ttk.LabelFrame(workspace_pane, text="Seleccion de Pipelines", padding="10")

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
        workspace_pane.add(pipeline_frame, minsize=190, stretch="always")

        log_frame = ttk.LabelFrame(workspace_pane, text="Consola de Salida", padding="5")

        self.log_area = scrolledtext.ScrolledText(
            log_frame,
            height=15,
            state=tk.DISABLED,
            bg="black",
            fg="lightgreen",
            font=("Courier", 10),
        )
        self.log_area.pack(fill=tk.BOTH, expand=True)
        workspace_pane.add(log_frame, minsize=280, stretch="always")
        self.main_vertical_pane.add(
            workspace_pane,
            minsize=180,
            stretch="always",
        )

    def refresh_sna_range_selector(self) -> None:
        selected_names = {
            self.sna_material_ranges[index].material_folder.name
            for index in self.sna_range_listbox.curselection()
            if index < len(self.sna_material_ranges)
        }
        self.sna_material_ranges = list(reversed(discover_material_ranges(REPO_ROOT)))
        self.sna_range_listbox.delete(0, tk.END)
        for index, item in enumerate(self.sna_material_ranges):
            self.sna_range_listbox.insert(tk.END, item.display_label)
            if item.material_folder.name in selected_names:
                self.sna_range_listbox.selection_set(index)
        self.update_sna_selection_status()

    def selected_sna_material_ranges(self) -> list[MaterialRange]:
        return [
            self.sna_material_ranges[index]
            for index in self.sna_range_listbox.curselection()
            if index < len(self.sna_material_ranges)
        ]

    def update_sna_selection_status(self) -> None:
        count = len(self.sna_range_listbox.curselection())
        suffix = "rango seleccionado" if count == 1 else "rangos seleccionados"
        self.sna_selection_status_var.set(f"{count} {suffix}")

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
        pipeline_records = read_pipeline_history(limit=1000)
        records = (
            latest_pipeline_records(pipeline_records)
            if pipeline_records
            else latest_downloads_by_pipeline(read_download_history(limit=500))
        )
        if not records:
            self.download_history_tree.insert(
                "",
                tk.END,
                values=("—", "Sin stages registrados", "—", "—", "—"),
            )
            return
        for record in records:
            finished = str(record.get("finished_at") or record.get("event_at") or "")
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
        if self.active_run_log is not None:
            with self._log_lock:
                ORQ.append_run_log(self.active_run_log, message)

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

    def validate_dependencies(self, selected, *, material_available: bool = False):
        selected_codes = {spec.code for spec in selected}
        if "5" in selected_codes and "4" not in selected_codes:
            self.log("Agregando Facebook Posts (4) como dependencia de Comentarios (5)")
            insert_at = next((index for index, item in enumerate(selected) if item.code == "5"), 0)
            selected.insert(insert_at, PIPELINES_BY_CODE["4"])

        selected = ensure_pipeline_before(selected, "4", "5")

        required_by_consolidador = {"7": "Claude", "8": "Influencia", "9": "Guiados"}
        for dep_code, dep_label in required_by_consolidador.items():
            selected_codes = {spec.code for spec in selected}
            if (
                dep_code in selected_codes
                and "6" not in selected_codes
                and not material_available
            ):
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

        selected_codes = {spec.code for spec in selected}
        material_folder, material_problem = probe_material_folder_for_selected(
            selected_codes,
            since,
            before,
        )
        material_available = material_folder is not None

        self.clear_log()
        self.active_run_id = ORQ.create_run_id(since, before)
        self.active_run_log = ORQ.pipeline_run_log_path(self.active_run_id)
        self.log(f"Iniciando ejecucion: {since} al {before}")
        self.log(f"ID de ejecucion: {self.active_run_id}")
        self.log(f"Bitacora persistente: {self.active_run_log}")
        if material_available:
            self.log(f"Material del análisis: {material_folder}")
        elif material_problem:
            self.log(
                "Material consolidado todavía no disponible; el Consolidador (6) "
                "se ejecutará antes de los análisis que lo requieren."
            )
            self.log(f"Detalle: {material_problem}")
        selected = self.validate_dependencies(
            selected,
            material_available=material_available,
        )
        include_chucho_nader = self.include_chucho_nader_var.get()
        self.log(
            "Información de Chucho Nader: "
            + ("incluida" if include_chucho_nader else "excluida")
        )
        self.log(f"Pipelines a ejecutar: {', '.join(spec.label for spec in selected)}")

        self.play_button.config(state=tk.DISABLED)
        self.set_sna_buttons_state(tk.DISABLED)
        self.stop_button.config(state=tk.NORMAL)
        self.stop_requested = False
        self.pipeline_had_error = False

        thread = threading.Thread(
            target=self.run_pipelines,
            args=(selected, since, before, include_chucho_nader),
            daemon=True,
        )
        thread.start()

    def build_python_exec(self) -> str:
        if self.use_venv_var.get() and self.venv_python:
            return self.venv_python
        return sys.executable

    def set_sna_buttons_state(self, state: str) -> None:
        self.sna_history_button.config(state=state)
        self.sna_recent_button.config(state=state)
        self.sna_selected_ranges_button.config(state=state)
        self.sna_range_listbox.config(state=state)

    def start_sna_execution(self, scope: str) -> None:
        if self.running_process is not None:
            messagebox.showwarning("En ejecución", "Ya hay un proceso en ejecución.")
            return

        try:
            if scope == "rangos_seleccionados":
                selected_ranges = self.selected_sna_material_ranges()
                run = build_sna_run(
                    scope,
                    selected_material_ranges=selected_ranges,
                )
            elif scope == "rango_escrito":
                since, before = parse_date_range(
                    self.since_var.get().strip(),
                    self.before_var.get().strip(),
                )
                run = build_sna_run(scope, since=since, before=before)
            else:
                run = build_sna_run(scope)
        except (OSError, RuntimeError, ValueError) as exc:
            messagebox.showerror("No se pudo resolver el alcance SNA", str(exc))
            return
        steps = run["steps"]
        self.active_run_id = ""
        self.active_run_log = None
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

    def run_pipelines(
        self,
        selected,
        since: str,
        before: str,
        include_chucho_nader: bool = True,
    ) -> None:
        use_defaults = self.mode_var.get() == "all_networks"
        facebook_posts_csv = ""
        selected_codes = {spec.code for spec in selected}
        unsuccessful_codes: set[str] = set()
        run_id = self.active_run_id or ORQ.create_run_id(since, before)
        run_log = self.active_run_log or ORQ.pipeline_run_log_path(run_id)

        def record(
            spec,
            cmd: list[str],
            *,
            started_at: str,
            status: str,
            return_code: int | None,
            reason: str = "",
        ) -> None:
            ORQ.record_pipeline_result(
                spec,
                since,
                before,
                cmd,
                run_id=run_id,
                started_at=started_at,
                status=status,
                return_code=return_code,
                log_path=run_log,
                reason=reason,
            )
            self.root.after(0, self.refresh_download_history)

        def mark_remaining_omitted(start_index: int, reason: str) -> None:
            for pending in selected[start_index:]:
                try:
                    pending_cmd, _ = ORQ.build_pipeline(
                        pending,
                        since,
                        before,
                        use_defaults=use_defaults,
                        facebook_posts_csv=facebook_posts_csv,
                        include_chucho_nader=include_chucho_nader,
                    )
                    started = ORQ.utc_now()
                    record(
                        pending,
                        pending_cmd,
                        started_at=started,
                        status="omitida",
                        return_code=None,
                        reason=reason,
                    )
                    self.log(f"Omitido {pending.label}: {reason}")
                except Exception as exc:
                    self.log(f"No se pudo registrar omision de {pending.label}: {exc}")

        for index, spec in enumerate(selected):
            if self.stop_requested:
                mark_remaining_omitted(index, "ejecucion_detenida_por_usuario")
                break

            cmd: list[str] | None = None
            started_at: str | None = None
            terminal_recorded = False
            try:
                cmd, env_vars = ORQ.build_pipeline(
                    spec,
                    since,
                    before,
                    use_defaults=use_defaults,
                    facebook_posts_csv=facebook_posts_csv,
                    include_chucho_nader=include_chucho_nader,
                )
                if self.use_venv_var.get() and self.venv_python and cmd and cmd[0] == sys.executable:
                    cmd[0] = self.venv_python

                blocked = ORQ.failed_dependencies_for_stage(
                    spec.code,
                    selected_codes,
                    unsuccessful_codes,
                )
                if blocked:
                    started_at = ORQ.utc_now()
                    reason = "dependencias_no_completadas:" + ",".join(sorted(blocked))
                    record(
                        spec,
                        cmd,
                        started_at=started_at,
                        status="omitida",
                        return_code=None,
                        reason=reason,
                    )
                    terminal_recorded = True
                    unsuccessful_codes.add(spec.code)
                    self.log(f"\n--- Omitido: {spec.label} ({reason}) ---")
                    continue

                force_source_refresh = (
                    not include_chucho_nader and spec.code in ORQ.SOURCE_PIPELINE_CODES
                )
                already_completed = (
                    self.resume_var.get()
                    and not force_source_refresh
                    and pipeline_completed_for_range(
                    spec.key,
                    since,
                    before,
                    )
                )
                if already_completed:
                    contract_ok, contract_detail = ORQ.validate_stage_output_contract(
                        spec,
                        since,
                        before,
                        cmd,
                    )
                    if not contract_ok:
                        self.log(
                            f"Reanudacion no omite {spec.label}: {contract_detail}; se ejecutara de nuevo."
                        )
                        already_completed = False

                if already_completed:
                    started_at = ORQ.utc_now()
                    record(
                        spec,
                        cmd,
                        started_at=started_at,
                        status="omitida",
                        return_code=0,
                        reason="ya_completada",
                    )
                    terminal_recorded = True
                    self.log(f"\n--- Reanudacion: {spec.label} ya estaba completado; se omite ---")
                    if spec.code == "4":
                        output_dir_arg = ORQ._extract_flag_value(cmd, "--output-dir") or str(REPO_ROOT / "Facebook")
                        report_tag = ORQ.build_range_report_tag(since, before, "Facebook")
                        candidate = Path(output_dir_arg) / report_tag / f"{report_tag}_posts.csv"
                        facebook_posts_csv = str(candidate) if candidate.exists() else ""
                    continue

                self.log(f"\n--- Ejecutando: {spec.label} ---")
                self.log(f"Comando: {ORQ.render_command(cmd)}")
                env = os.environ.copy()
                env.update(env_vars)
                started_at = ORQ.utc_now()
                record(
                    spec,
                    cmd,
                    started_at=started_at,
                    status="iniciada",
                    return_code=None,
                )

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

                if return_code == 0 and spec.code == "6":
                    datos_dir = ORQ._range_datos_dir_from_consolidador_cmd(
                        since,
                        before,
                        cmd,
                    )
                    limpieza_cmd = [
                        cmd[0],
                        str(SCRIPTS_DIR / "limpieza_texto.py"),
                        "--datos-dir",
                        str(datos_dir),
                    ]
                    self.log(f"🧼 Ejecutando limpieza de texto: {datos_dir}")
                    self.running_process = subprocess.Popen(
                        limpieza_cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        text=True,
                        env=env,
                        cwd=str(REPO_ROOT),
                        bufsize=1,
                    )
                    assert self.running_process.stdout is not None
                    for line in self.running_process.stdout:
                        self.log(line.rstrip())
                    return_code = self.running_process.wait()

                if return_code == 0:
                    contract_ok, contract_detail = ORQ.validate_stage_output_contract(
                        spec,
                        since,
                        before,
                        cmd,
                    )
                    if contract_ok:
                        self.log(f"✅ Contrato de rango verificado: {contract_detail}")
                    else:
                        self.log(f"❌ Stage sin contrato válido: {contract_detail}")
                        return_code = 3

                status = (
                    "completada"
                    if return_code == 0
                    else "detenida"
                    if self.stop_requested
                    else "fallida"
                )
                record(
                    spec,
                    cmd,
                    started_at=started_at,
                    status=status,
                    return_code=return_code,
                )
                terminal_recorded = True

                if return_code == 0:
                    self.log(f"{spec.label} finalizado con exito.")
                    if spec.code == "4":
                        output_dir_arg = ORQ._extract_flag_value(cmd, "--output-dir") or str(REPO_ROOT / "Facebook")
                        report_tag = ORQ.build_range_report_tag(since, before, "Facebook")
                        candidate = Path(output_dir_arg) / report_tag / f"{report_tag}_posts.csv"
                        facebook_posts_csv = str(candidate) if candidate.exists() else ""
                        if facebook_posts_csv:
                            self.log(f"Detectado CSV de posts: {facebook_posts_csv}")
                        else:
                            self.log(f"CSV esperado no encontrado: {candidate}")
                    continue

                unsuccessful_codes.add(spec.code)
                if self.stop_requested:
                    self.log("Proceso detenido por el usuario.")
                    mark_remaining_omitted(index + 1, "ejecucion_detenida_por_usuario")
                    break

                self.log(f"Error en {spec.label} (Codigo {return_code})")
                if not self.continue_error_var.get():
                    self.log("Abortando ejecucion; se registran los stages pendientes.")
                    mark_remaining_omitted(index + 1, f"abortada_por_fallo:{spec.code}")
                    break

            except Exception as exc:
                unsuccessful_codes.add(spec.code)
                if not terminal_recorded:
                    if cmd is None:
                        cmd = [
                            self.build_python_exec(),
                            str(SCRIPTS_DIR / spec.filename),
                            "--since",
                            since,
                            "--before",
                            before,
                        ]
                    if started_at is None:
                        started_at = ORQ.utc_now()
                    record(
                        spec,
                        cmd,
                        started_at=started_at,
                        status="detenida" if self.stop_requested else "fallida",
                        return_code=None,
                        reason=f"{type(exc).__name__}: {exc}",
                    )
                self.log(f"Error inesperado ejecutando {spec.label}: {exc}")
                if self.stop_requested or not self.continue_error_var.get():
                    reason = (
                        "ejecucion_detenida_por_usuario"
                        if self.stop_requested
                        else f"abortada_por_fallo:{spec.code}"
                    )
                    mark_remaining_omitted(index + 1, reason)
                    break

        self.pipeline_had_error = bool(unsuccessful_codes)
        self.log(f"\nProceso terminado. Bitacora: {run_log}")
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
        exact_since = run.get("since")
        exact_before = run.get("before")
        history_run_id = ORQ.create_run_id(exact_since, exact_before) if exact_since and exact_before else ""
        history_started_at = ORQ.utc_now()

        def record_sna_history(status: str, return_code: int | None, reason: str = "") -> None:
            if not isinstance(exact_since, str) or not isinstance(exact_before, str):
                return
            ORQ.append_pipeline_record(
                run_id=history_run_id,
                pipeline_code="11",
                pipeline_key="analisis_sna",
                pipeline_label="Generar Analisis SNA",
                since=exact_since,
                before=exact_before,
                status=status,
                started_at=history_started_at,
                output_dir=results_dir,
                return_code=return_code,
                log_path=run_log,
                reason=reason,
            )
            self.root.after(0, self.refresh_download_history)

        record_sna_history("iniciada", None)

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

                for step_index, (label, script_name, args) in enumerate(steps, 1):
                    if self.stop_requested:
                        had_error = True
                        break

                    cmd = [python_exec, str(SCRIPTS_DIR / script_name), *args]
                    update_sna_run_manifest(
                        manifest_path,
                        step_index=step_index,
                        step_status="iniciada",
                    )
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
                            update_sna_run_manifest(
                                manifest_path,
                                step_index=step_index,
                                step_status="completada",
                                return_code=0,
                            )
                            sna_log(f"{label} finalizado con éxito.")
                            continue

                        had_error = True
                        step_status = "detenida" if self.stop_requested else "fallida"
                        update_sna_run_manifest(
                            manifest_path,
                            step_index=step_index,
                            step_status=step_status,
                            return_code=return_code,
                        )
                        if self.stop_requested:
                            sna_log("Proceso SNA detenido por el usuario.")
                            break
                        sna_log(f"Error en {label} (código {return_code}).")
                        if not self.continue_error_var.get():
                            sna_log("Abortando ejecución SNA.")
                            break
                    except Exception as exc:
                        had_error = True
                        update_sna_run_manifest(
                            manifest_path,
                            step_index=step_index,
                            step_status="detenida" if self.stop_requested else "fallida",
                            message=f"{type(exc).__name__}: {exc}",
                        )
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
                final_status = (
                    "completada"
                    if success
                    else "detenida"
                    if self.stop_requested
                    else "fallida"
                )
                update_sna_run_manifest(
                    manifest_path,
                    run_status=final_status,
                    message="ejecucion_sna_incompleta" if not success else "",
                )
                record_sna_history(
                    final_status,
                    0 if success else None,
                    "" if success else "ejecucion_sna_incompleta",
                )
                sna_log(f"Bitácora: {run_log}")
            finally:
                self.root.after(0, self.finish_sna_ui, success, run)

    def finish_ui(self) -> None:
        run_log = self.active_run_log
        self.play_button.config(state=tk.NORMAL)
        self.set_sna_buttons_state(tk.NORMAL)
        self.stop_button.config(state=tk.DISABLED)
        self.running_process = None
        self.active_run_id = ""
        self.active_run_log = None
        if not self.stop_requested and self.pipeline_had_error:
            messagebox.showwarning(
                "Ejecucion incompleta",
                "Algunos stages fallaron o fueron omitidos por dependencias. "
                f"Revisa la bitacora:\n{run_log}",
            )
        elif not self.stop_requested:
            messagebox.showinfo(
                "Finalizado",
                f"Todos los stages seleccionados concluyeron.\nBitacora:\n{run_log}",
            )

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
