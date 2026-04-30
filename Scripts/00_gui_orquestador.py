#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import threading
import tkinter as tk
from datetime import datetime
from pathlib import Path
from tkinter import messagebox, scrolledtext, ttk


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
DEFAULT_GLOBAL_ISO_WEEK = ORQ.DEFAULT_GLOBAL_ISO_WEEK
DEFAULT_GLOBAL_SINCE, DEFAULT_GLOBAL_BEFORE = ORQ.iso_week_to_range(DEFAULT_GLOBAL_ISO_WEEK)


def validate_date(value: str) -> str:
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d").date().isoformat()
    except ValueError as exc:
        raise ValueError(f"Fecha invalida '{value}', usa YYYY-MM-DD") from exc


def parse_date_range(since: str, before: str) -> tuple[str, str]:
    parsed_since = validate_date(since)
    parsed_before = validate_date(before)
    if parsed_since > parsed_before:
        raise ValueError("La fecha inicial no puede ser mayor que la fecha final.")
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
        self.root.geometry("860x760")

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

        date_frame = ttk.LabelFrame(main_frame, text="Configuracion Temporal", padding="10")
        date_frame.pack(fill=tk.X, pady=5)

        ttk.Label(date_frame, text="Semana ISO (YYYY-Www):").grid(row=0, column=0, sticky=tk.W, padx=5)
        self.iso_week_var = tk.StringVar(value=DEFAULT_GLOBAL_ISO_WEEK)
        ttk.Entry(date_frame, textvariable=self.iso_week_var, width=15).grid(row=0, column=1, sticky=tk.W, padx=5)
        ttk.Button(date_frame, text="Usar Semana", command=self.update_dates_from_week).grid(row=0, column=2, padx=5)

        ttk.Label(date_frame, text="Desde (YYYY-MM-DD):").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)
        self.since_var = tk.StringVar(value=DEFAULT_GLOBAL_SINCE)
        ttk.Entry(date_frame, textvariable=self.since_var, width=15).grid(row=1, column=1, sticky=tk.W, padx=5)

        ttk.Label(date_frame, text="Hasta (YYYY-MM-DD):").grid(row=1, column=2, sticky=tk.W, padx=5)
        self.before_var = tk.StringVar(value=DEFAULT_GLOBAL_BEFORE)
        ttk.Entry(date_frame, textvariable=self.before_var, width=15).grid(row=1, column=3, sticky=tk.W, padx=5)

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

        control_frame = ttk.Frame(main_frame, padding="10")
        control_frame.pack(fill=tk.X)

        self.play_button = ttk.Button(control_frame, text="EJECUTAR", command=self.start_execution)
        self.play_button.pack(side=tk.LEFT, padx=5, expand=True, fill=tk.X)

        self.stop_button = ttk.Button(control_frame, text="DETENER", command=self.stop_execution, state=tk.DISABLED)
        self.stop_button.pack(side=tk.LEFT, padx=5, expand=True, fill=tk.X)

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

    def update_dates_from_week(self) -> None:
        week = self.iso_week_var.get().strip()
        try:
            since, before = ORQ.iso_week_to_range(week)
            self.since_var.set(since)
            self.before_var.set(before)
        except Exception as exc:
            messagebox.showerror("Error", f"Semana ISO invalida: {exc}")

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

        unique_selected = []
        seen = set()
        for spec in selected:
            if spec.code not in seen:
                unique_selected.append(spec)
                seen.add(spec.code)
        return unique_selected

    def start_execution(self) -> None:
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
        self.stop_button.config(state=tk.NORMAL)
        self.stop_requested = False

        thread = threading.Thread(target=self.run_pipelines, args=(selected, since, before), daemon=True)
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

                if return_code == 0:
                    self.log(f"{spec.label} finalizado con exito.")
                    if spec.code == "4":
                        output_dir_arg = ORQ._extract_flag_value(cmd, "--output-dir") or str(REPO_ROOT / "Facebook")
                        report_tag = ORQ.build_report_tag(since, "Facebook")
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
                self.log(f"Error inesperado ejecutando {spec.label}: {exc}")
                if not self.continue_error_var.get():
                    break

        self.log("\nProceso terminado.")
        self.root.after(0, self.finish_ui)

    def finish_ui(self) -> None:
        self.play_button.config(state=tk.NORMAL)
        self.stop_button.config(state=tk.DISABLED)
        self.running_process = None
        if not self.stop_requested:
            messagebox.showinfo("Finalizado", "La ejecucion de los pipelines ha concluido.")


if __name__ == "__main__":
    root = tk.Tk()
    app = OrquestadorGUI(root)
    root.mainloop()