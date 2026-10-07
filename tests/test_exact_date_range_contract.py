from __future__ import annotations

import importlib.util
import asyncio
import json
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "Scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from output_naming import (  # noqa: E402
    build_range_label,
    build_range_report_tag,
    validate_date_range,
    validate_range_contract_file,
    write_range_contract,
)
from download_history import (  # noqa: E402
    append_download_record,
    append_pipeline_record,
    latest_downloads_by_pipeline,
    latest_pipeline_records,
    pipeline_completed_for_range,
    read_download_history,
    read_pipeline_history,
)
from sna_recent_ranges import (  # noqa: E402
    discover_material_ranges,
    discover_source_ranges,
    resolve_recent_scope,
)


def load_script(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS_DIR / filename)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"No se pudo cargar {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class ExactDateRangeContractTests(unittest.TestCase):
    def test_spanish_storage_tag_keeps_both_exact_boundaries(self) -> None:
        self.assertEqual(
            build_range_label("2026-08-01", "2026-08-09"),
            "2026_agosto_01_al_2026_agosto_09",
        )
        self.assertEqual(
            build_range_report_tag("2026-08-01", "2026-08-09", "Facebook"),
            "2026_agosto_01_al_2026_agosto_09_Facebook",
        )

    def test_cross_year_range_is_unambiguous(self) -> None:
        self.assertEqual(
            build_range_label("2026-12-28", "2027-01-04"),
            "2026_diciembre_28_al_2027_enero_04",
        )

    def test_empty_or_reversed_range_is_rejected(self) -> None:
        for since, before in (
            ("2026-08-01", "2026-08-01"),
            ("2026-08-09", "2026-08-01"),
        ):
            with self.subTest(since=since, before=before):
                with self.assertRaises(ValueError):
                    validate_date_range(since, before)

    def test_manifest_makes_exclusive_boundary_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = write_range_contract(
                tmp,
                "2026-08-01",
                "2026-08-09",
                "Datos",
            )
            payload = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(payload["since"], "2026-08-01")
        self.assertEqual(payload["before"], "2026-08-09")
        self.assertEqual(payload["interval"], "[since,before)")
        self.assertTrue(payload["before_is_exclusive"])
        self.assertEqual(payload["timezone"], "UTC")


class DownloadHistoryTests(unittest.TestCase):
    def test_history_is_append_only_and_tolerates_malformed_lines(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            history_path = Path(tmp) / "download_history.jsonl"
            append_download_record(
                pipeline_code="1",
                pipeline_key="youtube",
                pipeline_label="YouTube",
                since="2026-08-01",
                before="2026-08-09",
                status="completada",
                started_at="2026-08-26T10:00:00Z",
                finished_at="2026-08-26T10:05:00Z",
                output_dir="/tmp/primera",
                return_code=0,
                history_path=history_path,
            )
            with history_path.open("a", encoding="utf-8") as handle:
                handle.write("línea dañada\n")
            append_download_record(
                pipeline_code="1",
                pipeline_key="youtube",
                pipeline_label="YouTube",
                since="2026-08-10",
                before="2026-08-17",
                status="fallida",
                started_at="2026-08-26T11:00:00Z",
                finished_at="2026-08-26T11:01:00Z",
                output_dir="/tmp/segunda",
                return_code=1,
                history_path=history_path,
            )
            records = read_download_history(history_path)

        self.assertEqual(len(records), 2)
        latest = latest_downloads_by_pipeline(records)
        self.assertEqual(len(latest), 1)
        self.assertEqual(latest[0]["since"], "2026-08-10")
        self.assertEqual(latest[0]["status"], "fallida")


class PipelineHistoryTests(unittest.TestCase):
    def test_all_stages_can_record_started_and_terminal_status(self) -> None:
        stages = [
            ("1", "youtube"),
            ("2", "twitter"),
            ("3", "medios_tampico"),
            ("4", "facebook_posts"),
            ("5", "facebook_comentarios"),
            ("12", "instagram"),
            ("13", "tiktok"),
            ("6", "consolidador_datos"),
            ("7", "claude_nlp"),
            ("8", "influencia_temas"),
            ("9", "temas_guiados"),
            ("10", "publicaciones_institucionales_claude"),
            ("11", "analisis_sna"),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            history_path = Path(tmp) / "pipeline_history.jsonl"
            download_path = Path(tmp) / "download_history.jsonl"
            for code, key in stages:
                common = {
                    "run_id": "run-1",
                    "pipeline_code": code,
                    "pipeline_key": key,
                    "pipeline_label": key,
                    "since": "2026-08-01",
                    "before": "2026-08-09",
                    "started_at": "2026-08-26T10:00:00Z",
                    "history_path": history_path,
                }
                append_pipeline_record(status="iniciada", **common)
                append_pipeline_record(status="completada", return_code=0, **common)

            records = read_pipeline_history(history_path)
            latest = latest_pipeline_records(
                records,
                since="2026-08-01",
                before="2026-08-09",
            )
            self.assertEqual(len(records), len(stages) * 2)
            self.assertEqual(len(latest), len(stages))
            self.assertTrue(all(row["status"] == "completada" for row in latest))
            for _, key in stages:
                self.assertTrue(
                    pipeline_completed_for_range(
                        key,
                        "2026-08-01",
                        "2026-08-09",
                        pipeline_history_path=history_path,
                        download_history_path=download_path,
                    )
                )

    def test_interrupted_restart_is_not_treated_as_completed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            history_path = Path(tmp) / "pipeline_history.jsonl"
            common = {
                "pipeline_code": "7",
                "pipeline_key": "claude_nlp",
                "pipeline_label": "Claude",
                "since": "2026-08-01",
                "before": "2026-08-09",
                "started_at": "2026-08-26T10:00:00Z",
                "history_path": history_path,
            }
            append_pipeline_record(run_id="run-1", status="completada", **common)
            append_pipeline_record(run_id="run-2", status="iniciada", **common)
            self.assertFalse(
                pipeline_completed_for_range(
                    "claude_nlp",
                    "2026-08-01",
                    "2026-08-09",
                    pipeline_history_path=history_path,
                    download_history_path=Path(tmp) / "downloads.jsonl",
                )
            )

    def test_omission_does_not_invalidate_a_previous_completion(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            history_path = Path(tmp) / "pipeline_history.jsonl"
            common = {
                "pipeline_code": "8",
                "pipeline_key": "influencia_temas",
                "pipeline_label": "Influencia",
                "since": "2026-08-01",
                "before": "2026-08-09",
                "started_at": "2026-08-26T10:00:00Z",
                "history_path": history_path,
            }
            append_pipeline_record(run_id="run-1", status="completada", **common)
            append_pipeline_record(
                run_id="run-2",
                status="omitida",
                reason="abortada_por_fallo:7",
                **common,
            )
            self.assertTrue(
                pipeline_completed_for_range(
                    "influencia_temas",
                    "2026-08-01",
                    "2026-08-09",
                    pipeline_history_path=history_path,
                    download_history_path=Path(tmp) / "downloads.jsonl",
                )
            )

    def test_contract_validator_rejects_wrong_range(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            write_range_contract(tmp, "2026-08-01", "2026-08-09", "Datos")
            valid, _ = validate_range_contract_file(
                tmp,
                "2026-08-01",
                "2026-08-09",
                "Datos",
            )
            wrong, detail = validate_range_contract_file(
                tmp,
                "2026-08-02",
                "2026-08-09",
                "Datos",
            )
        self.assertTrue(valid)
        self.assertFalse(wrong)
        self.assertIn("contrato incompatible", detail)


class TwitterExactRangeTests(unittest.TestCase):
    def test_scroll_stop_uses_the_same_utc_exact_range(self) -> None:
        twitter = load_script("test_tampico_twitter", "2_extractors_twitter.py")

        async def no_search(_page, _query):
            return None

        visible_tweets = [
            {
                "author": "@TampicoGob",
                "datetime": "2026-08-03T12:00:00Z",
                "url": "https://x.com/TampicoGob/status/1",
                "text": "Dentro del rango",
                "replies": 0,
                "retweets": 0,
                "likes": 0,
                "bookmarks": 0,
                "views": 0,
            },
            {
                "author": "@TampicoGob",
                "datetime": "2026-08-09T00:00:00Z",
                "url": "https://x.com/TampicoGob/status/2",
                "text": "En el límite exclusivo",
                "replies": 0,
                "retweets": 0,
                "likes": 0,
                "bookmarks": 0,
                "views": 0,
            },
            {
                "author": "@TampicoGob",
                "datetime": "2026-07-31T23:59:59Z",
                "url": "https://x.com/TampicoGob/status/3",
                "text": "Anterior al rango",
                "replies": 0,
                "retweets": 0,
                "likes": 0,
                "bookmarks": 0,
                "views": 0,
            },
        ]

        async def extract_visible(_page):
            return visible_tweets

        with tempfile.TemporaryDirectory() as tmp:
            extractor = twitter.TwitterExtractorIAD(
                "2026-08-01",
                "2026-08-09",
                output_base_dir=Path(tmp),
            )
            extractor.goto_search = no_search
            extractor.extract_visible_tweets = extract_visible
            rows = asyncio.run(extractor.extract_query_data(object(), "tampico"))

        self.assertEqual([row["text"] for row in rows], ["Dentro del rango"])
        self.assertEqual(rows[0]["fecha_inicio_rango"], "2026-08-01")
        self.assertEqual(rows[0]["nombre_rango"], "2026_agosto_01_al_2026_agosto_09_Twitter")

class SnaRecentRangeTests(unittest.TestCase):
    @staticmethod
    def _write_batch(
        root: Path,
        source: str,
        identity: str,
        dates: list[str],
    ) -> None:
        folder = root / source / f"{identity}_{source}"
        folder.mkdir(parents=True, exist_ok=True)
        rows = ["fecha,texto", *(f"{value},registro" for value in dates)]
        (folder / "datos.csv").write_text("\n".join(rows) + "\n", encoding="utf-8")

    def test_recent_ranges_are_inferred_from_csv_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_batch(root, "Twitter", "lote_a", ["2026-07-15", "2026-07-22"])
            self._write_batch(root, "Twitter", "lote_b", ["2026-07-22", "2026-07-29"])
            self._write_batch(root, "Twitter", "lote_c", ["2026-08-05", "2026-08-12"])
            self._write_batch(root, "Facebook", "lote_c", ["2026-08-05", "2026-08-11"])
            empty_contract = root / "Twitter" / "rango_sin_csv"
            write_range_contract(empty_contract, "2026-08-20", "2026-08-27", "Twitter")

            ranges = discover_source_ranges(root)
            recent = resolve_recent_scope(root, 2)

        self.assertEqual(
            [item.identity for item in ranges],
            [
                "2026_julio_15_al_2026_julio_23",
                "2026_julio_22_al_2026_julio_30",
                "2026_agosto_05_al_2026_agosto_13",
            ],
        )
        self.assertEqual(recent.since.isoformat(), "2026-07-22")
        self.assertEqual(recent.before.isoformat(), "2026-08-13")
        self.assertEqual(
            [item.identity for item in recent.selected_ranges],
            [
                "2026_julio_22_al_2026_julio_30",
                "2026_agosto_05_al_2026_agosto_13",
            ],
        )
        self.assertEqual(recent.selected_ranges[-1].sources, ("Facebook", "Twitter"))

    def test_gui_recent_sna_runs_use_unique_data_and_result_directories(self) -> None:
        gui = load_script("test_tampico_gui", "00_gui_orquestador.py")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_batch(root, "Facebook", "lote_reciente", ["2026-08-05", "2026-08-12"])
            first = gui.build_sna_run(
                "ultimo_rango",
                repo_root=root,
                now=datetime(2026, 8, 26, 10, 0, 0, 1),
            )
            second = gui.build_sna_run(
                "ultimo_rango",
                repo_root=root,
                now=datetime(2026, 8, 26, 10, 0, 0, 2),
            )
            manifest_path = gui.write_sna_run_manifest(first)
            assert manifest_path is not None
            gui.update_sna_run_manifest(
                manifest_path,
                step_index=1,
                step_status="completada",
                return_code=0,
            )
            gui.update_sna_run_manifest(
                manifest_path,
                run_status="fallida",
                message="prueba_de_fallo",
            )
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

        self.assertNotEqual(first["input_csv"], second["input_csv"])
        self.assertNotEqual(first["results_dir"], second["results_dir"])
        self.assertEqual(
            first["input_csv"].parent.name,
            "ejecucion_20260826T100000_000001",
        )
        self.assertEqual(
            first["results_dir"].name,
            "ejecucion_20260826T100000_000001",
        )
        self.assertEqual(manifest["since"], "2026-08-05")
        self.assertEqual(manifest["before"], "2026-08-13")
        self.assertEqual(manifest["status"], "fallida")
        self.assertEqual(manifest["steps"][0]["status"], "completada")
        self.assertTrue(
            all(step["status"] == "omitida" for step in manifest["steps"][1:])
        )
        self.assertEqual(
            manifest["selected_ranges"][0]["identity"],
            "2026_agosto_05_al_2026_agosto_13",
        )

    def test_gui_written_sna_range_requires_matching_material_folder(self) -> None:
        gui = load_script("test_tampico_gui_written_range", "00_gui_orquestador.py")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            material_dir = root / "Datos" / build_range_report_tag(
                "2026-08-12",
                "2026-08-19",
                "Datos",
            )
            write_range_contract(
                material_dir,
                "2026-08-12",
                "2026-08-19",
                "Datos",
            )
            (material_dir / "material_institucional.txt").write_text(
                "publicación\n",
                encoding="utf-8",
            )
            (material_dir / "material_comentarios.txt").write_text(
                "comentario\n",
                encoding="utf-8",
            )

            run = gui.build_sna_run(
                "rango_escrito",
                repo_root=root,
                since="2026-08-12",
                before="2026-08-19",
                now=datetime(2026, 8, 26, 10, 0, 0, 3),
            )

            self.assertEqual(run["since"], "2026-08-12")
            self.assertEqual(run["before"], "2026-08-19")
            self.assertEqual(
                run["selected_ranges"][0]["material_folder"],
                str(material_dir),
            )
            self.assertIn(material_dir.name, run["label"])

            with self.assertRaisesRegex(RuntimeError, "No existe la carpeta"):
                gui.build_sna_run(
                    "rango_escrito",
                    repo_root=root,
                    since="2026-08-19",
                    before="2026-08-26",
                )

    def test_gui_keeps_historical_and_two_recent_shortcuts(self) -> None:
        source = (SCRIPTS_DIR / "00_gui_orquestador.py").read_text(encoding="utf-8")
        self.assertIn("EJECUTAR SNA MATERIAL HISTÓRICO", source)
        self.assertIn("EJECUTAR SNA 2 RANGOS RECIENTES", source)
        self.assertIn("EJECUTAR SNA CON LOS RANGOS SELECCIONADOS", source)
        self.assertNotIn("EJECUTAR SNA ÚLTIMO RANGO", source)

    def test_gui_sections_have_draggable_resize_panes(self) -> None:
        source = (SCRIPTS_DIR / "00_gui_orquestador.py").read_text(encoding="utf-8")
        self.assertIn("self.root.resizable(True, True)", source)
        self.assertGreaterEqual(source.count("tk.PanedWindow("), 3)
        self.assertIn("orient=tk.VERTICAL", source)
        self.assertIn("orient=tk.HORIZONTAL", source)
        self.assertIn("sashwidth=8", source)

    def test_material_range_selector_discovers_legacy_and_exact_folders(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for batch, dates in (
                ("2026_W14", ["2026-03-30", "2026-04-07"]),
                ("2026_agosto_12_al_2026_agosto_19", ["2026-08-12", "2026-08-18"]),
            ):
                self._write_batch(root, "Twitter", batch, dates)
                material_dir = root / "Datos" / f"{batch}_Datos"
                material_dir.mkdir(parents=True)
                (material_dir / "material_institucional.txt").write_text(
                    "publicación\n", encoding="utf-8"
                )
                (material_dir / "material_comentarios.txt").write_text(
                    "comentario\n", encoding="utf-8"
                )
            write_range_contract(
                root / "Datos" / "2026_agosto_12_al_2026_agosto_19_Datos",
                "2026-08-12",
                "2026-08-19",
                "Datos",
            )

            ranges = discover_material_ranges(root)

        self.assertEqual([item.identity for item in ranges], ["2026_W14", "2026_agosto_12_al_2026_agosto_19"])
        self.assertEqual(ranges[0].since.isoformat(), "2026-03-30")
        self.assertEqual(ranges[0].before.isoformat(), "2026-04-08")
        self.assertTrue(ranges[0].inferred_from_rows)
        self.assertEqual(ranges[1].before.isoformat(), "2026-08-19")
        self.assertFalse(ranges[1].inferred_from_rows)

    def test_selected_sna_ranges_build_an_exact_union_command(self) -> None:
        gui = load_script("test_tampico_gui_selected_ranges", "00_gui_orquestador.py")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for batch, dates in (
                ("lote_a", ["2026-07-01", "2026-07-03"]),
                ("lote_b", ["2026-08-12", "2026-08-18"]),
            ):
                self._write_batch(root, "Twitter", batch, dates)
                material_dir = root / "Datos" / f"{batch}_Datos"
                material_dir.mkdir(parents=True)
                (material_dir / "material_institucional.txt").write_text("a\n", encoding="utf-8")
                (material_dir / "material_comentarios.txt").write_text("b\n", encoding="utf-8")
            ranges = discover_material_ranges(root)
            run = gui.build_sna_run(
                "rangos_seleccionados",
                repo_root=root,
                selected_material_ranges=ranges,
                now=datetime(2026, 8, 26, 10, 0, 0, 4),
            )

        consolidate_args = run["steps"][0][2]
        self.assertEqual(consolidate_args.count("--include-range"), 2)
        self.assertIn("2026-07-01", consolidate_args)
        self.assertIn("2026-08-19", consolidate_args)
        self.assertEqual(run["selection_mode"], "union_exacta")
        self.assertEqual(len(run["selected_ranges"]), 2)
        self.assertIn("selecciones", str(run["results_dir"]))

    def test_existing_material_does_not_auto_add_consolidator(self) -> None:
        gui = load_script("test_tampico_gui_dependencies", "00_gui_orquestador.py")
        instance = object.__new__(gui.OrquestadorGUI)
        instance.log = lambda _message: None

        without_material = instance.validate_dependencies(
            [gui.PIPELINES_BY_CODE["7"]],
            material_available=False,
        )
        with_material = instance.validate_dependencies(
            [gui.PIPELINES_BY_CODE["7"]],
            material_available=True,
        )

        self.assertEqual([item.code for item in without_material], ["6", "7"])
        self.assertEqual([item.code for item in with_material], ["7"])

    def test_missing_material_does_not_block_downloads_before_analysis(self) -> None:
        gui = load_script("test_tampico_gui_material_probe", "00_gui_orquestador.py")
        instance = object.__new__(gui.OrquestadorGUI)
        instance.log = lambda _message: None

        with tempfile.TemporaryDirectory() as tmp:
            material_folder, detail = gui.probe_material_folder_for_selected(
                {"1", "7"},
                "2026-08-26",
                "2026-09-02",
                repo_root=Path(tmp),
            )

        selected = instance.validate_dependencies(
            [gui.PIPELINES_BY_CODE["1"], gui.PIPELINES_BY_CODE["7"]],
            material_available=material_folder is not None,
        )
        self.assertIsNone(material_folder)
        self.assertIn("No existe la carpeta de material", detail)
        self.assertEqual([item.code for item in selected], ["1", "6", "7"])

    def test_sna_stage_does_not_require_weekly_datos_folder(self) -> None:
        gui = load_script("test_tampico_gui_sna_material_probe", "00_gui_orquestador.py")

        with tempfile.TemporaryDirectory() as tmp:
            material_folder, detail = gui.probe_material_folder_for_selected(
                {"11"},
                "2026-08-26",
                "2026-09-02",
                repo_root=Path(tmp),
            )

        self.assertIsNone(material_folder)
        self.assertEqual(detail, "")


class PipelinePropagationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.orq = load_script("test_tampico_orq", "00_orquestador_general.py")
        cls.sna = load_script("test_tampico_sna", "20_generar_analisis_sna.py")
        cls.consolidator = load_script("test_tampico_consolidator", "6_consolidador_datos.py")
        cls.sna_consolidator = load_script(
            "test_tampico_sna_consolidator",
            "11_consolidar_historico_sna.py",
        )
        cls.facebook_posts = load_script(
            "test_tampico_facebook_posts",
            "4_extractors_facebook_posts.py",
        )
        cls.facebook_comments = load_script(
            "test_tampico_facebook_comments",
            "5_extractors_facebook_comentarios.py",
        )

    def test_gui_exposes_only_exact_date_inputs(self) -> None:
        source = (SCRIPTS_DIR / "00_gui_orquestador.py").read_text(encoding="utf-8")
        self.assertIn("Desde inclusivo (YYYY-MM-DD)", source)
        self.assertIn("Antes de, exclusivo (YYYY-MM-DD)", source)

    def test_every_orchestrated_stage_receives_exact_range(self) -> None:
        since = "2026-08-01"
        before = "2026-08-09"
        for pipeline in self.orq.PIPELINES:
            command, _ = self.orq.build_pipeline(
                pipeline,
                since,
                before,
                use_defaults=True,
            )
            with self.subTest(stage=pipeline.code):
                self.assertIn("--since", command)
                self.assertIn("--before", command)
                self.assertEqual(command[command.index("--since") + 1], since)
                self.assertEqual(command[command.index("--before") + 1], before)

    def test_every_stage_has_a_range_scoped_contract_destination(self) -> None:
        since = "2026-08-01"
        before = "2026-08-09"
        for pipeline in self.orq.PIPELINES:
            command, _ = self.orq.build_pipeline(
                pipeline,
                since,
                before,
                use_defaults=True,
            )
            destination = self.orq.range_output_dir_for_command(
                pipeline,
                since,
                before,
                command,
            )
            with self.subTest(stage=pipeline.code):
                self.assertIsNotNone(destination)
                self.assertIn("2026_agosto_01_al_2026_agosto_09", str(destination))

    def test_gui_option_removes_chucho_nader_targets_from_all_extractors(self) -> None:
        since = "2026-08-01"
        before = "2026-08-09"
        for code in ("1", "2", "3", "4", "12", "13"):
            command, _ = self.orq.build_pipeline(
                self.orq.PIPELINES_BY_CODE[code],
                since,
                before,
                use_defaults=True,
                include_chucho_nader=False,
            )
            rendered = " ".join(command).casefold()
            with self.subTest(stage=code):
                self.assertNotIn("chucho nader", rendered)
                self.assertNotIn("chuchonader", rendered)
                self.assertNotIn("jesus nader", rendered)
                self.assertNotIn("diputado nader", rendered)

    def test_gui_option_keeps_chucho_nader_targets_when_enabled(self) -> None:
        for code in ("1", "2", "3", "4", "12", "13"):
            command, _ = self.orq.build_pipeline(
                self.orq.PIPELINES_BY_CODE[code],
                "2026-08-01",
                "2026-08-09",
                use_defaults=True,
                include_chucho_nader=True,
            )
            with self.subTest(stage=code):
                self.assertTrue(
                    any(
                        marker in " ".join(command).casefold()
                        for marker in self.orq.CHUCHO_NADER_MARKERS
                    )
                )

    def test_failure_only_blocks_dependent_stages(self) -> None:
        selected = {pipeline.code for pipeline in self.orq.PIPELINES}
        self.assertEqual(
            self.orq.failed_dependencies_for_stage("7", selected, {"6"}),
            {"6"},
        )
        self.assertEqual(
            self.orq.failed_dependencies_for_stage("8", selected, {"7"}),
            set(),
        )
        self.assertEqual(
            self.orq.failed_dependencies_for_stage("6", selected, {"2"}),
            {"2"},
        )

    def test_facebook_resume_reconnects_posts_to_comments(self) -> None:
        posts = self.orq.PIPELINES_BY_CODE["4"]
        comments = self.orq.PIPELINES_BY_CODE["5"]
        prepared = [
            (posts, ["python", "posts.py"], {}),
            (comments, ["python", "comments.py"], {}),
        ]
        self.orq.inject_facebook_posts_input(prepared, 1, "/tmp/posts.csv")
        self.assertEqual(
            prepared[1][1][-2:],
            ["--input-csv", "/tmp/posts.csv"],
        )

    def test_consolidator_reads_only_matching_range_directories(self) -> None:
        sources = self.consolidator._sources(
            "2026-08-01",
            "2026-08-09",
            Path("/tmp/range-contract-test"),
        )
        rendered = "\n".join(str(path) for paths in sources.values() for path in paths)
        self.assertIn("2026_agosto_01_al_2026_agosto_09_Twitter", rendered)
        self.assertIn("2026_agosto_01_al_2026_agosto_09_Medios", rendered)
        self.assertNotIn("2026_W", rendered)

    def test_sna_consolidator_uses_union_without_intermediate_dates(self) -> None:
        original_root = self.sna_consolidator.REPO_ROOT
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            folder = root / "Twitter" / "lotes_Twitter"
            folder.mkdir(parents=True)
            (folder / "lotes_Twitter_comentarios.csv").write_text(
                "author,datetime_parsed_utc,text,url\n"
                "uno,2026-07-02T12:00:00Z,primer rango,https://x.test/1\n"
                "medio,2026-07-20T12:00:00Z,no seleccionado,https://x.test/2\n"
                "dos,2026-08-13T12:00:00Z,segundo rango,https://x.test/3\n",
                encoding="utf-8",
            )
            self.sna_consolidator.REPO_ROOT = root
            try:
                output, _inventory = self.sna_consolidator.consolidate(
                    included_ranges=[
                        ("2026-07-01", "2026-07-05"),
                        ("2026-08-12", "2026-08-19"),
                    ]
                )
            finally:
                self.sna_consolidator.REPO_ROOT = original_root

        self.assertEqual(
            output["texto_original"].tolist(),
            ["primer rango", "segundo rango"],
        )

    def test_facebook_before_boundary_is_rejected(self) -> None:
        self.assertTrue(
            self.facebook_posts.in_date_range(
                datetime(2026, 8, 8, 23, 59),
                "2026-08-01",
                "2026-08-09",
            )
        )
        self.assertFalse(
            self.facebook_posts.in_date_range(
                datetime(2026, 8, 9, 0, 0),
                "2026-08-01",
                "2026-08-09",
            )
        )

    def test_facebook_comments_are_strictly_filtered(self) -> None:
        items = [
            {"text": "comentario dentro", "date": "2026-08-08T23:59:59Z"},
            {"text": "comentario en before", "date": "2026-08-09T00:00:00Z"},
            {"text": "comentario sin fecha"},
        ]
        rows = self.facebook_comments.procesar_items_comentarios(
            items,
            "2026-08-01",
            "2026-08-09",
        )
        self.assertEqual([row["comentario_texto"] for row in rows], ["comentario dentro"])

    def test_sna_chain_uses_range_scoped_data_and_results(self) -> None:
        steps, data_dir, results_dir = self.sna.build_steps(
            since="2026-08-01",
            before="2026-08-09",
        )
        tag = "2026_agosto_01_al_2026_agosto_09_SNA"
        self.assertEqual(data_dir.name, tag)
        self.assertEqual(results_dir.name, tag)
        self.assertEqual(len(steps), 12)
        rendered = "\n".join(" ".join(command) for _, command in steps)
        self.assertIn("--since 2026-08-01 --before 2026-08-09", rendered)
        self.assertIn(str(data_dir), rendered)
        self.assertIn(str(results_dir), rendered)
        self.assertNotIn("SNA/Resultados/historico", rendered)


if __name__ == "__main__":
    unittest.main()
