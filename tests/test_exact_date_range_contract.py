from __future__ import annotations

import importlib.util
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
    write_range_contract,
)
from download_history import (  # noqa: E402
    append_download_record,
    latest_downloads_by_pipeline,
    read_download_history,
)
from sna_recent_ranges import discover_source_ranges, resolve_recent_scope  # noqa: E402


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
        self.assertEqual(
            manifest["selected_ranges"][0]["identity"],
            "2026_agosto_05_al_2026_agosto_13",
        )


class PipelinePropagationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.orq = load_script("test_tampico_orq", "00_orquestador_general.py")
        cls.sna = load_script("test_tampico_sna", "20_generar_analisis_sna.py")
        cls.consolidator = load_script("test_tampico_consolidator", "6_consolidador_datos.py")
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
