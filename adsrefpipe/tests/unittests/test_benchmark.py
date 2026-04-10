import types
import json
import os
import tempfile
import unittest
from unittest.mock import patch

import sys

fake_requests = types.SimpleNamespace(
    post=lambda *args, **kwargs: None,
    get=lambda *args, **kwargs: None,
    exceptions=types.SimpleNamespace(RequestException=Exception),
)
sys.modules.setdefault("requests", fake_requests)
sys.modules.setdefault(
    "adsputils",
    types.SimpleNamespace(
        load_config=lambda *args, **kwargs: {},
        setup_logging=lambda *args, **kwargs: types.SimpleNamespace(info=lambda *a, **k: None, error=lambda *a, **k: None, debug=lambda *a, **k: None),
    ),
)

from adsrefpipe import benchmark


class TestBenchmark(unittest.TestCase):

    def test_collect_candidate_files_filters_results(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            raw_path = os.path.join(tmpdir, "one.raw")
            xml_path = os.path.join(tmpdir, "two.jats.xml")
            ignored_path = os.path.join(tmpdir, "three.raw.result")
            with open(raw_path, "w") as handle:
                handle.write("raw")
            with open(xml_path, "w") as handle:
                handle.write("xml")
            with open(ignored_path, "w") as handle:
                handle.write("ignored")

            files = benchmark.collect_candidate_files(tmpdir, ["*.raw", "*.xml"])
            self.assertEqual(files, sorted([raw_path, xml_path]))

    def test_classify_source_file_prefers_parser_extension(self):
        payload = benchmark.classify_source_file(
            "/tmp/foo.iop.xml",
            {"name": "IOP", "extension_pattern": ".iop.xml"},
        )
        self.assertEqual(payload["parser_name"], "IOP")
        self.assertEqual(payload["source_type"], ".iop.xml")

    def test_mock_resolver_returns_deterministic_payload(self):
        resolved = benchmark._mock_resolved_reference({"id": "H1I1", "refstr": "A ref"}, "http://example/text")
        self.assertEqual(resolved[0]["id"], "H1I1")
        self.assertEqual(resolved[0]["bibcode"], "2000mock........A")

    def test_run_case_mock_mode_collects_summary(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            sample_file = os.path.join(tmpdir, "sample.raw")
            with open(sample_file, "w") as handle:
                handle.write("content")
            events_path = os.path.join(tmpdir, "events.jsonl")

            def fake_process_files(files):
                for index, filename in enumerate(files, start=1):
                    extra = {
                        "source_filename": filename,
                        "source_type": ".raw",
                        "input_extension": ".raw",
                        "parser_name": "arXiv",
                        "record_count": 1,
                    }
                    benchmark.perf_metrics.emit_event("ingest_enqueue", record_id=None, extra=extra)
                    benchmark.perf_metrics.emit_event("parse_dispatch", record_id=None, duration_ms=5.0, extra=extra)
                    benchmark.perf_metrics.emit_event("resolver_http", record_id="rec-%s" % index, duration_ms=4.0, extra=extra)
                    benchmark.perf_metrics.emit_event("post_resolved_db", record_id="rec-%s" % index, duration_ms=3.0, extra=extra)
                    benchmark.perf_metrics.emit_event("record_wall", record_id="rec-%s" % index, duration_ms=12.0, extra=extra)
                    benchmark.perf_metrics.emit_event("file_wall", record_id=None, duration_ms=12.0, extra=extra)

            class FakePipelineRun:
                @staticmethod
                def process_files(files):
                    return fake_process_files(files)

            with patch.object(benchmark, "_pipeline_run_module", return_value=FakePipelineRun):
                summary = benchmark._run_case(
                    input_path=tmpdir,
                    extensions=["*.raw"],
                    max_files=1,
                    mode="mock",
                    events_path=events_path,
                    system_sample_interval_s=0.01,
                    system_load_enabled=False,
                    warmup=False,
                    group_by="source_type",
                )

            self.assertEqual(summary["status"], "complete")
            self.assertEqual(summary["counts"]["files_processed"], 1)
            self.assertEqual(summary["counts"]["records_processed"], 1)
            self.assertIn(".raw", summary["source_type_breakdown"])
            self.assertGreater(summary["throughput"]["overall_records_per_minute"], 0)

    def test_cmd_run_prints_artifacts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = os.path.join(tmpdir, "input")
            os.makedirs(input_path)
            with open(os.path.join(input_path, "sample.raw"), "w") as handle:
                handle.write("content")

            fake_summary = {
                "status": "complete",
                "counts": {"files_selected": 1, "files_processed": 1, "records_submitted": 1, "records_processed": 1, "failures": 0},
                "throughput": {"overall_records_per_minute": 60.0, "load_adjusted_records_per_minute": 60.0},
                "duration_s": {"wall_clock": 1.0},
                "per_record_metrics_ms": {},
                "source_type_breakdown": {".raw": {"file_count": 1, "record_count": 1, "wall_time_ms": {}, "parse_stage_ms": {}, "resolver_stage_ms": {}, "db_stage_ms": {}, "throughput_records_per_minute": 60.0}},
                "parser_breakdown": {},
                "system_load": {"collection": {"enabled": False}, "summary": {}},
                "run_metadata": {"run_id": "run-1"},
            }

            args = benchmark.build_parser().parse_args([
                "run",
                "--input-path", input_path,
                "--output-dir", tmpdir,
                "--events-path", os.path.join(tmpdir, "events.jsonl"),
                "--no-warmup",
                "--disable-system-load",
            ])

            with patch.object(benchmark, "_run_case", return_value=fake_summary):
                with patch("sys.stdout.write") as mock_write:
                    rc = benchmark.cmd_run(args)

            self.assertEqual(rc, 0)
            rendered = "".join(call.args[0] for call in mock_write.call_args_list)
            self.assertIn('"status": "complete"', rendered)
            self.assertTrue(any(name.endswith(".json") for name in os.listdir(tmpdir)))
            self.assertTrue(any(name.endswith(".source_types.csv") for name in os.listdir(tmpdir)))


if __name__ == "__main__":
    unittest.main()
