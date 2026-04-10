import json
import os
import tempfile
import unittest

import adsrefpipe.perf_metrics as perf_metrics


class TestPerfMetrics(unittest.TestCase):

    def test_source_type_from_filename(self):
        self.assertEqual(perf_metrics.source_type_from_filename("foo.raw"), ".raw")
        self.assertEqual(perf_metrics.source_type_from_filename("foo.jats.xml"), ".jats.xml")
        self.assertEqual(perf_metrics.source_type_from_filename("foo.iop.xml"), ".iop.xml")
        self.assertEqual(perf_metrics.source_type_from_filename("foo.living.xml"), ".living.xml")
        self.assertEqual(perf_metrics.source_type_from_filename("foo.ref.ocr.txt"), ".ocr.txt")
        self.assertEqual(perf_metrics.source_type_from_filename("0000A&A.....0.....Z.raw"), ".raw")
        self.assertEqual(perf_metrics.source_type_from_filename("foo.tex"), ".tex")
        self.assertEqual(
            perf_metrics.raw_subfamily_from_metadata(
                filename="/tmp/txt/arXiv/0/00000.raw",
                parser_name="arXiv",
                source_type=".raw",
            ),
            "raw_arxiv",
        )
        self.assertEqual(
            perf_metrics.raw_subfamily_from_metadata(
                filename="/tmp/txt/ARA+A/0/0000ADSTEST.0.....Z.ref.raw",
                parser_name="ThreeBibsTxt",
                source_type=".raw",
            ),
            "raw_ref_raw",
        )
        self.assertEqual(
            perf_metrics.raw_subfamily_from_metadata(
                filename="/tmp/html/PASJ/0/iss0.raw",
                parser_name="PASJhtml",
                source_type=".raw",
            ),
            "raw_pasj_html",
        )
        self.assertEqual(
            perf_metrics.raw_subfamily_from_metadata(
                filename="/tmp/test.aas.raw",
                parser_name="AAS",
                source_type=".raw",
            ),
            "raw_aas",
        )

    def test_format_benchmark_progress_line_from_log_line(self):
        line = json.dumps({
            "timestamp": "2026-04-10T18:29:31.196Z",
            "message": (
                "Source file /app/adsrefpipe/tests/unittests/stubdata/test.jats.xml "
                "for bibcode 0000HiA.....Z with 16 references, processed successfully."
            ),
        })

        rendered = perf_metrics.format_benchmark_progress_line_from_log_line(line)

        self.assertEqual(rendered, "18:29:31 test.jats.xml with 16 references")

    def test_format_benchmark_progress_line_from_log_line_ignores_non_matches(self):
        self.assertIsNone(perf_metrics.format_benchmark_progress_line_from_log_line("not-json"))
        self.assertIsNone(perf_metrics.format_benchmark_progress_line_from_log_line(json.dumps({
            "timestamp": "2026-04-10T18:29:31.196Z",
            "message": "Updated 1 resolved reference records successfully.",
        })))

    def test_emit_event_uses_registered_context(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            events_path = os.path.join(tmpdir, "events.jsonl")
            context_dir = os.path.join(tmpdir, "context")
            config = {
                "PERF_METRICS_ENABLED": False,
                "PERF_METRICS_CONTEXT_DIR": context_dir,
            }
            os.environ["PERF_METRICS_CONTEXT_DIR"] = context_dir
            try:
                perf_metrics.register_run_metrics_context(
                    run_id="run-1",
                    enabled=True,
                    path=events_path,
                    context_id="ctx-1",
                    config=config,
                    context_dir=context_dir,
                )
                perf_metrics.emit_event(
                    stage="record_wall",
                    run_id="run-1",
                    context_id="ctx-1",
                    record_id="rec-1",
                    duration_ms=5.0,
                    extra={"source_type": ".raw"},
                    config=config,
                )
                payloads = perf_metrics.load_events(events_path, run_id="run-1", context_id="ctx-1")
                self.assertEqual(len(payloads), 1)
                self.assertEqual(payloads[0]["record_id"], "rec-1")
                self.assertEqual(payloads[0]["stage"], "record_wall")
            finally:
                os.environ.pop("PERF_METRICS_CONTEXT_DIR", None)

    def test_aggregate_ads_events_groups_by_source_type(self):
        events = [
            {
                "ts": 1.0,
                "stage": "ingest_enqueue",
                "duration_ms": 2.0,
                "status": "ok",
                "record_id": None,
                "extra": {"record_count": 2, "source_type": ".raw", "parser_name": "arXiv", "source_filename": "a.raw"},
            },
            {
                "ts": 2.0,
                "stage": "parse_dispatch",
                "duration_ms": 20.0,
                "status": "ok",
                "record_id": None,
                "extra": {"record_count": 2, "source_type": ".raw", "parser_name": "arXiv", "source_filename": "a.raw"},
            },
            {
                "ts": 3.0,
                "stage": "resolver_http",
                "duration_ms": 8.0,
                "status": "ok",
                "record_id": "r1",
                "extra": {"record_count": 1, "source_type": ".raw", "parser_name": "arXiv", "source_filename": "a.raw"},
            },
            {
                "ts": 4.0,
                "stage": "post_resolved_db",
                "duration_ms": 6.0,
                "status": "ok",
                "record_id": "r1",
                "extra": {"record_count": 1, "source_type": ".raw", "parser_name": "arXiv", "source_filename": "a.raw"},
            },
            {
                "ts": 5.0,
                "stage": "record_wall",
                "duration_ms": 18.0,
                "status": "ok",
                "record_id": "r1",
                "extra": {"record_count": 1, "source_type": ".raw", "parser_name": "arXiv", "source_filename": "a.raw"},
            },
            {
                "ts": 6.0,
                "stage": "record_wall",
                "duration_ms": 22.0,
                "status": "ok",
                "record_id": "r2",
                "extra": {"record_count": 1, "source_type": ".raw", "parser_name": "arXiv", "source_filename": "a.raw"},
            },
        ]

        summary = perf_metrics.aggregate_ads_events(events, started_at=1.0, ended_at=6.0, expected_files=1)
        self.assertEqual(summary["counts"]["records_submitted"], 2)
        self.assertEqual(summary["counts"]["records_processed"], 2)
        self.assertIn(".raw", summary["source_type_breakdown"])
        self.assertEqual(summary["source_type_breakdown"][".raw"]["record_count"], 2)
        self.assertIn("raw_arxiv", summary["raw_subfamily_breakdown"])
        self.assertEqual(summary["raw_subfamily_breakdown"]["raw_arxiv"]["record_count"], 2)
        self.assertEqual(summary["per_record_metrics_ms"]["parse_stage"]["p95"], 10.0)
        self.assertEqual(summary["status"], "complete")

    def test_render_markdown_and_write_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            summary = {
                "status": "complete",
                "counts": {"files_processed": 1, "records_processed": 2},
                "throughput": {"overall_records_per_minute": 120.0, "load_adjusted_records_per_minute": 140.0},
                "duration_s": {"wall_clock": 1.0},
                "per_record_metrics_ms": {
                    "wall_time": {"count": 2, "mean": 10.0, "p50": 10.0, "p95": 11.0, "p99": 11.0},
                    "parse_stage": {"count": 2, "mean": 3.0, "p50": 3.0, "p95": 4.0, "p99": 4.0},
                    "resolver_stage": {"count": 2, "mean": 4.0, "p50": 4.0, "p95": 5.0, "p99": 5.0},
                    "db_stage": {"count": 2, "mean": 2.0, "p50": 2.0, "p95": 3.0, "p99": 3.0},
                },
                "source_type_breakdown": {},
                "parser_breakdown": {},
                "system_load": {
                    "collection": {
                        "enabled": True,
                        "sample_count": 2,
                        "sample_interval_s": 1.0,
                        "platform": "linux",
                        "cpu_count": 4,
                        "memory_probe": "linux_meminfo",
                    },
                    "summary": {
                        "loadavg_1m": {"mean": 2.0, "min": 1.0, "max": 3.0, "p50": 2.0, "p95": 2.9},
                        "loadavg_5m": {"mean": 1.5, "min": 1.0, "max": 2.0, "p50": 1.5, "p95": 1.95},
                        "loadavg_15m": {"mean": 1.0, "min": 0.8, "max": 1.2, "p50": 1.0, "p95": 1.18},
                        "normalized_load_1m": {"mean": 0.5, "min": 0.25, "max": 0.75, "p50": 0.5, "p95": 0.72},
                        "memory_total_bytes": {"mean": 1000.0, "min": 1000.0, "max": 1000.0, "p50": 1000.0, "p95": 1000.0},
                        "memory_available_bytes": {"mean": 250.0, "min": 200.0, "max": 300.0, "p50": 250.0, "p95": 295.0},
                        "memory_used_bytes": {"mean": 750.0, "min": 700.0, "max": 800.0, "p50": 750.0, "p95": 795.0},
                        "memory_available_ratio": {"mean": 0.25, "min": 0.2, "max": 0.3, "p50": 0.25, "p95": 0.295},
                        "memory_used_ratio": {"mean": 0.75, "min": 0.7, "max": 0.8, "p50": 0.75, "p95": 0.795},
                    },
                },
                "run_metadata": {"mode": "mock"},
            }
            json_path = os.path.join(tmpdir, "summary.json")
            md_path = os.path.join(tmpdir, "summary.md")
            perf_metrics.write_json(json_path, summary)
            perf_metrics.render_markdown(summary, md_path)
            csv_path = os.path.join(tmpdir, "summary.source_types.csv")
            perf_metrics.write_source_type_csv(summary, csv_path)
            self.assertTrue(os.path.exists(json_path))
            self.assertTrue(os.path.exists(md_path))
            self.assertTrue(os.path.exists(csv_path))
            with open(json_path, "r") as handle:
                self.assertEqual(json.loads(handle.read())["status"], "complete")
            with open(md_path, "r") as handle:
                rendered = handle.read()
                self.assertIn("ADS Reference Benchmark Report", rendered)
                self.assertIn("Mean Raw Load", rendered)
                self.assertIn("Memory used", rendered)

    def test_aggregate_system_samples_includes_raw_host_usage(self):
        samples = [
            {
                "platform": "linux",
                "cpu_count": 4,
                "memory_probe": "linux_meminfo",
                "loadavg_1m": 2.0,
                "loadavg_5m": 1.5,
                "loadavg_15m": 1.0,
                "normalized_load_1m": 0.5,
                "normalized_load_5m": 0.375,
                "normalized_load_15m": 0.25,
                "memory_total_bytes": 1000.0,
                "memory_available_bytes": 300.0,
                "memory_used_bytes": 700.0,
                "memory_available_ratio": 0.3,
                "memory_used_ratio": 0.7,
            },
            {
                "platform": "linux",
                "cpu_count": 4,
                "memory_probe": "linux_meminfo",
                "loadavg_1m": 4.0,
                "loadavg_5m": 3.0,
                "loadavg_15m": 2.0,
                "normalized_load_1m": 1.0,
                "normalized_load_5m": 0.75,
                "normalized_load_15m": 0.5,
                "memory_total_bytes": 1000.0,
                "memory_available_bytes": 200.0,
                "memory_used_bytes": 800.0,
                "memory_available_ratio": 0.2,
                "memory_used_ratio": 0.8,
            },
        ]

        result = perf_metrics.aggregate_system_samples(samples, enabled=True, sample_interval_s=2.0)
        self.assertEqual(result["summary"]["loadavg_1m"]["mean"], 3.0)
        self.assertEqual(result["summary"]["memory_available_bytes"]["min"], 200.0)
        self.assertEqual(result["summary"]["memory_used_ratio"]["max"], 0.8)


if __name__ == "__main__":
    unittest.main()
