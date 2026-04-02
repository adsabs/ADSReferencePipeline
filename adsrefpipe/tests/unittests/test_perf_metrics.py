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
        self.assertEqual(perf_metrics.source_type_from_filename("foo.tex"), ".tex")

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
                "system_load": {"collection": {}, "summary": {}},
                "run_metadata": {"mode": "mock"},
            }
            json_path = os.path.join(tmpdir, "summary.json")
            md_path = os.path.join(tmpdir, "summary.md")
            perf_metrics.write_json(json_path, summary)
            perf_metrics.render_markdown(summary, md_path)
            self.assertTrue(os.path.exists(json_path))
            self.assertTrue(os.path.exists(md_path))
            with open(json_path, "r") as handle:
                self.assertEqual(json.loads(handle.read())["status"], "complete")
            with open(md_path, "r") as handle:
                self.assertIn("ADS Reference Benchmark Report", handle.read())


if __name__ == "__main__":
    unittest.main()
