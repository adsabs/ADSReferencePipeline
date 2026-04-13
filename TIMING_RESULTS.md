# ADSReferencePipeline Timing Results Guide

This document explains how to read the Markdown timing report produced by the ADSReferencePipeline benchmark tooling. It is written as a general reference for any future benchmark run, not for one specific run.

The benchmark report is intended to answer three questions:

- How fast did the pipeline process references overall?
- Which source formats, parsers, or raw subfamilies were slower or faster?
- What was happening on the host while the benchmark was running?

## Artifact Files

An attached benchmark run normally produces several artifacts.

The most useful files are:

- `attached_benchmark_manifest.json`: Host-side wrapper manifest. This is the canonical attached-run record. It includes host context, container exit status, and paths to in-container benchmark artifacts.
- `attached_benchmark_manifest.md`: Short host-side summary intended for quick inspection.
- `attached_*.json`: Full in-container benchmark summary. This contains all detailed timing data used to render the Markdown report.
- `attached_*.md`: Full timing report for humans. This is the primary report described by this guide.
- `attached_*.source_types.csv`: Flat source-type table for spreadsheet comparison across runs.
- `perf_events.jsonl`: Raw event stream emitted during the run. This is useful for debugging aggregation logic or building custom analyses.

## Run Config

The `Run Config` section records the benchmark inputs and execution options.

Common fields include:

- `run_id`: Unique benchmark run identifier.
- `context_id`: Unique metrics context used to isolate events for this run.
- `input_path`: File or directory benchmarked inside the container.
- `extensions`: File patterns used to select benchmark inputs.
- `max_files`: Optional cap on the number of files processed.
- `mode`: Benchmark mode. `mock` replaces resolver calls with deterministic in-process responses; `real` uses the configured resolver service.
- `group_by`: Preferred grouping mode requested by the benchmark command.
- `git_commit`: Git commit detected inside the benchmark environment, when available.
- `timestamp_utc`: UTC timestamp for the benchmark run.
- `system_load_enabled`: Whether in-container system-load sampling was enabled.
- `system_sample_interval_s`: Sampling interval for system-load metrics.
- `warmup`: Whether a warmup pass was attempted before the measured run.

Use this section first when comparing runs. Differences in input set, mode, file cap, or warmup can make timing numbers non-comparable.

## Top-Line Results

The `Top-Line Results` section summarizes the overall run.

Important fields:

- `Status`: `complete` means the benchmark completed its expected processing path. `incomplete` means the run should be inspected before comparing performance.
- `Files Processed`: Number of source files that emitted timing events.
- `Records Processed`: Number of reference records observed in the per-record timing events.
- `Throughput`: Overall processed records per minute, based on wall-clock benchmark duration.
- `Load-Adjusted Throughput`: Throughput multiplied by a host-load adjustment factor. This is useful as a rough comparison signal, not a replacement for raw throughput.
- `Wall Duration`: Total measured benchmark wall-clock duration in seconds.

Throughput is record-based, not file-based. A single input file can produce many records.

## Per-Record Metrics

The `Per-Record Metrics (ms)` table reports timing distributions in milliseconds per processed record.

Columns:

- `Metric`: Timing category.
- `Count`: Number of timing samples in that category.
- `p50 / record`: Median per-record latency.
- `p95 / record`: 95th percentile per-record latency.
- `p99 / record`: 99th percentile per-record latency.
- `Mean / record`: Average per-record latency.

Common metrics:

- `wall_time`: End-to-end time for one record through the measured per-record task path.
- `parse_stage`: Per-record normalized parsing time. File-level parse time is divided by records produced by that file or block.
- `resolver_stage`: Time spent in resolver call handling for one record. In `mock` mode this should be very small because no real network resolver call is made.
- `db_stage`: Per-record normalized database update or insertion time.

These are per-record values. They are not per file and not totals for the run.

## Stage Latency

The `Stage Latency (ms)` section lists lower-level benchmark stages emitted by instrumentation points.

Unless otherwise noted, stage timings are normalized to per-record milliseconds when a stage handles multiple records.

Common stages:

- `file_wall`: Time spent processing one source file. This stage may appear in broader latency summaries but is not the same as per-record wall time.
- `parser_lookup`: Time spent selecting the parser for a source filename.
- `parser_init`: Time spent constructing the parser object.
- `parse_dispatch`: Time spent parsing and dispatching references from a source file.
- `pre_resolved_db`: Time spent creating initial database rows before resolution.
- `queue_references`: Time spent iterating through references and invoking the task path.
- `record_wall`: End-to-end time for one record in the task path.
- `resolver_http`: Time spent resolving one record. In `mock` mode this measures the mock resolver path; in `real` mode it includes the configured resolver request.
- `post_resolved_db`: Time spent updating resolved-reference rows after resolution.

Use this section to identify which pipeline phase is driving latency.

## Slowest Source Types

The `Slowest Source Types` table ranks source types by `Wall p95 / record`.

Columns:

- `Source Type`: Normalized input type, such as `.raw`, `.jats.xml`, `.iop.xml`, `.html`, or `.ocr.txt`.
- `Files`: Number of source files in that source-type group.
- `Records`: Number of processed references in that source-type group.
- `Wall p95 / record`: 95th percentile per-record wall time for that source type.
- `Wall Mean / record`: Mean per-record wall time for that source type.
- `Throughput (records/min)`: Estimated throughput for records in that source-type group.

This table is useful for quickly identifying the source formats with the slowest tail latency.

## Source Types

The `Source Types` table provides a full source-type breakdown.

All mean values are per-record milliseconds.

Columns:

- `Source Type`: Normalized file/source type.
- `Files`: Number of input source files in the group.
- `Records`: Number of extracted and processed references in the group.
- `Wall Mean / record`: Mean end-to-end per-record wall time.
- `Parse Mean / record`: Mean per-record normalized parsing time.
- `Resolver Mean / record`: Mean per-record resolver time.
- `DB Mean / record`: Mean per-record database time.
- `Throughput (records/min)`: Estimated records-per-minute throughput for that source type.

Important interpretation:

- `Files` and `Records` are different units.
- `Wall Mean / record` is not the time to process one file.
- A file type with high total runtime may still have a low per-record wall mean if it produced many references.
- A source type with only one file or very few records may have unstable percentile estimates.

## Parsers

The `Parsers` section groups records by parser name.

Columns:

- `Parser`: Parser selected by ADSReferencePipeline.
- `Files`: Number of source files handled by that parser.
- `Records`: Number of processed references handled by that parser.
- `Wall Mean / record`: Mean per-record wall time for records handled by that parser.

Use parser breakdowns when multiple source types map to related parser behavior, or when a single source type can be processed differently based on journal/path matching.

## Raw Subfamilies

The `.raw` source type includes several materially different input families. The `Raw Subfamilies` section preserves these distinctions without relying on noisy filename artifacts such as `.z.raw`.

Common raw subfamilies:

- `raw_arxiv`: arXiv-style raw input.
- `raw_adstxt`: generic ADS text raw input.
- `raw_ref_raw`: `*.ref.raw` input.
- `raw_pasj_html`: PASJ HTML-derived raw input.
- `raw_pasp_html`: PASP HTML-derived raw input.
- `raw_jlven_html`: JLVEn HTML-derived raw input.
- `raw_aas`: AAS raw fixture family.
- `raw_icarus`: Icarus raw fixture family.
- `raw_pthph`: PThPh raw input.
- `raw_pthps`: PThPS raw input.
- `raw_other`: Fallback for raw inputs that do not match a known subfamily.

Columns:

- `Raw Subfamily`: Normalized raw subtype.
- `Files`: Number of raw source files in that subfamily.
- `Records`: Number of processed references in that subfamily.
- `Wall Mean / record`: Mean per-record wall time.
- `Wall p95 / record`: 95th percentile per-record wall time.
- `Throughput (records/min)`: Estimated records-per-minute throughput.

Use this section when `.raw` looks unusually fast or slow. The overall `.raw` source-type row can hide meaningful differences between raw formats.

## System Load

The `System Load` section describes the benchmark host while the run was executing.

Collection fields:

- `Enabled`: Whether system sampling was enabled.
- `Sample Count`: Number of samples collected.
- `Sample Interval`: Requested seconds between samples.
- `Platform`: Operating system reported by Python.
- `CPU Count`: CPU count visible to the benchmark process.
- `Memory Probe`: Method used to collect memory data, such as `linux_meminfo` or `macos_vm_stat`.

Raw CPU load fields:

- `Mean Raw Load (1m / 5m / 15m)`: Mean host load average across collected samples.
- `Peak Raw Load (1m)`: Maximum observed 1-minute load average.

Normalized CPU load fields:

- `Mean Normalized Load (1m)`: Mean 1-minute load divided by CPU count.
- `Peak Normalized Load (1m)`: Maximum normalized 1-minute load.

Normalized load interpretation:

- Around `0.0` to `1.0`: Load is at or below visible CPU capacity.
- Greater than `1.0`: More runnable work than visible CPU capacity.
- This is a coarse indicator. It is not the same as CPU utilization percentage.

Memory fields:

- `Mean Memory Total`: Mean total memory visible to the sampler.
- `Mean Memory Available`: Mean available memory.
- `Minimum Memory Available`: Lowest observed available memory.
- `Mean Memory Used`: Mean estimated used memory.
- `Peak Memory Used`: Highest estimated used memory.
- `Mean Memory Available Ratio`: Available memory divided by total memory.
- `Minimum Memory Available Ratio`: Lowest observed available-memory ratio.
- `Mean Memory Used Ratio`: Used memory divided by total memory.
- `Peak Memory Used Ratio`: Highest observed used-memory ratio.

The `System Load Samples` table gives mean, min, max, p50, and p95 for raw load, normalized load, and memory metrics.

Use this section when comparing runs. A slower run under higher load or lower available memory may reflect host contention rather than a pipeline regression.

## CSV Output

The `attached_*.source_types.csv` file contains a flat source-type summary for comparison across benchmark runs.

Typical columns:

- `source_type`
- `file_count`
- `record_count`
- `wall_mean_ms`
- `wall_p95_ms`
- `parse_mean_ms`
- `resolver_mean_ms`
- `db_mean_ms`
- `throughput_records_per_minute`

This CSV is best for trend tracking or spreadsheet comparison. It does not include every nested metric from the JSON summary.

## JSON Output

The full `attached_*.json` benchmark summary is the most complete machine-readable output.

Important top-level keys:

- `counts`: File and record counts.
- `duration_s`: Overall wall-clock duration.
- `throughput`: Raw and load-adjusted throughput.
- `latency_ms`: Stage latency statistics.
- `task_timing_ms`: Task-level timing statistics.
- `app_timing_ms`: Application-level timing statistics.
- `per_record_metrics_ms`: Main per-record timing summary.
- `source_type_breakdown`: Metrics grouped by normalized source type.
- `parser_breakdown`: Metrics grouped by parser.
- `raw_subfamily_breakdown`: Metrics grouped by raw subfamily.
- `system_load`: Raw samples and aggregated host-load/memory statistics.
- `run_metadata`: Configuration and identifying metadata.
- `errors`: Stage-level error counts.

Use JSON for automated analysis and Markdown for human review.

## Interpreting Mock vs Real Mode

Benchmark mode strongly affects interpretation.

In `mock` mode:

- Resolver calls are replaced by deterministic in-process responses.
- `Resolver Mean / record` should be near zero.
- Results are useful for measuring parser, database, task orchestration, and benchmark overhead.
- Results do not measure real resolver network/service latency.

In `real` mode:

- Resolver calls use the configured resolver service.
- `Resolver Mean / record` includes real service request behavior.
- Results are closer to production end-to-end performance.
- Results may vary more due to network, service, and external load.

Do not compare `mock` and `real` runs as if they measure the same workload.

## Comparison Tips

For meaningful comparisons:

- Compare runs with the same `mode`.
- Compare runs with the same input path and extension selection.
- Check `Files Processed` and `Records Processed` before comparing throughput.
- Inspect `System Load` to identify host contention.
- Prefer p95 latency for tail-performance regressions.
- Use source-type and raw-subfamily breakdowns to avoid hiding format-specific behavior.
- Treat very small groups with caution; percentile values are less reliable when record counts are low.

## Common Misreadings

- `Wall Mean / record` is not per file.
- `Throughput` is records per minute, not files per minute.
- `Parse Mean / record` is often normalized from file-level parse timing.
- In `mock` mode, resolver timing is intentionally tiny.
- `Load-Adjusted Throughput` is a rough comparison aid, not a direct measurement.
- A source type with more files is not necessarily slower; compare record counts and per-record latency.
