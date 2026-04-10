#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

INPUT_PATH="/app/adsrefpipe/tests/unittests/stubdata"
EXTENSIONS="*.raw,*.xml,*.txt,*.html,*.tex,*.refs,*.pairs"
MODE="mock"
MAX_FILES=""
TIMEOUT="900"
OUTPUT_DIR="/app/logs/benchmarks/container_benchmark_runs"
EVENTS_PATH="/app/logs/benchmarks/perf_events.jsonl"
LABEL=""
RUN_STAMP=""
SYSTEM_SAMPLE_INTERVAL="1.0"
DISABLE_SYSTEM_LOAD="false"
GROUP_BY="source_type"
WARMUP="true"

usage() {
  cat <<'EOF'
Usage: /app/scripts/run-in-container-benchmark.bash [options]

Options:
  --input-path PATH                  File or directory inside the container
  --extensions CSV                   Comma-separated file patterns
  --max-files N                      Optional file cap
  --mode real|mock                   Benchmark mode
  --timeout N                        Benchmark timeout in seconds
  --output-dir PATH                  Output directory inside the container
  --events-path PATH                 Perf events path inside the container
  --label TEXT                       Stable artifact label prefix
  --run-stamp TEXT                   Reuse an externally supplied run timestamp
  --system-sample-interval FLOAT     Host/system sample interval inside container
  --disable-system-load              Disable system load sampling
  --group-by source_type|parser|none Preferred grouping mode
  --no-warmup                        Disable warmup pass
  --help                             Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --input-path)
      INPUT_PATH="$2"
      shift 2
      ;;
    --extensions)
      EXTENSIONS="$2"
      shift 2
      ;;
    --max-files)
      MAX_FILES="$2"
      shift 2
      ;;
    --mode)
      MODE="$2"
      shift 2
      ;;
    --timeout)
      TIMEOUT="$2"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --events-path)
      EVENTS_PATH="$2"
      shift 2
      ;;
    --label)
      LABEL="$2"
      shift 2
      ;;
    --run-stamp)
      RUN_STAMP="$2"
      shift 2
      ;;
    --system-sample-interval)
      SYSTEM_SAMPLE_INTERVAL="$2"
      shift 2
      ;;
    --disable-system-load)
      DISABLE_SYSTEM_LOAD="true"
      shift
      ;;
    --group-by)
      GROUP_BY="$2"
      shift 2
      ;;
    --no-warmup)
      WARMUP="false"
      shift
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

RUN_STAMP="${RUN_STAMP:-$(date -u +"%Y%m%dT%H%M%SZ")}"
RUN_LABEL="${LABEL:-benchmark_${RUN_STAMP}}"
RUN_DIR="${OUTPUT_DIR}/run_${RUN_STAMP}"
RUN_LOG_DIR="${RUN_DIR}/run_logs"
STDOUT_PATH="${RUN_LOG_DIR}/${RUN_LABEL}.stdout.log"
RESULT_PATH="${RUN_LOG_DIR}/${RUN_LABEL}.result.json"
mkdir -p "${RUN_DIR}" "${RUN_LOG_DIR}"

if [[ ! -e "${INPUT_PATH}" ]]; then
  cat > "${RESULT_PATH}" <<EOF
{"status":"failed","error":"input path not found: ${INPUT_PATH}"}
EOF
  cat "${RESULT_PATH}"
  exit 1
fi

BENCHMARK_CMD=(
  python3 -m adsrefpipe.benchmark run
  --input-path "${INPUT_PATH}"
  --extensions "${EXTENSIONS}"
  --mode "${MODE}"
  --timeout "${TIMEOUT}"
  --output-dir "${RUN_DIR}"
  --events-path "${EVENTS_PATH}"
  --system-sample-interval "${SYSTEM_SAMPLE_INTERVAL}"
  --group-by "${GROUP_BY}"
)

if [[ -n "${MAX_FILES}" ]]; then
  BENCHMARK_CMD+=(--max-files "${MAX_FILES}")
fi
if [[ "${DISABLE_SYSTEM_LOAD}" == "true" ]]; then
  BENCHMARK_CMD+=(--disable-system-load)
fi
if [[ "${WARMUP}" == "false" ]]; then
  BENCHMARK_CMD+=(--no-warmup)
fi

(
  cd "${APP_DIR}"
  "${BENCHMARK_CMD[@]}"
) > "${STDOUT_PATH}" 2>&1 || BENCHMARK_EXIT_CODE=$?

BENCHMARK_EXIT_CODE="${BENCHMARK_EXIT_CODE:-0}"

python3 - "${STDOUT_PATH}" "${RESULT_PATH}" "${RUN_DIR}" "${RUN_LABEL}" "${BENCHMARK_EXIT_CODE}" <<'PY'
import json
import shutil
import sys
from pathlib import Path

stdout_path = Path(sys.argv[1])
result_path = Path(sys.argv[2])
run_dir = Path(sys.argv[3])
run_label = sys.argv[4]
benchmark_exit_code = int(sys.argv[5])

payload = {
    "status": "failed" if benchmark_exit_code else "complete",
    "benchmark_exit_code": benchmark_exit_code,
    "run_dir": str(run_dir),
    "stdout_log": str(stdout_path),
}

stdout = stdout_path.read_text() if stdout_path.exists() else ""
decoder = json.JSONDecoder()
benchmark_result = None
for index, char in enumerate(stdout):
    if char != "{":
        continue
    try:
        candidate, _ = decoder.raw_decode(stdout[index:])
    except json.JSONDecodeError:
        continue
    if isinstance(candidate, dict):
        benchmark_result = candidate

if benchmark_result is None:
    payload["status"] = "invalid" if benchmark_exit_code == 0 else payload["status"]
    payload["error"] = "failed to find final benchmark JSON object in stdout"
else:
    payload["status"] = benchmark_result.get("status", payload["status"])
    raw_json = Path(benchmark_result.get("json", ""))
    raw_md = Path(benchmark_result.get("markdown", ""))
    if raw_json.exists():
        summary = json.loads(raw_json.read_text())
        stable_json = run_dir / f"{run_label}.json"
        stable_md = run_dir / f"{run_label}.md"
        stable_csv = run_dir / f"{run_label}.source_types.csv"
        stable_json.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
        payload["artifact_json"] = str(stable_json)
        payload["throughput"] = ((summary.get("throughput") or {}).get("overall_records_per_minute"))
        payload["load_adjusted_throughput"] = ((summary.get("throughput") or {}).get("load_adjusted_records_per_minute"))
        payload["wall_duration_s"] = ((summary.get("duration_s") or {}).get("wall_clock"))
        payload["records_processed"] = ((summary.get("counts") or {}).get("records_processed"))
        payload["files_processed"] = ((summary.get("counts") or {}).get("files_processed"))
        payload["per_record_wall_mean_ms"] = ((((summary.get("per_record_metrics_ms") or {}).get("wall_time") or {}).get("mean")))
        payload["source_type_breakdown"] = summary.get("source_type_breakdown")
        payload["raw_subfamily_breakdown"] = summary.get("raw_subfamily_breakdown")
        if raw_md.exists():
            shutil.copy2(raw_md, stable_md)
            payload["artifact_markdown"] = str(stable_md)
        raw_csv = Path(benchmark_result.get("source_type_csv", ""))
        if raw_csv.exists():
            shutil.copy2(raw_csv, stable_csv)
            payload["artifact_source_type_csv"] = str(stable_csv)

result_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
print(json.dumps(payload))
PY
