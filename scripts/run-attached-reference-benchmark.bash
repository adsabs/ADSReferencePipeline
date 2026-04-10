#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR_REL="Reference/logs/benchmarks/attached_reference_benchmark_runs"
INPUT_PATH="/app/adsrefpipe/tests/unittests/stubdata"
EXTENSIONS="*.raw,*.xml,*.txt,*.html,*.tex,*.refs,*.pairs"
MODE="mock"
MAX_FILES=""
TIMEOUT="900"
READINESS_TIMEOUT="180"
RUNNER_PATH="/app/scripts/run-in-container-benchmark.bash"
TARGET_CONTAINER="reference_pipeline"
SYSTEM_SAMPLE_INTERVAL="1.0"
GROUP_BY="source_type"
DISABLE_SYSTEM_LOAD="false"

usage() {
  cat <<'USAGE'
Usage: ./run-attached-reference-benchmark.bash [options]

Options:
  --container NAME               Target reference container name
  --input-path PATH              Input file or directory inside the target container
  --extensions CSV               Comma-separated file patterns
  --max-files N                  Optional file cap
  --mode real|mock               Benchmark mode
  --timeout N                    Benchmark timeout in seconds
  --readiness-timeout N          Container readiness timeout in seconds
  --output-dir RELPATH           Host output directory relative to wrapper root
  --runner-path PATH             In-container runner path
  --system-sample-interval FLOAT Sampling interval in seconds
  --group-by source_type|parser|none
  --disable-system-load          Disable in-container system sampling
  --help                         Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --container|--target)
      TARGET_CONTAINER="$2"
      shift 2
      ;;
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
    --readiness-timeout)
      READINESS_TIMEOUT="$2"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR_REL="$2"
      shift 2
      ;;
    --runner-path)
      RUNNER_PATH="$2"
      shift 2
      ;;
    --system-sample-interval)
      SYSTEM_SAMPLE_INTERVAL="$2"
      shift 2
      ;;
    --group-by)
      GROUP_BY="$2"
      shift 2
      ;;
    --disable-system-load)
      DISABLE_SYSTEM_LOAD="true"
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

RUN_STAMP="$(date -u +"%Y%m%dT%H%M%SZ")"
OUTPUT_BASE_DIR="${SCRIPT_DIR}/${OUTPUT_DIR_REL}"
OUTPUT_DIR="${OUTPUT_BASE_DIR}/run_${RUN_STAMP}"
RUN_LOG_DIR="${OUTPUT_DIR}/run_logs"
MANIFEST_PATH="${OUTPUT_DIR}/attached_benchmark_manifest.json"
SUMMARY_PATH="${OUTPUT_DIR}/attached_benchmark_manifest.md"
WRAPPER_LOG="${OUTPUT_DIR}/attached_benchmark.log"
HOST_CONTEXT_PATH="${RUN_LOG_DIR}/host_context.json"
CONTAINER_STDOUT_PATH="${RUN_LOG_DIR}/container_runner.stdout.log"
mkdir -p "${OUTPUT_DIR}" "${RUN_LOG_DIR}"
: > "${WRAPPER_LOG}"

log() {
  local message="$1"
  local timestamp
  timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "[${timestamp}] ${message}" | tee -a "${WRAPPER_LOG}"
}

check_docker_access() {
  docker info >/dev/null 2>&1
}

check_target_container() {
  docker inspect --format '{{.State.Running}}' "${TARGET_CONTAINER}" 2>/dev/null | grep -q true
}

wait_for_container_readiness() {
  local deadline=$(( $(date +%s) + READINESS_TIMEOUT ))
  while [[ $(date +%s) -lt ${deadline} ]]; do
    if docker exec "${TARGET_CONTAINER}" python3 -c "import os; import sys; target=os.environ.get('INPUT_PATH', '${INPUT_PATH}'); sys.exit(0 if os.path.exists(target) else 1)" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  return 1
}

capture_host_context() {
  local output_path="$1"
  python3 - "${output_path}" <<'PY'
import json
import re
import subprocess
import sys
from datetime import datetime, timezone

output_path = sys.argv[1]

def run_command(args):
    try:
        completed = subprocess.run(args, capture_output=True, text=True, check=False)
        return {
            "returncode": completed.returncode,
            "stdout": completed.stdout.strip(),
            "stderr": completed.stderr.strip(),
        }
    except Exception as exc:
        return {
            "returncode": None,
            "stdout": "",
            "stderr": str(exc),
        }

def parse_load(stdout):
    match = re.search(r"load averages?: ([0-9.]+)[, ]+([0-9.]+)[, ]+([0-9.]+)", stdout)
    if not match:
        return None
    return {
        "load_1m": float(match.group(1)),
        "load_5m": float(match.group(2)),
        "load_15m": float(match.group(3)),
    }

def parse_memory(stdout):
    page_size = 4096
    page_match = re.search(r"page size of (\d+) bytes", stdout)
    if page_match:
        page_size = int(page_match.group(1))
    values = {}
    for key in ["Pages free", "Pages inactive", "Pages speculative"]:
        match = re.search(rf"{re.escape(key)}:\s+(\d+)\.", stdout)
        if match:
            values[key] = int(match.group(1))
    if not values:
        return None
    available_pages = sum(values.values())
    return {
        "page_size_bytes": page_size,
        "pages_free": values.get("Pages free"),
        "pages_inactive": values.get("Pages inactive"),
        "pages_speculative": values.get("Pages speculative"),
        "available_bytes_estimate": available_pages * page_size,
    }

payload = {
    "captured_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
}

uptime_result = run_command(["uptime"])
payload["cpu_load"] = {
    "raw": uptime_result["stdout"],
    "parse": parse_load(uptime_result["stdout"]),
}

vm_stat_result = run_command(["vm_stat"])
payload["free_memory"] = {
    "raw": vm_stat_result["stdout"],
    "parse": parse_memory(vm_stat_result["stdout"]),
}

docker_df_result = run_command(["docker", "system", "df"])
payload["docker_disk_usage"] = docker_df_result

with open(output_path, "w") as handle:
    json.dump(payload, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY
}

resolve_app_mount() {
  docker inspect --format '{{range .Mounts}}{{if eq .Destination "/app"}}{{println .Source}}{{end}}{{end}}' "${TARGET_CONTAINER}" 2>/dev/null | awk 'NF {print; exit}'
}

APP_HOST_DIR="$(resolve_app_mount || true)"
CONTAINER_OUTPUT_DIR="/app/logs/benchmarks/attached_reference_benchmark_runs"
CONTAINER_LABEL="attached_${RUN_STAMP}"
LOGS_HOST_DIR="${SCRIPT_DIR}/Reference/logs"

if ! check_docker_access; then
  log "Docker daemon is not reachable from this shell"
  exit 1
fi

if ! check_target_container; then
  log "Target container is not running: ${TARGET_CONTAINER}"
  exit 1
fi

if ! wait_for_container_readiness; then
  log "Target container did not become benchmark-ready within ${READINESS_TIMEOUT}s"
  exit 1
fi

capture_host_context "${HOST_CONTEXT_PATH}"

CONTAINER_COMMAND="${RUNNER_PATH} --input-path '${INPUT_PATH}' --extensions '${EXTENSIONS}' --mode '${MODE}' --timeout '${TIMEOUT}' --output-dir '${CONTAINER_OUTPUT_DIR}' --label '${CONTAINER_LABEL}' --run-stamp '${RUN_STAMP}' --system-sample-interval '${SYSTEM_SAMPLE_INTERVAL}' --group-by '${GROUP_BY}'"
if [[ -n "${MAX_FILES}" ]]; then
  CONTAINER_COMMAND+=" --max-files '${MAX_FILES}'"
fi
if [[ "${DISABLE_SYSTEM_LOAD}" == "true" ]]; then
  CONTAINER_COMMAND+=" --disable-system-load"
fi

log "Running in-container benchmark via ${TARGET_CONTAINER}"
if docker exec "${TARGET_CONTAINER}" bash -lc "${CONTAINER_COMMAND}" > "${CONTAINER_STDOUT_PATH}" 2>> "${WRAPPER_LOG}"; then
  CONTAINER_EXIT_CODE=0
else
  CONTAINER_EXIT_CODE=$?
fi

python3 - "${CONTAINER_STDOUT_PATH}" "${MANIFEST_PATH}" "${SUMMARY_PATH}" "${HOST_CONTEXT_PATH}" "${TARGET_CONTAINER}" "${APP_HOST_DIR}" "${LOGS_HOST_DIR}" "${CONTAINER_EXIT_CODE}" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

stdout_path = Path(sys.argv[1])
manifest_path = Path(sys.argv[2])
summary_path = Path(sys.argv[3])
host_context_path = Path(sys.argv[4])
target_container = sys.argv[5]
app_host_dir = sys.argv[6]
logs_host_dir = sys.argv[7]
container_exit_code = int(sys.argv[8])

stdout = stdout_path.read_text() if stdout_path.exists() else ""
result = None
if stdout.strip():
    try:
        candidate = json.loads(stdout.strip())
        if isinstance(candidate, dict):
            result = candidate
    except json.JSONDecodeError:
        decoder = json.JSONDecoder()
        for index, char in enumerate(stdout):
            if char != "{":
                continue
            try:
                candidate, _ = decoder.raw_decode(stdout[index:])
            except json.JSONDecodeError:
                continue
            if isinstance(candidate, dict):
                result = candidate

host_context = None
if host_context_path.exists():
    host_context = json.loads(host_context_path.read_text())

payload = {
    "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "target_container": target_container,
    "container_exit_code": container_exit_code,
    "app_host_dir": app_host_dir or None,
    "host_context": host_context,
    "result": result,
}

if result and app_host_dir:
    for key in ["artifact_json", "artifact_markdown", "artifact_source_type_csv", "stdout_log", "run_dir"]:
        value = result.get(key)
        if isinstance(value, str) and value.startswith("/app/logs/"):
            result[f"{key}_host"] = str(Path(logs_host_dir) / value[len('/app/logs/'):])
        elif isinstance(value, str) and value.startswith("/app/"):
            result[f"{key}_host"] = str(Path(app_host_dir) / value[len('/app/'):])

manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")

host = host_context or {}
cpu_1m = (((host.get("cpu_load") or {}).get("parse") or {}).get("load_1m"))
free_bytes = (((host.get("free_memory") or {}).get("parse") or {}).get("available_bytes_estimate"))
free_gb = round(free_bytes / (1024 ** 3), 2) if isinstance(free_bytes, (int, float)) else ""
lines = [
    "# Attached Reference Benchmark",
    "",
    f"- Target container: `{target_container}`",
    f"- Container exit code: `{container_exit_code}`",
    f"- Artifact JSON: `{(result or {}).get('artifact_json', '')}`",
    f"- Artifact JSON host path: `{(result or {}).get('artifact_json_host', '')}`",
    "",
    "| Status | Throughput | Load-Adj | Files | Records | Per-Record Wall ms | CPU 1m | Free GB |",
    "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    "| {status} | {throughput} | {load_adjusted} | {files} | {records} | {wall} | {cpu_1m} | {free_gb} |".format(
        status=(result or {}).get("status", ""),
        throughput=(result or {}).get("throughput", ""),
        load_adjusted=(result or {}).get("load_adjusted_throughput", ""),
        files=(result or {}).get("files_processed", ""),
        records=(result or {}).get("records_processed", ""),
        wall=(result or {}).get("per_record_wall_mean_ms", ""),
        cpu_1m=cpu_1m if cpu_1m is not None else "",
        free_gb=free_gb,
    ),
]
summary_path.write_text("\n".join(lines) + "\n")

print(json.dumps(payload))
PY

log "Attached benchmark manifest: ${MANIFEST_PATH}"
log "Attached benchmark summary: ${SUMMARY_PATH}"
