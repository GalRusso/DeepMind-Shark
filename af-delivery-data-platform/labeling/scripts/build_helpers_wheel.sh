#!/usr/bin/env bash
set -euo pipefail

# Resolve repo root (script is under scripts/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DIST_DIR="${ROOT_DIR}/dist"

mkdir -p "${DIST_DIR}"

echo "[1/3] Building main project wheel (uv build) ..."
( cd "${ROOT_DIR}" && uv build )

echo "[2/3] Building minimal helpers wheel ..."
TMP_DIR="$(mktemp -d)"
cleanup() { rm -rf "${TMP_DIR}"; }
trap cleanup EXIT

# Layout: <tmp>/llm_orchestrator/{__init__.py,helpers.py}
mkdir -p "${TMP_DIR}/llm_orchestrator"

# Copy source files
SRC_PKG_DIR="${ROOT_DIR}/src/data_transformation/llm_orchestrator"
# Ensure __init__.py exists; create empty if missing
if [[ -f "${SRC_PKG_DIR}/__init__.py" ]]; then
  cp "${SRC_PKG_DIR}/__init__.py" "${TMP_DIR}/llm_orchestrator/__init__.py"
else
  : > "${TMP_DIR}/llm_orchestrator/__init__.py"
fi
cp "${SRC_PKG_DIR}/helpers.py" "${TMP_DIR}/llm_orchestrator/helpers.py"

# Minimal pyproject for helpers-only wheel
cat > "${TMP_DIR}/pyproject.toml" <<'PYPROJECT'
[project]
name = "minimal-helpers"
version = "0.0.1"
requires-python = ">=3.11"
dependencies = ["langchain-core>=0.3.27"]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["llm_orchestrator"]
PYPROJECT

# Build helpers wheel
( cd "${TMP_DIR}" && uv build )

# Copy helpers wheel to repo dist
cp "${TMP_DIR}"/dist/*.whl "${DIST_DIR}/"

echo "[3/3] Done. Wheels in: ${DIST_DIR}"
ls -lh "${DIST_DIR}"