#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export UV_PROJECT_ENVIRONMENT="${UV_PROJECT_ENVIRONMENT:-.env}"

uv sync --group dev
uv run pre-commit install

echo "Workspace ready at $ROOT_DIR using environment $UV_PROJECT_ENVIRONMENT"
