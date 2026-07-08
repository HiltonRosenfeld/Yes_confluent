#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

# use uv if found
if command -v uv >/dev/null 2>&1; then
    echo "Using uv..."
    uv venv --python 3.12
    uv pip install -r requirements.txt

# otherwise use pip
else
    echo "uv not found — falling back to pip..."
    python3 -m venv "$REPO_ROOT/.venv"
    "$REPO_ROOT/.venv/bin/pip" install --upgrade pip
    "$REPO_ROOT/.venv/bin/pip" install -r requirements.txt
fi

echo "Setup complete."
