#!/usr/bin/env bash
set -euo pipefail

export MODE=incremental
python -m vademecum_builder --mode incremental "$@"
