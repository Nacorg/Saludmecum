#!/usr/bin/env bash
set -euo pipefail

export MODE=full
python -m vademecum_builder --mode full "$@"
