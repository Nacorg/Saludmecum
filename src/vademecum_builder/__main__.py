from __future__ import annotations

import argparse
import logging
import sys

from .build_full import run_full_build
from .build_incremental import run_incremental_build
from .config import BuildMode, Settings


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="vademecum_builder",
        description="Construye datasets offline de vademécum CIMA (FULL o INCREMENTAL).",
    )
    parser.add_argument(
        "--mode",
        choices=[BuildMode.FULL.value, BuildMode.INCREMENTAL.value],
        default=None,
        help="Modo de construcción. Si se omite, usa MODE y por defecto full.",
    )
    parser.add_argument(
        "--version",
        default=None,
        help="Fecha de versión (YYYY-MM-DD). Por defecto, hoy en UTC.",
    )
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Directorio de salida. Por defecto OUT_DIR o ./out.",
    )
    parser.add_argument(
        "--state-path",
        default=None,
        help="Ruta de state. Por defecto STATE_PATH o <out-dir>/state.json.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Nivel de logs (DEBUG, INFO, WARNING, ERROR). Por defecto: INFO",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    try:
        settings = Settings.from_sources(
            cli_mode=args.mode,
            cli_version=args.version,
            cli_out_dir=args.out_dir,
            cli_state_path=args.state_path,
        )
    except ValueError as exc:
        logging.getLogger(__name__).error("Configuración inválida: %s", exc)
        return 2

    if settings.mode is BuildMode.FULL:
        return run_full_build(settings)
    return run_incremental_build(settings)


if __name__ == "__main__":
    sys.exit(main())
