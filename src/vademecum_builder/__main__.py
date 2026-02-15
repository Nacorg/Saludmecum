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
        description="Build CIMA offline vademecum datasets (FULL or INCREMENTAL).",
    )
    parser.add_argument(
        "--mode",
        choices=[BuildMode.FULL.value, BuildMode.INCREMENTAL.value],
        default=None,
        help="Build mode. If omitted, MODE env var is used and defaults to full.",
    )
    parser.add_argument(
        "--version",
        default=None,
        help="Version date (YYYY-MM-DD). Defaults to UTC today.",
    )
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Output directory. Defaults to OUT_DIR or ./out.",
    )
    parser.add_argument(
        "--state-path",
        default=None,
        help="State path. Defaults to STATE_PATH or <out-dir>/state.json.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO",
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
        logging.getLogger(__name__).error("Invalid configuration: %s", exc)
        return 2

    if settings.mode is BuildMode.FULL:
        return run_full_build(settings)
    return run_incremental_build(settings)


if __name__ == "__main__":
    sys.exit(main())
