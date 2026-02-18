from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

from .utils import validate_iso_date


class BuildMode(str, Enum):
    FULL = "full"
    INCREMENTAL = "incremental"


@dataclass(frozen=True)
class Settings:
    mode: BuildMode
    out_dir: Path
    version: str
    nomenclator_url: str | None
    nomenclator_path: Path | None
    http_timeout: int
    http_max_retries: int
    state_path: Path
    max_error_ids: int

    cima_base_url: str = "https://cima.aemps.es/cima/rest"

    @staticmethod
    def from_sources(
        cli_mode: str | None,
        cli_version: str | None,
        cli_out_dir: str | None,
        cli_state_path: str | None,
    ) -> "Settings":
        mode_raw = (cli_mode or os.getenv("MODE") or BuildMode.FULL.value).strip().lower()
        if mode_raw not in {BuildMode.FULL.value, BuildMode.INCREMENTAL.value}:
            raise ValueError(f"MODE inválido: {mode_raw}")
        mode = BuildMode(mode_raw)

        out_dir = Path(cli_out_dir or os.getenv("OUT_DIR") or "./out").resolve()

        version = (cli_version or os.getenv("VERSION") or _today_utc_iso()).strip()
        validate_iso_date(version)

        nomenclator_url = os.getenv("NOMENCLATOR_URL") or None
        nomenclator_path_raw = os.getenv("NOMENCLATOR_PATH") or None
        nomenclator_path = Path(nomenclator_path_raw).resolve() if nomenclator_path_raw else None

        timeout = int(os.getenv("HTTP_TIMEOUT") or "60")
        retries = int(os.getenv("HTTP_MAX_RETRIES") or "5")
        max_error_ids = int(os.getenv("MAX_ERROR_IDS") or "2000")

        state_path = Path(
            cli_state_path or os.getenv("STATE_PATH") or out_dir / "state.json"
        ).resolve()

        if timeout <= 0:
            raise ValueError("HTTP_TIMEOUT debe ser > 0")
        if retries < 0:
            raise ValueError("HTTP_MAX_RETRIES debe ser >= 0")
        if max_error_ids <= 0:
            raise ValueError("MAX_ERROR_IDS debe ser > 0")

        return Settings(
            mode=mode,
            out_dir=out_dir,
            version=version,
            nomenclator_url=nomenclator_url,
            nomenclator_path=nomenclator_path,
            http_timeout=timeout,
            http_max_retries=retries,
            state_path=state_path,
            max_error_ids=max_error_ids,
        )


def _today_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")
