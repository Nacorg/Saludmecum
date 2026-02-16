from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)


@dataclass
class StateData:
    last_success_version: str | None = None
    last_full_version: str | None = None
    last_incremental_date: str | None = None
    total_presentaciones_full: int = 0
    stats_last_run: dict[str, Any] | None = None
    failed_nregistro_last_run: list[str] | None = None

    @staticmethod
    def from_raw(raw: dict[str, Any]) -> "StateData":
        return StateData(
            last_success_version=raw.get("last_success_version"),
            last_full_version=raw.get("last_full_version"),
            last_incremental_date=raw.get("last_incremental_date"),
            total_presentaciones_full=int(raw.get("total_presentaciones_full") or 0),
            stats_last_run=(
                raw.get("stats_last_run")
                if isinstance(raw.get("stats_last_run"), dict)
                else {}
            ),
            failed_nregistro_last_run=list(raw.get("failed_nregistro_last_run") or []),
        )

    def to_raw(self) -> dict[str, Any]:
        return {
            "last_success_version": self.last_success_version,
            "last_full_version": self.last_full_version,
            "last_incremental_date": self.last_incremental_date,
            "total_presentaciones_full": self.total_presentaciones_full,
            "stats_last_run": self.stats_last_run or {},
            "failed_nregistro_last_run": self.failed_nregistro_last_run or [],
        }


def load_state(path: Path) -> StateData | None:
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        LOGGER.warning("Unable to read state file %s: %s", path, exc)
        return None
    if not isinstance(raw, dict):
        LOGGER.warning("State file %s is not a JSON object", path)
        return None
    return StateData.from_raw(raw)


def save_state(path: Path, state: StateData) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state.to_raw(), ensure_ascii=False, indent=2), encoding="utf-8")
