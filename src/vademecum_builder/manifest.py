from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Manifest:
    version: str
    mode: str
    file: str
    deleted_file: str | None
    sha256: str
    size: int
    generated_at: str
    base_version: str | None
    source_versions: dict[str, str]
    stats: dict[str, int]

    def to_raw(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "version": self.version,
            "mode": self.mode,
            "file": self.file,
            "sha256": self.sha256,
            "size": self.size,
            "generated_at": self.generated_at,
            "base_version": self.base_version,
            "source_versions": self.source_versions,
            "stats": self.stats,
        }
        if self.deleted_file:
            payload["deleted_file"] = self.deleted_file
        return payload


def write_manifest(path: Path, manifest: Manifest) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest.to_raw(), ensure_ascii=False, indent=2), encoding="utf-8")
