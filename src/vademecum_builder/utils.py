from __future__ import annotations

import gzip
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import TextIO


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def iso_utc_now_z() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def validate_iso_date(value: str) -> None:
    datetime.strptime(value, "%Y-%m-%d")


def iso_to_ddmmyyyy(value: str) -> str:
    dt = datetime.strptime(value, "%Y-%m-%d")
    return dt.strftime("%d/%m/%Y")


def normalize_cn(value: object) -> str | None:
    if value is None:
        return None
    text = "".join(ch for ch in str(value).strip() if ch.isdigit())
    if not text:
        return None
    return text.zfill(6)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_size(path: Path) -> int:
    return path.stat().st_size


def open_gzip_jsonl_writer(path: Path) -> TextIO:
    path.parent.mkdir(parents=True, exist_ok=True)
    return gzip.open(path, "wt", encoding="utf-8", newline="\n")


def open_gzip_text_writer(path: Path) -> TextIO:
    path.parent.mkdir(parents=True, exist_ok=True)
    return gzip.open(path, "wt", encoding="utf-8", newline="\n")


def dumps_json_line(data: dict[str, object]) -> str:
    return json.dumps(data, ensure_ascii=False, separators=(",", ":")) + "\n"
