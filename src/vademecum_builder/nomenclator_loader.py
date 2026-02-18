from __future__ import annotations

import csv
import hashlib
import io
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from .utils import normalize_cn

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class NomenclatorEntry:
    financiado: bool | None
    precio: float | None
    via_administracion: str | None
    laboratorio: str | None


@dataclass(frozen=True)
class NomenclatorData:
    by_cn: dict[str, NomenclatorEntry]
    source_ref: str


def load_nomenclator(
    *,
    url: str | None,
    path: Path | None,
    out_dir: Path,
    timeout: int,
) -> NomenclatorData | None:
    source_path: Path | None = None
    source_ref = "none"

    if url:
        try:
            source_path = _download(url=url, out_dir=out_dir, timeout=timeout)
            source_ref = f"{url}#{_sha256_file(source_path)}"
        except Exception as exc:
            LOGGER.warning("Falló la descarga del nomenclátor (%s). Se continúa sin él.", exc)
            return None
    elif path:
        if not path.exists():
            LOGGER.warning("NOMENCLATOR_PATH no existe: %s", path)
            return None
        source_path = path
        source_ref = f"{path.name}#{_sha256_file(path)}"
    else:
        return None

    if source_path is None:
        return None

    ext = source_path.suffix.lower()
    try:
        if ext in {".csv", ".txt"}:
            data = _load_csv_like(source_path)
        elif ext in {".xls", ".xlsx"}:
            data = _load_excel(source_path)
        else:
            LOGGER.warning("Formato de nomenclátor no soportado: %s", source_path)
            return None
    except Exception as exc:
        LOGGER.warning("Falló la lectura del fichero de nomenclátor (%s). Se continúa sin él.", exc)
        return None

    LOGGER.info("Entradas de nomenclátor cargadas=%s desde %s", len(data), source_path)
    return NomenclatorData(by_cn=data, source_ref=source_ref)


def _load_csv_like(path: Path) -> dict[str, NomenclatorEntry]:
    content = path.read_text(encoding="utf-8-sig")
    sample = content[:2048]
    delimiter = _detect_delimiter(sample)
    try:
        has_header = csv.Sniffer().has_header(sample)
    except csv.Error:
        has_header = True

    if has_header:
        dict_reader = csv.DictReader(io.StringIO(content), delimiter=delimiter)
        mapped = _parse_rows(dict_reader)
        if mapped:
            return mapped

    plain_reader = csv.reader(io.StringIO(content), delimiter=delimiter)
    rows: list[dict[str, Any]] = []
    for raw in plain_reader:
        if not raw:
            continue
        rows.append({f"col_{idx}": value for idx, value in enumerate(raw)})
    return _parse_rows(rows)


def _load_excel(path: Path) -> dict[str, NomenclatorEntry]:
    try:
        import pandas as pd  # type: ignore
    except ImportError as exc:
        raise RuntimeError("pandas/openpyxl no instalados para soporte XLS/XLSX") from exc

    frame = pd.read_excel(path)
    rows = frame.to_dict(orient="records")
    return _parse_rows(rows)


def _parse_rows(rows: Any) -> dict[str, NomenclatorEntry]:
    mapped: dict[str, NomenclatorEntry] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue

        lowered = {str(k).strip().lower(): v for k, v in row.items()}
        cn = _coalesce(lowered, ["cn", "codigo_nacional", "c_n", "cod_nacional", "codigo nacional"])
        if cn is None:
            cn = _find_cn_in_values(lowered.values())
        cn_norm = normalize_cn(cn)
        if not cn_norm:
            continue

        financiado = _parse_bool(
            _coalesce(
                lowered,
                ["financiado", "financiacion", "financia", "financiado_sns"],
            )
        )
        precio = _parse_float(_coalesce(lowered, ["precio", "pvp", "precio_iva", "importe"]))
        via = _parse_str(_coalesce(lowered, ["via", "via_administracion", "v_a", "administracion"]))
        lab = _parse_str(
            _coalesce(lowered, ["laboratorio", "lab", "titular", "nombre_laboratorio"])
        )

        mapped[cn_norm] = NomenclatorEntry(
            financiado=financiado,
            precio=precio,
            via_administracion=via,
            laboratorio=lab,
        )
    return mapped


def _download(url: str, out_dir: Path, timeout: int) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / "nomenclator_source"
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()

    ext = _guess_ext(response.headers.get("Content-Type", ""), url)
    target = target.with_suffix(ext)
    target.write_bytes(response.content)
    return target


def _guess_ext(content_type: str, url: str) -> str:
    lower_ct = content_type.lower()
    lower_url = url.lower()
    if ".csv" in lower_url or "csv" in lower_ct:
        return ".csv"
    if ".xlsx" in lower_url:
        return ".xlsx"
    if ".xls" in lower_url:
        return ".xls"
    if ".txt" in lower_url or "text" in lower_ct:
        return ".txt"
    return ".csv"


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _coalesce(row: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return None


def _parse_bool(value: Any) -> bool | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "t", "si", "sí", "s", "y", "yes"}:
        return True
    if text in {"0", "false", "f", "no", "n"}:
        return False
    return None


def _parse_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace("€", "").replace(" ", "").replace(",", ".")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _detect_delimiter(sample: str) -> str:
    if not sample.strip():
        return ","
    candidates = [",", ";", "\t", "|"]
    lines = [line for line in sample.splitlines() if line.strip()]
    best_delimiter = ","
    best_score = (0, 0.0)

    for delimiter in candidates:
        if not lines:
            continue
        widths: list[int] = []
        for line in lines[:20]:
            row = next(csv.reader([line], delimiter=delimiter))
            widths.append(len(row))
        rows_with_splits = sum(1 for width in widths if width > 1)
        avg_width = sum(widths) / len(widths)
        score = (rows_with_splits, avg_width)
        if score > best_score:
            best_score = score
            best_delimiter = delimiter

    if best_score[0] > 0:
        return best_delimiter
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;|\t")
        return dialect.delimiter
    except csv.Error:
        return ","


def _find_cn_in_values(values: Any) -> str | None:
    for value in values:
        cn = normalize_cn(value)
        if cn:
            return cn
    return None
