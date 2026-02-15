from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .nomenclator_loader import NomenclatorEntry
from .utils import normalize_cn


@dataclass(frozen=True)
class BuildStats:
    medicamentos_procesados: int = 0
    presentaciones_emitidas: int = 0
    presentaciones_eliminadas: int = 0
    errores: int = 0


def map_presentaciones_from_medicamento(payload: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("presentaciones", "items", "resultados"):
        value = payload.get(key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]
    return []


def record_from_cima(
    *,
    nregistro: str,
    med_payload: dict[str, Any],
    presentacion: dict[str, Any],
    updated_at: str,
    nomenclator: NomenclatorEntry | None,
) -> dict[str, Any] | None:
    cn = normalize_cn(
        presentacion.get("cn")
        or presentacion.get("codigoNacional")
        or presentacion.get("codigo_nacional")
        or presentacion.get("codigo")
    )
    if not cn:
        return None

    atc = _extract_atc(med_payload)
    nombre = _to_str(med_payload.get("nombre") or presentacion.get("nombre")) or ""
    lab = _to_str(
        med_payload.get("labtitular")
        or med_payload.get("laboratorio")
        or presentacion.get("laboratorio")
    )
    forma = _to_str(med_payload.get("formaFarmaceutica") or med_payload.get("forma"))
    via = _to_str(
        presentacion.get("viaAdministracion")
        or med_payload.get("viaAdministracion")
        or med_payload.get("viasAdministracion")
    )

    ft = _to_str(med_payload.get("fichaTecnica") or med_payload.get("urlFichaTecnica"))
    pros = _to_str(med_payload.get("prospecto") or med_payload.get("urlProspecto"))

    financiado = None
    precio = None
    if nomenclator:
        financiado = nomenclator.financiado
        precio = nomenclator.precio
        if not via:
            via = nomenclator.via_administracion
        if not lab:
            lab = nomenclator.laboratorio

    return {
        "cn": cn,
        "nregistro": str(nregistro),
        "nombre": nombre,
        "lab": lab,
        "atc": atc,
        "forma": forma,
        "via": via,
        "docs": {
            "ft": ft,
            "pros": pros,
        },
        "financiado": financiado,
        "precio": precio,
        "updated_at": updated_at,
        "source": "CIMA",
    }


def _extract_atc(med_payload: dict[str, Any]) -> list[str]:
    raw = med_payload.get("atc") or med_payload.get("principiosActivos") or []
    values: list[str] = []
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                code = _to_str(item.get("codigo") or item.get("atc") or item.get("codATC"))
                if code:
                    values.append(code)
            else:
                text = _to_str(item)
                if text:
                    values.append(text)
    elif isinstance(raw, str):
        values.append(raw)
    return sorted(set(values))


def _to_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
