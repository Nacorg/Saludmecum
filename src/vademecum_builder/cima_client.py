from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, cast

import requests
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class CimaChange:
    nregistro: str
    tipo_cambio: str
    cn: str | None = None


class CimaClient:
    def __init__(self, base_url: str, timeout: int = 60, max_retries: int = 5) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = _build_session(max_retries=max_retries)

    def iter_medicamentos(self) -> Iterator[dict[str, Any]]:
        page = 1
        while True:
            payload = self._get_json("/medicamentos", params={"pagina": page})
            items = _extract_list(payload)
            if not items:
                LOGGER.info("No hay más páginas de medicamentos tras la página=%s", page)
                break

            LOGGER.info("Página de medicamentos obtenida=%s elementos=%s", page, len(items))
            for item in items:
                if isinstance(item, dict):
                    yield item
            page += 1

    def get_medicamento(self, nregistro: str) -> dict[str, Any]:
        data = self._get_json("/medicamento", params={"nregistro": nregistro})
        return data if isinstance(data, dict) else {}

    def get_registro_cambios(self, fecha_ddmmyyyy: str) -> list[CimaChange]:
        payload = self._get_json("/registroCambios", params={"fecha": fecha_ddmmyyyy})
        rows = _extract_list(payload)
        changes: list[CimaChange] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            nregistro = str(row.get("nregistro") or row.get("nRegistro") or "").strip()
            tipo = str(row.get("tipoCambio") or row.get("tipo") or "").strip()
            if nregistro and tipo:
                cn = row.get("cn") or row.get("codigoNacional") or row.get("codigo_nacional")
                cn_norm = str(cn).strip() if cn is not None else None
                changes.append(CimaChange(nregistro=nregistro, tipo_cambio=tipo, cn=cn_norm))
        return changes

    def _get_json(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any] | list[Any]:
        url = f"{self.base_url}{path}"
        response = self.session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, (dict, list)):
            return cast(dict[str, Any] | list[Any], payload)
        return {}


def _build_session(max_retries: int) -> Session:
    retry = Retry(
        total=max_retries,
        connect=max_retries,
        read=max_retries,
        status=max_retries,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": "vademecum-builder/0.1"})
    return session


def _extract_list(payload: dict[str, Any] | list[Any]) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    for key in ("resultados", "result", "medicamentos", "items", "contenido", "data"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
    return []
