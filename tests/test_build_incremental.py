from __future__ import annotations

import gzip
import json

from vademecum_builder import build_incremental
from vademecum_builder.cima_client import CimaChange
from vademecum_builder.config import BuildMode, Settings
from vademecum_builder.state import StateData


class _FakeCimaClientBajaFallback:
    def __init__(self, *args: object, **kwargs: object) -> None:
        pass

    def get_registro_cambios(self, fecha_ddmmyyyy: str) -> list[CimaChange]:
        assert fecha_ddmmyyyy == "01/02/2026"
        return [CimaChange(nregistro="2001", tipo_cambio="Baja", cn="765432")]

    def get_medicamento(self, nregistro: str):
        raise RuntimeError(f"simulated error {nregistro}")


def test_incremental_baja_uses_cn_fallback(tmp_path, monkeypatch) -> None:
    out_dir = tmp_path / "out"
    state_path = out_dir / "state.json"
    settings = Settings(
        mode=BuildMode.INCREMENTAL,
        out_dir=out_dir,
        version="2026-02-15",
        nomenclator_url=None,
        nomenclator_path=None,
        http_timeout=5,
        http_max_retries=0,
        state_path=state_path,
        max_error_ids=10,
    )

    prior_state = StateData(
        last_success_version="2026-02-01",
        last_full_version="2026-01-01",
        last_incremental_date="01/02/2026",
        total_presentaciones_full=100,
        stats_last_run={},
        failed_nregistro_last_run=[],
    )

    monkeypatch.setattr(build_incremental, "CimaClient", _FakeCimaClientBajaFallback)
    monkeypatch.setattr(build_incremental, "load_nomenclator", lambda **kwargs: None)
    monkeypatch.setattr(build_incremental, "load_state", lambda _: prior_state)

    code = build_incremental.run_incremental_build(settings)
    assert code == 0

    deleted_path = out_dir / "deleted_2026-02-15.txt.gz"
    assert deleted_path.exists()
    with gzip.open(deleted_path, "rt", encoding="utf-8") as handle:
        deleted = [line.strip() for line in handle if line.strip()]
    assert deleted == ["765432"]

    manifest = json.loads((out_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["mode"] == "incremental"
    assert manifest["stats"]["presentaciones_eliminadas"] == 1
