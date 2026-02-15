from __future__ import annotations

import gzip
import json
from pathlib import Path

from vademecum_builder import build_full
from vademecum_builder.config import BuildMode, Settings


class _FakeCimaClient:
    def __init__(self, *args: object, **kwargs: object) -> None:
        pass

    def iter_medicamentos(self):
        yield {"nregistro": "1001"}

    def get_medicamento(self, nregistro: str) -> dict[str, object]:
        assert nregistro == "1001"
        return {
            "nombre": "Medicamento Test",
            "labtitular": "Lab Test",
            "atc": [{"codigo": "A01"}],
            "formaFarmaceutica": "Comprimido",
            "presentaciones": [
                {"cn": "12345", "viaAdministracion": "Oral"},
                {"codigoNacional": "678901"},
            ],
        }


def _read_gzip_jsonl(path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            rows.append(json.loads(line))
    return rows


def test_run_full_build_generates_files(tmp_path, monkeypatch) -> None:
    out_dir = tmp_path / "out"
    state_path = out_dir / "state.json"
    settings = Settings(
        mode=BuildMode.FULL,
        out_dir=out_dir,
        version="2026-02-15",
        nomenclator_url=None,
        nomenclator_path=None,
        http_timeout=5,
        http_max_retries=0,
        state_path=state_path,
        max_error_ids=10,
    )

    monkeypatch.setattr(build_full, "CimaClient", _FakeCimaClient)
    monkeypatch.setattr(build_full, "load_nomenclator", lambda **kwargs: None)

    code = build_full.run_full_build(settings)
    assert code == 0

    payload_file = out_dir / "vademecum_full.jsonl.gz"
    manifest_file = out_dir / "manifest.json"
    assert payload_file.exists()
    assert manifest_file.exists()
    assert state_path.exists()

    rows = _read_gzip_jsonl(payload_file)
    assert len(rows) == 2
    assert rows[0]["source"] == "CIMA"
    assert rows[0]["updated_at"] == "2026-02-15"
    assert rows[0]["cn"] == "012345"

    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    assert manifest["mode"] == "full"
    assert manifest["stats"]["presentaciones_emitidas"] == 2
