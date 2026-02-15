from __future__ import annotations

from vademecum_builder.nomenclator_loader import load_nomenclator


def test_load_nomenclator_csv_without_headers(tmp_path) -> None:
    source = tmp_path / "nomenclator.csv"
    source.write_text("123456;si;12,34;oral;Laboratorio X\n", encoding="utf-8")

    data = load_nomenclator(url=None, path=source, out_dir=tmp_path / "out", timeout=5)
    assert data is not None
    assert "123456" in data.by_cn
