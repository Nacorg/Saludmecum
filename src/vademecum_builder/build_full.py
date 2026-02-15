from __future__ import annotations

import logging
from dataclasses import replace

from .cima_client import CimaClient
from .config import Settings
from .incremental import BuildStats, map_presentaciones_from_medicamento, record_from_cima
from .manifest import Manifest, write_manifest
from .nomenclator_loader import load_nomenclator
from .state import StateData, save_state
from .utils import (
    dumps_json_line,
    ensure_dir,
    file_size,
    iso_to_ddmmyyyy,
    iso_utc_now_z,
    normalize_cn,
    open_gzip_jsonl_writer,
    sha256_file,
)

LOGGER = logging.getLogger(__name__)


def run_full_build(settings: Settings) -> int:
    ensure_dir(settings.out_dir)

    client = CimaClient(
        base_url=settings.cima_base_url,
        timeout=settings.http_timeout,
        max_retries=settings.http_max_retries,
    )

    nomenclator_data = load_nomenclator(
        url=settings.nomenclator_url,
        path=settings.nomenclator_path,
        out_dir=settings.out_dir,
        timeout=settings.http_timeout,
    )
    nomenclator_map = nomenclator_data.by_cn if nomenclator_data else {}

    main_file = settings.out_dir / "vademecum_full.jsonl.gz"
    manifest_file = settings.out_dir / "manifest.json"

    stats = BuildStats()
    failed_ids: list[str] = []

    with open_gzip_jsonl_writer(main_file) as writer:
        for med_item in client.iter_medicamentos():
            nregistro = str(med_item.get("nregistro") or med_item.get("nRegistro") or "").strip()
            if not nregistro:
                continue

            try:
                med_payload = client.get_medicamento(nregistro)
            except Exception as exc:
                LOGGER.exception("Error requesting medicamento nregistro=%s: %s", nregistro, exc)
                stats = replace(stats, errores=stats.errores + 1)
                if len(failed_ids) < settings.max_error_ids:
                    failed_ids.append(nregistro)
                continue

            stats = replace(stats, medicamentos_procesados=stats.medicamentos_procesados + 1)
            presentaciones = map_presentaciones_from_medicamento(med_payload)
            for presentacion in presentaciones:
                cn_raw = (
                    presentacion.get("cn")
                    or presentacion.get("codigoNacional")
                    or presentacion.get("codigo_nacional")
                    or presentacion.get("codigo")
                )
                cn = normalize_cn(cn_raw)
                nom_entry = nomenclator_map.get(cn) if cn else None

                rec = record_from_cima(
                    nregistro=nregistro,
                    med_payload=med_payload,
                    presentacion=presentacion,
                    updated_at=settings.version,
                    nomenclator=nom_entry,
                )
                if not rec:
                    continue
                writer.write(dumps_json_line(rec))
                stats = replace(stats, presentaciones_emitidas=stats.presentaciones_emitidas + 1)

    sha = sha256_file(main_file)
    size = file_size(main_file)

    manifest = Manifest(
        version=settings.version,
        mode="full",
        file=main_file.name,
        deleted_file=None,
        sha256=sha,
        size=size,
        generated_at=iso_utc_now_z(),
        base_version=settings.version,
        source_versions={
            "cima_base": settings.cima_base_url,
            "nomenclator": nomenclator_data.source_ref if nomenclator_data else "none",
        },
        stats={
            "medicamentos_procesados": stats.medicamentos_procesados,
            "presentaciones_emitidas": stats.presentaciones_emitidas,
            "presentaciones_eliminadas": 0,
            "errores": stats.errores,
        },
    )
    write_manifest(manifest_file, manifest)

    state = StateData(
        last_success_version=settings.version,
        last_full_version=settings.version,
        last_incremental_date=iso_to_ddmmyyyy(settings.version),
        total_presentaciones_full=stats.presentaciones_emitidas,
        stats_last_run=manifest.stats,
        failed_nregistro_last_run=failed_ids,
    )
    save_state(settings.state_path, state)

    LOGGER.info(
        "FULL complete version=%s medicamentos=%s presentaciones=%s errores=%s",
        settings.version,
        stats.medicamentos_procesados,
        stats.presentaciones_emitidas,
        stats.errores,
    )
    return 0
