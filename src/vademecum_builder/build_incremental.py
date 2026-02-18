from __future__ import annotations

import logging
from dataclasses import replace

from .build_full import run_full_build
from .cima_client import CimaClient
from .config import Settings
from .incremental import BuildStats, map_presentaciones_from_medicamento, record_from_cima
from .manifest import Manifest, write_manifest
from .nomenclator_loader import load_nomenclator
from .state import StateData, load_state, save_state
from .utils import (
    dumps_json_line,
    ensure_dir,
    file_size,
    iso_to_ddmmyyyy,
    iso_utc_now_z,
    normalize_cn,
    open_gzip_jsonl_writer,
    open_gzip_text_writer,
    sha256_file,
)

LOGGER = logging.getLogger(__name__)


def run_incremental_build(settings: Settings) -> int:
    ensure_dir(settings.out_dir)

    prior_state = load_state(settings.state_path)
    if prior_state is None or not prior_state.last_incremental_date:
        LOGGER.warning("state.json ausente o inválido. Se hará fallback a FULL.")
        return run_full_build(settings)

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

    delta_file = settings.out_dir / f"vademecum_delta_{settings.version}.jsonl.gz"
    deleted_file = settings.out_dir / f"deleted_{settings.version}.txt.gz"
    manifest_file = settings.out_dir / "manifest.json"

    stats = BuildStats()
    failed_ids: list[str] = []

    changes = client.get_registro_cambios(prior_state.last_incremental_date)
    LOGGER.info(
        "registroCambios fecha=%s rows=%s",
        prior_state.last_incremental_date,
        len(changes),
    )

    with (
        open_gzip_jsonl_writer(delta_file) as delta_writer,
        open_gzip_text_writer(deleted_file) as deleted_writer,
    ):
        for change in changes:
            tipo = change.tipo_cambio.strip().lower()
            nregistro = change.nregistro

            if "baja" in tipo:
                try:
                    med_payload = client.get_medicamento(nregistro)
                    presentaciones = map_presentaciones_from_medicamento(med_payload)
                    deleted_count = 0
                    for p in presentaciones:
                        cn = normalize_cn(
                            p.get("cn")
                            or p.get("codigoNacional")
                            or p.get("codigo_nacional")
                            or p.get("codigo")
                        )
                        if not cn:
                            continue
                        deleted_writer.write(f"{cn}\n")
                        deleted_count += 1
                    if deleted_count == 0 and change.cn:
                        cn_fallback = normalize_cn(change.cn)
                        if cn_fallback:
                            deleted_writer.write(f"{cn_fallback}\n")
                            deleted_count = 1
                    stats = replace(
                        stats,
                        presentaciones_eliminadas=(
                            stats.presentaciones_eliminadas + deleted_count
                        ),
                    )
                except Exception as exc:
                    cn_fallback = normalize_cn(change.cn)
                    if cn_fallback:
                        LOGGER.warning(
                            (
                                "Error procesando baja nregistro=%s. "
                                "Usando CN de respaldo desde registroCambios: %s"
                            ),
                            nregistro,
                            cn_fallback,
                        )
                        deleted_writer.write(f"{cn_fallback}\n")
                        stats = replace(
                            stats,
                            presentaciones_eliminadas=stats.presentaciones_eliminadas + 1,
                        )
                    else:
                        LOGGER.exception("Error procesando baja nregistro=%s: %s", nregistro, exc)
                        stats = replace(stats, errores=stats.errores + 1)
                    if len(failed_ids) < settings.max_error_ids:
                        failed_ids.append(nregistro)
                continue

            try:
                med_payload = client.get_medicamento(nregistro)
            except Exception as exc:
                LOGGER.exception("Error al solicitar medicamento nregistro=%s: %s", nregistro, exc)
                stats = replace(stats, errores=stats.errores + 1)
                if len(failed_ids) < settings.max_error_ids:
                    failed_ids.append(nregistro)
                continue

            stats = replace(stats, medicamentos_procesados=stats.medicamentos_procesados + 1)
            presentaciones = map_presentaciones_from_medicamento(med_payload)
            for p in presentaciones:
                cn_raw = (
                    p.get("cn")
                    or p.get("codigoNacional")
                    or p.get("codigo_nacional")
                    or p.get("codigo")
                )
                cn = normalize_cn(cn_raw)
                nom_entry = nomenclator_map.get(cn) if cn else None

                record = record_from_cima(
                    nregistro=nregistro,
                    med_payload=med_payload,
                    presentacion=p,
                    updated_at=settings.version,
                    nomenclator=nom_entry,
                )
                if not record:
                    continue
                delta_writer.write(dumps_json_line(record))
                stats = replace(stats, presentaciones_emitidas=stats.presentaciones_emitidas + 1)

    sha = sha256_file(delta_file)
    size = file_size(delta_file)

    manifest = Manifest(
        version=settings.version,
        mode="incremental",
        file=delta_file.name,
        deleted_file=deleted_file.name,
        sha256=sha,
        size=size,
        generated_at=iso_utc_now_z(),
        base_version=prior_state.last_full_version,
        source_versions={
            "cima_base": settings.cima_base_url,
            "nomenclator": nomenclator_data.source_ref if nomenclator_data else "none",
        },
        stats={
            "medicamentos_procesados": stats.medicamentos_procesados,
            "presentaciones_emitidas": stats.presentaciones_emitidas,
            "presentaciones_eliminadas": stats.presentaciones_eliminadas,
            "errores": stats.errores,
        },
    )
    write_manifest(manifest_file, manifest)

    new_state = StateData(
        last_success_version=settings.version,
        last_full_version=prior_state.last_full_version,
        last_incremental_date=iso_to_ddmmyyyy(settings.version),
        total_presentaciones_full=prior_state.total_presentaciones_full,
        stats_last_run=manifest.stats,
        failed_nregistro_last_run=failed_ids,
    )
    save_state(settings.state_path, new_state)

    LOGGER.info(
        "INCREMENTAL completado version=%s meds=%s emitidos=%s eliminados=%s errores=%s",
        settings.version,
        stats.medicamentos_procesados,
        stats.presentaciones_emitidas,
        stats.presentaciones_eliminadas,
        stats.errores,
    )
    return 0
