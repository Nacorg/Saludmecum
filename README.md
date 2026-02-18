# Constructor de Vademécum (CIMA + Nomenclátor)

Pipeline en Python 3.11+ para construir vademécum offline en `.jsonl.gz` con modo `full` e `incremental`, incluyendo `manifest.json` y `state.json`.

## Instalación

```bash
python -m pip install --upgrade pip
pip install -e .
```

Soporte opcional para Excel:

```bash
pip install -e .[excel]
```

Instalación reproducible (versiones fijadas):

```bash
pip install -r requirements.lock
pip install -e .
```

## Variables de entorno

- `MODE=full|incremental`
- `NOMENCLATOR_URL` (opcional)
- `NOMENCLATOR_PATH` (opcional)
- `OUT_DIR` (por defecto `./out`)
- `VERSION` (por defecto fecha UTC `YYYY-MM-DD`)
- `HTTP_TIMEOUT` (por defecto `60`)
- `HTTP_MAX_RETRIES` (por defecto `5`)
- `STATE_PATH` (por defecto `OUT_DIR/state.json`)
- `MAX_ERROR_IDS` (por defecto `2000`)

## Ejecución

Full:

```bash
python -m vademecum_builder --mode full
```

Incremental:

```bash
python -m vademecum_builder --mode incremental
```

Retroceso automático: si no existe `state.json` en incremental, ejecuta full.

## Salidas

Modo full:

- `out/vademecum_full.jsonl.gz`
- `out/manifest.json`
- `out/state.json`

Modo incremental:

- `out/vademecum_delta_YYYY-MM-DD.jsonl.gz`
- `out/deleted_YYYY-MM-DD.txt.gz`
- `out/manifest.json`
- `out/state.json`

## Estrategia Android (Room)

1. Descargar `manifest.json`.
2. Comparar `version`/`sha256` con lo aplicado localmente.
3. Si `mode=full`: reemplazar índice local con `vademecum_full.jsonl.gz`.
4. Si `mode=incremental`: aplicar upserts del `delta` por `cn`, y luego borrar CN listados en `deleted`.
5. Persistir `version` aplicada para evitar descargas redundantes.

## Cron semanal

Ejemplo Linux (`03:00 UTC` domingos):

```cron
0 3 * * 0 cd /ruta/repo && /usr/bin/python -m vademecum_builder --mode incremental >> /var/log/vademecum.log 2>&1
```

## Depuración

- Subir nivel de log: `python -m vademecum_builder --mode full --log-level DEBUG`
- Verificar estado: `cat out/state.json`
- Validar integridad: comparar `sha256` en `manifest.json` con hash local del `.gz`.

## Calidad y pruebas

```bash
pip install -r requirements-dev.lock
python -m ruff check src tests
python -m mypy src
python -m pytest -q
```

## GitHub Actions (semanal + publicación)

Workflow incluido en `.github/workflows/vademecum.yml`:

- Ejecuta incremental cada semana (`schedule`) o manual (`workflow_dispatch`).
- Ejecuta full mensual automáticamente (día 1) para refresco de base.
- Recupera `state.json` previo de la publicación `latest` si existe.
- Genera artefactos y actualiza la publicación `latest` con `manifest.json`, `state.json` y `*.gz`.
