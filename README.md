# Tampico_Semanal_Reportes

Repo base para descargar datos semanales de redes y medios en Tampico, y dejar la salida lista para procesos posteriores de NLP, homologación y análisis.

## Alcance

Este repo parte de `Datos_Radar`, pero quedó limpiado para uso operativo:

- Sin datos históricos descargados.
- Sin la carpeta `Datos_Redes_Sets_Enteros_55_Semanas`.
- Con las carpetas `Facebook`, `Medios`, `Twitter` y `Youtube` vacías, dejando solo la arquitectura.
- Con scripts ajustados para escribir dentro de este mismo repo.
- Sin secretos embebidos en código.

## Estructura

```text
Tampico_Semanal_Reportes/
├── Datos/
├── Facebook/
├── Medios/
├── Scripts/
├── Twitter/
├── Youtube/
└── state/
```

## Scripts incluidos

- `Scripts/00_orquestador_general.py`
- `Scripts/01_youtube_tampico.py`
- `Scripts/02_twitter_tampico.py`
- `Scripts/03_medios_tampico.py`
- `Scripts/04_facebook_comentarios_tampico.py`
- `Scripts/05_facebook_posts_tampico.py`

## Variables de entorno

Define las credenciales antes de correr los extractores:

```bash
export YOUTUBE_API_KEY=""
export APIFY_TOKEN=""
```

Opcionales para YouTube:

```bash
export YT_PROXY_HTTP=""
export YT_PROXY_HTTPS=""
```

## Instalación rápida

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

## Uso recomendado

```bash
python Scripts/00_orquestador_general.py
```

El detalle script por script de argumentos y prompts quedó en `ORQUESTADOR_ARGUMENTOS.md`.

## Notas operativas

- `state/x_state.example.json` es solo una referencia. Debes crear `state/x_state.json` con un `storage_state` válido para correr el extractor de X/Twitter.
- Las salidas semanales se generan dentro de `Facebook/`, `Medios/`, `Twitter/` y `Youtube/`, usando carpetas como `2026-04-06_Youtube` o `2026-04-06_Facebook`.
- `.gitignore` está configurado para no versionar descargas, cachés ni credenciales futuras.
