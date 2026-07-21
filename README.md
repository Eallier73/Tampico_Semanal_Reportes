# Tampico_Semanal_Reportes

Repo base para descargar datos semanales de redes y medios en Tampico, y dejar la salida lista para procesos posteriores de NLP, homologación y análisis.

## Alcance

Este repo parte de `Datos_Radar`, pero quedó limpiado para uso operativo:

- Sin datos históricos descargados.
- Sin la carpeta `Datos_Redes_Sets_Enteros_55_Semanas`.
- Con carpetas de salida separadas para cada red, dejando solo la arquitectura en Git.
- Con scripts ajustados para escribir dentro de este mismo repo.
- Sin secretos embebidos en código.

## Estructura

```text
Tampico_Semanal_Reportes/
├── Claude/
├── Datos/
├── Facebook/
├── Instagram/
├── Medios/
├── Scripts/
├── TikTok/
├── Twitter/
├── Youtube/
└── state/
```

Donde:

- `Claude/`: Analisis tematicos generados por Claude (corpus combinado + analisis)
- `Datos/`: Archivos consolidados y procesados por semana
- `Influencia_Temas/`: Analisis correlacional de influencia de temas sobre polaridad
- `Temas_Guiados/`: Clasificacion de documentos por temas guiados por palabras clave
- `Facebook/`, `Instagram/`, `Medios/`, `TikTok/`, `Twitter/`, `Youtube/`: Descargas por red/fuente

## Scripts incluidos

- `Scripts/00_orquestador_general.py`
- `Scripts/01_youtube_tampico.py`
- `Scripts/02_twitter_tampico.py`
- `Scripts/03_medios_tampico.py`
- `Scripts/04_facebook_comentarios_tampico.py`
- `Scripts/05_facebook_posts_tampico.py`
- `Scripts/6_consolidador_datos.py`
- `Scripts/7_modelado_temas_claude.py`
- `Scripts/8_influencia_temas.py`
- `Scripts/9_temas_guiados.py`
- `Scripts/5a_extractors_instagram.py`
- `Scripts/5b_extractors_tiktok.py`

## Variables de entorno

Define las credenciales antes de correr los extractores:

```bash
export YOUTUBE_API_KEY=""
export APIFY_TOKEN=""
export CLAUDE_API_KEY=""
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
- Las salidas semanales se generan dentro de la carpeta de cada fuente y usan etiquetas por semana.
- Instagram (pipeline 12) y TikTok (pipeline 13) usan `APIFY_TOKEN`. Sus búsquedas, hashtags y perfiles se centralizan en `Scripts/queries_config.py`.
- Los perfiles oficiales de Instagram y TikTok permanecen vacíos hasta confirmarlos. Mientras tanto, ambos extractores operan como descubrimiento por consultas y hashtags dirigidos.
- Ambos extractores admiten `--dry-run`: muestran la entrada prevista sin llamar a Apify, sin requerir token y sin crear archivos.
- La carpeta `Influencia_Temas/{semana}/` contiene analisis correlacional de temas sobre polaridad con reportes tecnicos (CSVs) y ejecutivos (KPIs, hallazgos, alertas).
- El pipeline 8 (Analisis de Influencia) requiere que se ejecute primero el pipeline 6 (Consolidador) para generar `material_institucional.txt` e `material_comentarios.txt`.
- La carpeta `Temas_Guiados/{semana}/` contiene clasificacion por tema, top de palabras y reporte textual del analisis guiado.
- El pipeline 9 (Temas Guiados) requiere que se ejecute primero el pipeline 6 (Consolidador), salvo que se indique un `--input-file` explicito.
- El análisis temático con Claude toma su insumo desde `Datos/{semana}/`, donde primero se crea un corpus combinado sin borrar los dos materiales originales.
- `.gitignore` está configurado para no versionar descargas, cachés ni credenciales futuras.
