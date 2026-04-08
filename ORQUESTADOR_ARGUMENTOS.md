# Argumentos del Orquestador

Este documento resume, script por script, qué argumentos conviene pedir desde el orquestador general y qué tan interactivo era cada extractor antes de unificarlo.

## Convención de nombres

- `01_youtube_tampico.py`
- `02_twitter_tampico.py`
- `03_medios_tampico.py`
- `04_facebook_comentarios_tampico.py`
- `05_facebook_posts_tampico.py`
- `06_consolidador_datos.py`
- `07_modelado_temas_claude.py`
- `00_orquestador_general.py`

## 01 YouTube

- Prompt propio previo: sí, solo para decidir modo de descarga.
- Argumentos clave:
  - `--since`
  - `--before`
  - `--channels`
  - `--queries`
  - `--mode`
  - `--max-videos-query`
  - `--max-videos-channel`
  - `--output-dir`
- Credenciales:
  - `YOUTUBE_API_KEY`
  - `YT_PROXY_HTTP` opcional
  - `YT_PROXY_HTTPS` opcional

## 02 Twitter/X

- Prompt propio previo: no.
- Argumentos clave:
  - `--since`
  - `--before`
  - `--query` repetible
  - `--output-dir`
  - `--state-path`
  - `--max-tweets`
  - `--max-replies-per-tweet`
  - `--max-reply-scrolls`
  - `--no-headless` opcional
- Requisito operativo:
  - `state/x_state.json`

## 03 Medios Tampico

- Prompt propio previo: no.
- Argumentos clave:
  - `--since`
  - `--before`
  - `--medio` repetible
  - `--termino` repetible
  - `--modo-queries`
  - `--output-dir`
  - `--nombre-archivo-base`
  - `--omitir-semanas-existentes`
  - `--pausa`
  - `--pausa-entre-queries`

## 04 Facebook desde CSV de URLs

- Prompt propio previo: sí.
- Argumentos clave:
  - `--mode`
  - `--pages`
  - `--input-csv`
  - `--max-comments`
  - `--max-urls`
  - `--sample-percent`
  - `--sample-seed`
  - `--since`
  - `--before`
  - `--batch-size`
  - `--output-dir`
- Credenciales:
  - `APIFY_TOKEN` cuando se descargan comentarios

## 05 Facebook posts

- Prompt propio previo: sí.
- Argumentos clave:
  - `--pages`
  - `--since`
  - `--before`
  - `--max-posts`
  - `--max-pages`
  - `--sample-percent`
  - `--sample-seed`
  - `--batch-size`
  - `--output-dir`
- Credenciales:
  - `APIFY_TOKEN`

## 06 Consolidador de datos

- Prompt propio previo: no.
- Argumentos clave:
  - `--since`
  - `--before`
  - `--base-dir`
  - `--output-dir`

## 07 Modelado temático con Claude

- Prompt propio previo: no.
- Argumentos clave:
  - `--since`
  - `--before`
  - `--input-dir`
  - `--output-dir`
  - `--model`
  - `--max-corpus-chars`
- Credenciales:
  - `CLAUDE_API_KEY`
- Dependencia operativa:
  - Requiere que exista `Datos/{semana}/material_institucional.txt`
  - Requiere que exista `Datos/{semana}/material_comentarios.txt`

  ## 08 Analisis de Influencia de Temas

  - Prompt propio previo: no.
  - Argumentos clave:
    - `--since`
    - `--before`
    - `--input-dir`
    - `--output-dir`
    - `--stopwords-path`
  - No requiere credenciales
  - Dependencia operativa:
    - Requiere que exista `Datos/{semana}/material_institucional.txt`
    - Requiere que exista `Datos/{semana}/material_comentarios.txt`
    - Se ejecuta tipicamente despues del pipeline 6 (Consolidador)
  - Salidas:
    - `Influencia_Temas/{semana}/tecnico/`: influencia_temas.csv, polaridad_documentos.csv
    - `Influencia_Temas/{semana}/ejecutivo/`: 00_resumen_ejecutivo.md, 01_kpis_polaridad_por_tema.csv, 01b_kpis_polaridad_por_subtema.csv, 02_top_hallazgos_polaridad.csv, 03_alertas_polaridad.csv
  - Métodos empleados:
    - Ridge Regression para coeficientes de influencia
    - Regresion Logistica para direccion de polaridad
    - Correlacion de Pearson para asociacion tema-polaridad
    - Clasificacion de impacto (Alta/Media/Baja) y confianza
  - Antes del envío crea un corpus combinado `.txt` dentro de la carpeta semanal de `Datos`

## Criterio del orquestador

- El orquestador pregunta una vez el rango global `since/before`.
- Luego pide solo los parámetros específicos de cada pipeline seleccionado.
- Las credenciales sensibles se capturan sin exponerlas en la línea de comandos.
- La ejecución de los scripts se hace con CLI explícita y, cuando aplica, con `--no-prompt`.
