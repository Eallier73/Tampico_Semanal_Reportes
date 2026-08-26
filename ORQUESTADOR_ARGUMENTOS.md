# Argumentos del Orquestador

Este documento resume, script por script, qué argumentos conviene pedir desde el orquestador general y qué tan interactivo era cada extractor antes de unificarlo.

## Contrato temporal común

- `--since` es inclusivo y `--before` es exclusivo: `[since, before)`.
- Ambos son obligatorios y `before` debe ser posterior a `since`.
- Todas las fuentes y stages derivados usan una etiqueta con ambos límites, por ejemplo `2026_agosto_01_al_2026_agosto_09_Datos`.
- Cada carpeta guarda `contrato_rango_fechas.json`.
- Cada descarga real agrega un registro a `state/download_history.jsonl`; los `dry-run` no agregan entradas.
- La GUI presenta la última descarga registrada de cada fuente y permite actualizar la tabla manualmente.

## Convención de nombres

- `1_extractors_youtube.py`
- `2_extractors_twitter.py`
- `3_extractors_medios.py`
- `4_extractors_facebook_posts.py`
- `5_extractors_facebook_comentarios.py`
- `6_consolidador_datos.py`
- `7_modelado_temas_claude.py`
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

## 04 Facebook posts

- Prompt propio previo: sí.
- Argumentos clave:
  - `--pages`
  - `--input-csv`
  - `--max-posts`
  - `--max-pages`
  - `--sample-percent`
  - `--sample-seed`
  - `--since`
  - `--before`
  - `--batch-size`
  - `--output-dir`
- Credenciales:
  - `APIFY_TOKEN`

## 05 Facebook comentarios desde CSV de posts

- Prompt propio previo: sí.
- Argumentos clave:
  - `--input-csv`
  - `--since`
  - `--before`
  - `--max-comments`
  - `--max-urls`
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

## 12 Instagram (Apify)

- Prompt propio previo: no.
- Argumentos clave:
  - `--since`
  - `--before`
  - `--profile` repetible
  - `--hashtag` repetible
  - `--query` repetible
  - `--results-limit`
  - `--search-limit`
  - `--output-dir`
  - `--dry-run`
- Credenciales:
  - `APIFY_TOKEN` solo durante una ejecución real
- Salida:
  - `Instagram/{rango}/`: CSV canónico y materiales de posts, menciones y comentarios incluidos en los resultados.

## 13 TikTok (Apify)

- Prompt propio previo: no.
- Argumentos clave:
  - `--since`
  - `--before`
  - `--profile` repetible
  - `--hashtag` repetible
  - `--query` repetible
  - `--results-limit`
  - `--output-dir`
  - `--dry-run`
- Credenciales:
  - `APIFY_TOKEN` solo durante una ejecución real
- Salida:
  - `TikTok/{rango}/`: CSV canónico y materiales de publicaciones o menciones.

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
  - Requiere que exista `Datos/{rango}/material_institucional.txt`
  - Requiere que exista `Datos/{rango}/material_comentarios.txt`

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
    - Requiere que exista `Datos/{rango}/material_institucional.txt`
    - Requiere que exista `Datos/{rango}/material_comentarios.txt`
    - Se ejecuta tipicamente despues del pipeline 6 (Consolidador)
  - Salidas:
    - `Influencia_Temas/{rango}/tecnico/`: influencia_temas.csv, polaridad_documentos.csv
    - `Influencia_Temas/{rango}/ejecutivo/`: 00_resumen_ejecutivo.md, 01_kpis_polaridad_por_tema.csv, 01b_kpis_polaridad_por_subtema.csv, 02_top_hallazgos_polaridad.csv, 03_alertas_polaridad.csv
  - Métodos empleados:
    - Ridge Regression para coeficientes de influencia
    - Regresion Logistica para direccion de polaridad
    - Correlacion de Pearson para asociacion tema-polaridad
    - Clasificacion de impacto (Alta/Media/Baja) y confianza

  ## 09 Analisis de Temas Guiados

  - Prompt propio previo: no.
  - Argumentos clave:
    - `--since`
    - `--before`
    - `--input-dir`
    - `--output-dir`
    - `--exclude-words-path`
    - `--input-file` (opcional)
  - No requiere credenciales
  - Dependencia operativa:
    - Requiere que exista `Datos/{rango}/material_institucional.txt`
    - Requiere que exista `Datos/{rango}/material_comentarios.txt`
    - Alternativamente se puede usar `--input-file` para forzar un archivo de entrada especifico
  - Salidas:
    - `Temas_Guiados/{rango}/`: clasificacion_temas_guiados.csv, distribucion_temas_guiados.png, top75_palabras_temas_guiados.csv, informe_temas_guiados.txt
  - Antes del envío crea un corpus combinado `.txt` dentro de la carpeta semanal de `Datos`

## Criterio del orquestador

- El orquestador pregunta una vez el rango global `since/before`.
- Luego pide solo los parámetros específicos de cada pipeline seleccionado.
- Las credenciales sensibles se capturan sin exponerlas en la línea de comandos.
- La ejecución de los scripts se hace con CLI explícita y, cuando aplica, con `--no-prompt`.
- El pipeline 11 recibe el mismo rango y genera los doce pasos SNA en carpetas de `SNA/Datos` y `SNA/Resultados` identificadas por ambos límites.
- Los accesos rápidos de SNA de la GUI detectan el último rango o los dos rangos descargados más recientes a partir de los CSV. Guardan cada repetición en una subcarpeta de ejecución única y dejan un `manifiesto_ejecucion_sna.json` con los lotes seleccionados.
