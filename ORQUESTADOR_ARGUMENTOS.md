# Argumentos del Orquestador

Este documento resume, script por script, qué argumentos conviene pedir desde el orquestador general y qué tan interactivo era cada extractor antes de unificarlo.

## Convención de nombres

- `01_youtube_tampico.py`
- `02_twitter_tampico.py`
- `03_medios_tampico.py`
- `04_facebook_comentarios_tampico.py`
- `05_facebook_posts_tampico.py`
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

## Criterio del orquestador

- El orquestador pregunta una vez el rango global `since/before`.
- Luego pide solo los parámetros específicos de cada pipeline seleccionado.
- Las credenciales sensibles se capturan sin exponerlas en la línea de comandos.
- La ejecución de los scripts se hace con CLI explícita y, cuando aplica, con `--no-prompt`.
