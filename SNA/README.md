# SNA historico de Tampico

El corpus de entrada es `SNA/Datos/tampico_datos_tabulares_consolidados.csv`.
Incluye Twitter, Facebook y YouTube; `Medios` queda excluido.

El consolidador integra las descargas del repo y, cuando existe, el historico
social de `/home/emilio/Documentos/RAdAR/Datos_RAdAR/Juntos`. Los formatos sin
cuenta se conservan para temas, pero no participan en la red de cuentas. Para
generar solo con las fuentes del repo se puede usar `--sin-radar`; otra copia
del historico se indica con `--radar-dir RUTA`.

## Preparacion

```bash
.venv/bin/pip install -r requirements-sna.txt
.venv/bin/python -m spacy download es_core_news_md
```

## Ejecucion

```bash
.venv/bin/python Scripts/11_consolidar_historico_sna.py
.venv/bin/python Scripts/12_lda_sna.py
.venv/bin/python Scripts/12b_subclusters_louvain.py --resolution 1.0
.venv/bin/python Scripts/12c_diagnostico_umbrales.py
.venv/bin/python Scripts/12c_red_completa.py
.venv/bin/python Scripts/18_cuentas_clusters.py
.venv/bin/python Scripts/12d_red_cuentas.py
.venv/bin/python Scripts/19_red_posiciones_discursivas.py
```

Los resultados se escriben en `SNA/Resultados/historico/`. El diagnostico
calcula umbrales por capa a partir del percentil 75 del corpus; la red completa
los utiliza automaticamente, salvo que se indiquen valores por CLI.
