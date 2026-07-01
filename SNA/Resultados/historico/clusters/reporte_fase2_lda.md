# Reporte SNA - Fase 2: Modelado de Temas (LDA)

_Generado: 2026-07-01 11:49 UTC_

## 1. Barrido de K

| K | Coherencia c_v | Perplexity |
|---|---------------:|-----------:|
| 4 | 0.5574 | -7.09 |
| 5 | 0.5780 | -7.14 |
| 6 | 0.5759 | -7.17 |
| 7 | 0.5206 | -7.18 |
| 8 | 0.5192 | -7.19 |
| 9 | 0.5257 | -7.19 |
| 10 | 0.4876 | -7.22 |

**K optimo: 5** (c_v = 0.5780)

## 2. Temas descubiertos

### Tema 0 (170 terminos)

**Top 10:** presidenta, gracias, villarreal, gobernador, mónica, alcaldesa, américo, exelente, gobierno, persona

- Aristas internas (coocurrencia ventana=3): **2189**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 1 (182 terminos)

**Top 10:** seguir, bienestar, acción, familia, colonia, tampicotecuida, día, espacio, salud, fortalecer

- Aristas internas (coocurrencia ventana=3): **5083**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 2 (174 terminos)

**Top 10:** excelente, tamaulipas, altamira, paz, cdvictoria, reynosa, matamoros, madero, slp, nuevolaredo

- Aristas internas (coocurrencia ventana=3): **1985**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 3 (200 terminos)

**Top 10:** méxico, pasión, felicidad, querer, deber, pasar, gente, mexicano, partido, fútbol

- Aristas internas (coocurrencia ventana=3): **3712**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 4 (200 terminos)

**Top 10:** tampico, ciudad, historia, impulsar, desarrollo, exposición, económico, jaiba, tampiqueño, importante

- Aristas internas (coocurrencia ventana=3): **4672**
- Vinculos externos (coocurrencia ventana=12): **200**

---

**Archivos generados:**
- `lda_barrido.csv` - resultados del barrido K=5..10
- `lda_mejor_modelo.json` - metadata del modelo optimo
- `lda_asignacion.csv` - termino, tema_id, peso
- `temas_terminos.csv` - top 20 terminos por tema
- `intracluster/tema_XX.csv` - coocurrencias internas (ventana=3)
- `extracluster/tema_XX.csv` - vinculos externos (ventana=12)
- `matriz_entre_temas.csv` - matriz KxK de vinculos entre temas
- `matriz_entre_temas_top.csv` - top-3 pares por par de temas
- `resumen_fase2_lda.json` - resumen estructurado

## 3. Matriz de vinculos entre temas (K x K)

Cada celda `M[i][j]` = suma de coocurrencias de terminos del tema i con terminos del tema j (ventana=12, deduplicada).

| De \\ Hacia | T0 | T1 | T2 | T3 | T4 |
|---|---|---|---|---|---|
| T0 | 0 | 2917 | 472 | 274 | 1806 |
| T1 | 2188 | 0 | 214 | 99 | 4180 |
| T2 | 613 | 634 | 0 | 603 | 2101 |
| T3 | 539 | 364 | 648 | 0 | 862 |
| T4 | 1554 | 4163 | 1503 | 341 | 0 |
