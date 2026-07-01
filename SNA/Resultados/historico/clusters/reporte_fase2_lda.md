# Reporte SNA - Fase 2: Modelado de Temas (LDA)

_Generado: 2026-07-01 14:17 UTC_

## 1. Barrido de K

| K | Coherencia c_v | Perplexity |
|---|---------------:|-----------:|
| 15 | 0.4952 | -9.18 |
| 16 | 0.4732 | -9.26 |
| 17 | 0.4655 | -9.33 |
| 18 | 0.4678 | -9.40 |
| 19 | 0.4520 | -9.48 |
| 20 | 0.4600 | -9.56 |
| 21 | 0.4779 | -9.61 |
| 22 | 0.4478 | -9.70 |
| 23 | 0.4391 | -9.76 |
| 24 | 0.4642 | -9.83 |
| 25 | 0.4511 | -9.91 |

**K optimo: 15** (c_v = 0.4952)

## 2. Temas descubiertos

### Tema 0 (177 terminos)

**Top 10:** deporte, ciudad, invitar, cultura, deportivo, historia, maria, disfrutar, actividad, orgullo

- Aristas internas (coocurrencia ventana=3): **7113**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 1 (182 terminos)

**Top 10:** madre, mujer, hijo, cartel, mama, policia, salir, justicia, volver, nombre

- Aristas internas (coocurrencia ventana=3): **9657**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 2 (230 terminos)

**Top 10:** ganar, love, like, comentario, campeon, extorsion, pareja, tecnica, real, santiago

- Aristas internas (coocurrencia ventana=3): **3640**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 3 (207 terminos)

**Top 10:** pesos, ayuda, necesitar, estudio, mercado, contar, dirigido, cientifico, mante, tarjeta

- Aristas internas (coocurrencia ventana=3): **2905**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 4 (222 terminos)

**Top 10:** animal, adan, simulacro, impacto, denuncia, rapido, par, lagunario, patrimonio, bache

- Aristas internas (coocurrencia ventana=3): **4539**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 5 (170 terminos)

**Top 10:** calle, zona, colonia, agua, basura, norte, casa, seleccion, lluvia, avenida

- Aristas internas (coocurrencia ventana=3): **6310**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 6 (176 terminos)

**Top 10:** desarrollo, impulsar, laguna, obra, junio, carpintero, proyecto, espacio, local, economico

- Aristas internas (coocurrencia ventana=3): **6886**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 7 (142 terminos)

**Top 10:** monica, villarreal, presidenta, seguir, municipal, bienestar, accion, gobierno, tampicotecuida, fortalecer

- Aristas internas (coocurrencia ventana=3): **7790**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 8 (132 terminos)

**Top 10:** gracias, dia, familia, felicidad, felicidades, excelente, vida, hermoso, ano, feliz

- Aristas internas (coocurrencia ventana=3): **5400**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 9 (152 terminos)

**Top 10:** tampico, tamaulipas, mexico, altamira, cdvictoria, futbol, mundial, matamoros, nota, reynosa

- Aristas internas (coocurrencia ventana=3): **3071**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 10 (236 terminos)

**Top 10:** riobravo, vallehermoso, estudiante, servir, coahuila, torre, uat, vivar, directo, reparacion

- Aristas internas (coocurrencia ventana=3): **4869**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 11 (149 terminos)

**Top 10:** deber, pais, pueblo, pagar, pasar, mal, mexicano, puro, claudia, dinero

- Aristas internas (coocurrencia ventana=3): **8987**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 12 (198 terminos)

**Top 10:** lopez, morena, destruir, millon, investigacion, judicial, unidos, votar, ejercito, obrador

- Aristas internas (coocurrencia ventana=3): **7208**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 13 (227 terminos)

**Top 10:** mundo, perder, persona, venir, dejar, alma, arbol, mes, semana, dato

- Aristas internas (coocurrencia ventana=3): **8483**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 14 (195 terminos)

**Top 10:** gente, querer, gustar, pensar, favor, video, luz, creer, alguien, andar

- Aristas internas (coocurrencia ventana=3): **8817**
- Vinculos externos (coocurrencia ventana=12): **200**

---

**Archivos generados:**
- `lda_barrido.csv` - resultados del barrido de K configurado
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

| De \\ Hacia | T0 | T1 | T2 | T3 | T4 | T5 | T6 | T7 | T8 | T9 | T10 | T11 | T12 | T13 | T14 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| T0 | 0 | 191 | 0 | 41 | 0 | 336 | 797 | 4770 | 2029 | 5067 | 0 | 41 | 0 | 0 | 94 |
| T1 | 437 | 0 | 0 | 82 | 0 | 265 | 174 | 1981 | 2547 | 1499 | 0 | 761 | 32 | 336 | 965 |
| T2 | 313 | 86 | 0 | 139 | 38 | 54 | 48 | 357 | 470 | 765 | 27 | 478 | 78 | 203 | 306 |
| T3 | 178 | 212 | 109 | 0 | 0 | 216 | 62 | 1466 | 863 | 922 | 0 | 398 | 95 | 521 | 435 |
| T4 | 271 | 121 | 38 | 17 | 0 | 314 | 202 | 1372 | 530 | 1396 | 53 | 99 | 36 | 55 | 49 |
| T5 | 407 | 234 | 0 | 0 | 63 | 0 | 455 | 3948 | 1003 | 4232 | 35 | 208 | 0 | 78 | 479 |
| T6 | 797 | 40 | 0 | 0 | 52 | 283 | 0 | 5622 | 671 | 4780 | 0 | 38 | 0 | 45 | 0 |
| T7 | 2082 | 307 | 0 | 0 | 0 | 605 | 1388 | 0 | 9342 | 15038 | 0 | 873 | 520 | 78 | 903 |
| T8 | 904 | 823 | 0 | 0 | 77 | 164 | 274 | 11968 | 0 | 4966 | 0 | 670 | 63 | 650 | 1183 |
| T9 | 3136 | 388 | 0 | 292 | 0 | 2481 | 2719 | 15263 | 4433 | 0 | 232 | 836 | 188 | 387 | 743 |
| T10 | 75 | 121 | 15 | 15 | 53 | 87 | 141 | 1234 | 198 | 2158 | 0 | 110 | 45 | 14 | 38 |
| T11 | 74 | 602 | 47 | 157 | 0 | 208 | 110 | 2955 | 1744 | 2020 | 0 | 0 | 329 | 593 | 1853 |
| T12 | 22 | 149 | 63 | 76 | 21 | 0 | 120 | 2073 | 399 | 1425 | 0 | 1181 | 0 | 107 | 429 |
| T13 | 187 | 394 | 0 | 248 | 0 | 223 | 45 | 1552 | 2186 | 1374 | 0 | 930 | 0 | 0 | 1138 |
| T14 | 94 | 767 | 42 | 120 | 0 | 413 | 0 | 2797 | 2789 | 1604 | 0 | 1718 | 85 | 751 | 0 |
