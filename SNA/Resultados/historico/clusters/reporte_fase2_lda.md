# Reporte SNA - Fase 2: Modelado de Temas (LDA)

_Generado: 2026-07-01 13:16 UTC_

## 1. Barrido de K

| K | Coherencia c_v | Perplexity |
|---|---------------:|-----------:|
| 15 | 0.5081 | -6.95 |
| 16 | 0.4754 | -6.99 |
| 17 | 0.4874 | -7.00 |
| 18 | 0.4885 | -7.00 |
| 19 | 0.4491 | -7.04 |
| 20 | 0.4848 | -7.02 |
| 21 | 0.4198 | -7.07 |
| 22 | 0.4704 | -7.07 |
| 23 | 0.4709 | -7.07 |
| 24 | 0.4702 | -7.10 |
| 25 | 0.4656 | -7.12 |

**K optimo: 15** (c_v = 0.5081)

## 2. Temas descubiertos

### Tema 0 (97 terminos)

**Top 10:** proteccion, civil, lluvia, zona, seguridad, bomberos, recorrido, cuida, abril, emergencia

- Aristas internas (coocurrencia ventana=3): **1351**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 1 (107 terminos)

**Top 10:** espacio, familia, convivencia, impulsar, actividad, cultura, deportivo, cultural, disfrutar, pasion

- Aristas internas (coocurrencia ventana=3): **1916**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 2 (83 terminos)

**Top 10:** seguir, excelente, colonia, bienestar, programa, tampicotecuida, accion, tampicovacontodo, tampiqueno, salud

- Aristas internas (coocurrencia ventana=3): **1408**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 3 (99 terminos)

**Top 10:** monica, villarreal, anaya, limpieza, laguna, gob, jornada, agua, basura, visitante

- Aristas internas (coocurrencia ventana=3): **1242**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 4 (111 terminos)

**Top 10:** mundial, partido, nacional, mexicano, persona, padre, emocion, venir, mal, mes

- Aristas internas (coocurrencia ventana=3): **1221**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 5 (71 terminos)

**Top 10:** tamaulipas, cdvictoria, reynosa, matamoros, altamira, nuevolaredo, cdmadero, nota, cdmx, monterrey

- Aristas internas (coocurrencia ventana=3): **573**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 6 (88 terminos)

**Top 10:** dia, revistadebate, gente, libertad, col, papa, marina, exposicion, entregar, sumar

- Aristas internas (coocurrencia ventana=3): **842**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 7 (91 terminos)

**Top 10:** atencion, mantener, limpio, vida, hospital, reporte, calor, medico, morir, banqueta

- Aristas internas (coocurrencia ventana=3): **757**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 8 (100 terminos)

**Top 10:** presidenta, nino, gracias, felicidad, felicidades, evento, nina, alcaldesa, bonito, vivir

- Aristas internas (coocurrencia ventana=3): **1271**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 9 (102 terminos)

**Top 10:** calle, dejar, senora, proteger, recibir, limpiar, andar, canal, ojala, peaton

- Aristas internas (coocurrencia ventana=3): **1065**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 10 (120 terminos)

**Top 10:** hora, internet, madre, mujer, ganar, llegar, numero, mayo, acapulco, comentario

- Aristas internas (coocurrencia ventana=3): **1442**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 11 (137 terminos)

**Top 10:** pueblo, pagar, pasar, deber, ano, justicia, hablar, salir, querer, saludo

- Aristas internas (coocurrencia ventana=3): **2303**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 12 (130 terminos)

**Top 10:** mexico, gobierno, narco, morena, lopez, claudia, politico, corrupto, huachicol, robar

- Aristas internas (coocurrencia ventana=3): **2234**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 13 (82 terminos)

**Top 10:** equipo, joven, ciudad, oportunidad, esfuerzo, importante, construir, economico, futuro, campeon

- Aristas internas (coocurrencia ventana=3): **894**
- Vinculos externos (coocurrencia ventana=12): **200**

### Tema 14 (92 terminos)

**Top 10:** fortalecer, municipal, publico, educacion, leer, secretaria, sector, turistico, coordinacion, avanzar

- Aristas internas (coocurrencia ventana=3): **1331**
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
| T0 | 0 | 80 | 522 | 403 | 38 | 125 | 23 | 9 | 76 | 68 | 57 | 0 | 20 | 77 | 362 |
| T1 | 31 | 0 | 941 | 524 | 30 | 129 | 60 | 81 | 280 | 0 | 14 | 0 | 144 | 297 | 476 |
| T2 | 149 | 724 | 0 | 1267 | 0 | 77 | 88 | 103 | 1191 | 30 | 0 | 0 | 166 | 426 | 514 |
| T3 | 205 | 486 | 1489 | 0 | 0 | 106 | 119 | 264 | 1063 | 96 | 14 | 0 | 107 | 314 | 699 |
| T4 | 58 | 204 | 164 | 97 | 0 | 116 | 90 | 8 | 178 | 0 | 7 | 58 | 196 | 102 | 22 |
| T5 | 165 | 187 | 234 | 171 | 66 | 0 | 146 | 26 | 92 | 70 | 69 | 116 | 176 | 245 | 354 |
| T6 | 68 | 196 | 436 | 225 | 70 | 146 | 0 | 24 | 268 | 64 | 37 | 35 | 103 | 107 | 99 |
| T7 | 23 | 125 | 504 | 502 | 8 | 63 | 43 | 0 | 96 | 47 | 11 | 0 | 18 | 149 | 191 |
| T8 | 27 | 314 | 1529 | 1086 | 28 | 83 | 175 | 57 | 0 | 18 | 30 | 75 | 196 | 233 | 193 |
| T9 | 104 | 59 | 250 | 388 | 12 | 99 | 88 | 70 | 53 | 0 | 36 | 77 | 29 | 74 | 84 |
| T10 | 57 | 106 | 188 | 137 | 19 | 91 | 62 | 11 | 185 | 36 | 0 | 49 | 66 | 77 | 84 |
| T11 | 0 | 56 | 199 | 105 | 64 | 151 | 55 | 6 | 294 | 54 | 31 | 0 | 274 | 30 | 14 |
| T12 | 20 | 221 | 411 | 151 | 175 | 183 | 103 | 11 | 325 | 16 | 41 | 191 | 0 | 117 | 298 |
| T13 | 59 | 384 | 848 | 430 | 33 | 164 | 70 | 89 | 274 | 21 | 11 | 12 | 33 | 0 | 243 |
| T14 | 195 | 488 | 747 | 798 | 0 | 264 | 47 | 39 | 171 | 15 | 51 | 0 | 173 | 199 | 0 |
