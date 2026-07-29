#!/usr/bin/env python3
"""
Modulo SNA - Fase 2 (reescritura): Modelado de temas con LDA + cohesión.

Pipeline:
  1. Lee el historico consolidado desde SNA/Datos/
  2. Tokeniza y lematiza con spaCy (es_core_news_md) - reusando la misma
     limpieza que en el script anterior, para que el vocabulario sea comparable.
  3. Construye el diccionario gensim y el corpus BoW.
  4. Barrido LDA de K=25 a K=35 y selección por coherencia c_v para evitar
     sobresegmentar la conversación en temas débiles o redundantes.
  5. Asignacion hard: cada termino va al tema con mayor P(term|tema).
  6. Cohesion intracluster: coocurrencias (ventana=3) entre los terminos
     de cada tema. Output por tema: aristas internas con peso.
  7. Vinculos extracluster: coocurrencias (ventana=12) entre los terminos
     de un tema y el resto del vocabulario. Output: palabras "puente"
     con peso y tema destino.

Salidas (todo dentro de SNA/Resultados/historico/clusters/):
  - lda_barrido.csv           : K, coherencia_c_v, perplexity, n_iter
  - lda_mejor_modelo.json     : K optimo, metricas, alpha, eta
  - lda_asignacion.csv        : termino, tema_id, peso_term_en_tema
  - temas_terminos.csv        : tema_id, n_terminos, top_terminos (string)
  - intracluster/<tema>.csv   : source, target, weight (coocurrencia interna)
  - extracluster/<tema>.csv   : termino_interno, termino_externo, weight
  - resumen_fase2_lda.json    : resumen estructurado
  - reporte_fase2_lda.md      : reporte en markdown

Uso:
  .venv/bin/python Scripts/12_lda_sna.py
"""

from __future__ import annotations

import argparse
import json
import re
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from sna_spanish_filter import (
    DEFAULT_ENGLISH_DICTIONARY,
    DEFAULT_MIN_SPANISH_SHARE,
    DEFAULT_SPANISH_DICTIONARY,
    is_spanish_message,
    load_language_vocabulary,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_INPUT = REPO_ROOT / "SNA" / "Datos" / "tampico_datos_tabulares_consolidados.csv"
DEFAULT_OUTPUT = REPO_ROOT / "SNA" / "Resultados" / "historico" / "clusters"


# Pronombres clíticos que spaCy pega al lema del verbo en español.
# Lemas que terminan en " <pron>" son variantes del mismo término.
# Los descartamos para no inflar el vocabulario artificialmente.
_RE_PRONOMBRE_PEGADO = re.compile(
    r"\s+(él|ella|ellos|ellas|yo|tú|nosotros|nosotras|vosotros|vosotras|"
    r"me|te|se|nos|os|lo|le|les|la|las|los|mi|tu|su|mis|tus|sus)$"
)

_GENERIC_LEMMAS = {
    "aplauso", "bien", "bonito", "bravo", "excelente", "felicidad",
    "felicidades", "genial", "gracias", "hermoso", "saludo", "saludos",
    "trabajo",
}
_LEMMA_ALIASES = {
    "exelente": "excelente",
}


# ===========================================================================
# 1. Carga del corpus unificado (Fase 1)
# ===========================================================================

def cargar_corpus(path: Path) -> pd.DataFrame:
    """Lee el historico tabular consolidado de Tampico."""
    if not path.exists():
        raise SystemExit(f"No se encontro el corpus consolidado: {path}")
    print(f"[1/8] Cargando corpus: {path.name}")
    df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False, on_bad_lines="skip")
    df.columns = [c.strip() for c in df.columns]
    return df


# Palabras tipicas de footers / barras de navegacion de scraping web.
# Si una linea tiene una densidad alta de estas palabras, la descartamos.
_FOOTER_KEYWORDS = {
    "galerias", "videos", "issuu", "aplicaciones", "moviles", "ultima",
    "hora", "anuncio", "anuncios", "publicidad", "comercial", "busqueda",
    "menu", "inicio", "seccion", "contacto", "cookies", "newsletter",
    "suscribete", "compartir", "tambien", "relacionadas", "recomendamos",
    "patrocina", "patrocinado",
}
_RE_URL = re.compile(r"https?://\S+|www\.\S+")
_RE_MENCION = re.compile(r"@\w+")
_RE_HASH = re.compile(r"#\w+")


def cargar_texto_plano(path_txt: Path) -> list[str]:
    """
    Lee un archivo .txt de texto crudo (un documento por linea) y lo limpia:
      - descarta lineas vacias
      - descarta URLs, menciones, hashtags del texto
      - descarta lineas con alta densidad de palabras de footer web
    """
    print(f"[1/8] Cargando texto plano: {path_txt.name}")
    with open(path_txt, encoding="utf-8", errors="ignore") as f:
        lineas_raw = [ln.rstrip("\n") for ln in f]

    textos_limpios: list[str] = []
    n_descartados_footer = 0
    n_descartados_vacios = 0
    for ln in lineas_raw:
        ln = ln.strip()
        if not ln:
            n_descartados_vacios += 1
            continue
        # Quitar URLs, menciones, hashtags
        ln = _RE_URL.sub(" ", ln)
        ln = _RE_MENCION.sub(" ", ln)
        ln = _RE_HASH.sub(" ", ln)
        ln = re.sub(r"\s+", " ", ln).strip()
        if not ln:
            n_descartados_vacios += 1
            continue
        # Deteccion de footer: si la mayoria de tokens son palabras de navegacion
        tokens = ln.lower().split()
        if len(tokens) >= 5:
            n_footer = sum(1 for t in tokens if t in _FOOTER_KEYWORDS)
            if n_footer / len(tokens) > 0.5:
                n_descartados_footer += 1
                continue
        textos_limpios.append(ln)
    print(f"         Lineas totales: {len(lineas_raw)}")
    print(f"         Descartadas (vacias o solo ruido): {n_descartados_vacios}")
    print(f"         Descartadas (footer de navegacion): {n_descartados_footer}")
    print(f"         Textos utiles: {len(textos_limpios)}")
    return textos_limpios


# ===========================================================================
# 2. Tokenizacion y lematizacion con spaCy (misma logica que 12_clusters)
# ===========================================================================

def _cargar_stopwords(extras: set[str] | None = None) -> set[str]:
    """
    Carga la stoplist del repo tal cual esta (sin normalizar).

    Si se pasa `extras`, se agregan al set final (esto NO modifica la
    stoplist del repo, solo añade terminos para esta corrida).
    """
    stopwords_total: set[str] = set()
    stopwords_path = (
        REPO_ROOT / "Scripts" / "diccionarios" / "stopwords" / "stop_list_espanol.txt"
    )
    if stopwords_path.exists():
        with open(stopwords_path, encoding="utf-8") as f:
            for line in f:
                w = line.strip().lower()
                if w:
                    stopwords_total.add(w)
    if extras:
        stopwords_total |= {t.lower().strip() for t in extras if t.strip()}
    return stopwords_total


def lematizar_textos(
    textos: list[str],
    batch_size: int = 256,
    stopwords_extra: set[str] | None = None,
) -> list[list[str]]:
    """Tokeniza, lematiza, filtra stopwords y tokens no alfabeticos.

    Si se pasa `stopwords_extra`, se concatenan a la stoplist del repo
    (sin modificarla). Esto sirve para quitar terminos especificos de
    una corrida (ej: gobierno, mexico) sin tocar la lista principal.
    """
    import spacy

    stopwords_total = _cargar_stopwords(stopwords_extra)
    n_extra = len(stopwords_extra) if stopwords_extra else 0
    print(
        f"         Stopwords: {len(stopwords_total)} terminos "
        f"(stoplist del repo tal cual + {n_extra} extra)"
    )

    print("[2/8] Cargando modelo spaCy es_core_news_md...")
    nlp = spacy.load("es_core_news_md", disable=["ner", "parser"])
    nlp.max_length = 2_000_000

    textos_proc: list[str] = []
    for t in textos:
        if not isinstance(t, str):
            textos_proc.append("")
            continue
        if len(t) > 1500:
            t = t[:1500]
        textos_proc.append(t)

    print(f"[2/8] Lematizando {len(textos_proc)} documentos (batch={batch_size})...")
    lemas_por_doc: list[list[str]] = []
    n_pronombres_descartados = 0
    for i, doc in enumerate(nlp.pipe(textos_proc, batch_size=batch_size)):
        lemas: list[str] = []
        for tok in doc:
            if tok.is_space or tok.is_punct or tok.like_num:
                continue
            if tok.like_url or tok.like_email:
                continue
            if not tok.is_alpha:
                continue
            lemma = tok.lemma_.lower().strip()
            if len(lemma) < 3:
                continue
            if lemma in stopwords_total:
                continue
            # Filtro anti-pronombres pegados al lema del verbo
            if _RE_PRONOMBRE_PEGADO.search(lemma):
                n_pronombres_descartados += 1
                continue
            lemas.append(lemma)
        lemas_por_doc.append(lemas)
        if (i + 1) % 5000 == 0:
            print(f"         {i + 1}/{len(textos_proc)}...")
    print(f"         Lemas descartados por pronombre pegado: {n_pronombres_descartados}")
    return lemas_por_doc


def normalizar_lemas(docs: list[list[str]]) -> list[list[str]]:
    """Unifica acentos y variantes ortograficas despues de lematizar."""
    salida: list[list[str]] = []
    for doc in docs:
        normalizado = []
        for lemma in doc:
            ascii_lemma = "".join(
                char for char in unicodedata.normalize("NFKD", lemma)
                if not unicodedata.combining(char)
            ).lower()
            normalizado.append(_LEMMA_ALIASES.get(ascii_lemma, ascii_lemma))
        salida.append(normalizado)
    return salida


def preparar_corpus_modelado(
    df: pd.DataFrame,
    lemas_por_mensaje: list[list[str]],
    max_frecuencia_contexto: int = 5,
    max_lemas_contexto: int = 500,
) -> tuple[list[list[str]], pd.DataFrame]:
    """Depura y agrega mensajes por publicacion para el modelado tematico.

    Los mensajes originales no se modifican. Los duplicados se retiran solo
    del corpus LDA para que su frecuencia no determine los temas.
    """
    rows: list[dict[str, object]] = []
    vistos: set[str] = set()
    descartados_genericos = 0
    descartados_duplicados = 0

    for pos, (_, row) in enumerate(df.iterrows()):
        lemas = lemas_por_mensaje[pos]
        if not lemas or (len(lemas) <= 3 and set(lemas) <= _GENERIC_LEMMAS):
            descartados_genericos += 1
            continue
        firma = " ".join(lemas)
        if firma in vistos:
            descartados_duplicados += 1
            continue
        vistos.add(firma)

        def valor(nombre: str) -> str:
            raw = row.get(nombre, "")
            return "" if pd.isna(raw) else str(raw).strip()

        plataforma = valor("plataforma")
        contexto = valor("url_contexto")
        origen = valor("url_origen")
        tipo_registro = valor("tipo_registro")
        registro_id = str(row.get("id", pos))
        referencia = origen if tipo_registro == "publicacion_institucional" else contexto or origen
        contexto_id = f"{plataforma}|{referencia or registro_id}"
        rows.append({
            "contexto_id": contexto_id,
            "plataforma": plataforma,
            "lemas_mensaje": lemas,
        })

    grupos: list[dict[str, object]] = []
    for contexto_id, grupo in pd.DataFrame(rows).groupby("contexto_id", sort=False):
        frecuencias: Counter[str] = Counter()
        lemas_contexto: list[str] = []
        for lemas in grupo["lemas_mensaje"]:
            for lemma in lemas:
                if frecuencias[lemma] >= max_frecuencia_contexto:
                    continue
                frecuencias[lemma] += 1
                lemas_contexto.append(lemma)
        if len(lemas_contexto) > max_lemas_contexto:
            paso = len(lemas_contexto) / max_lemas_contexto
            lemas_contexto = [
                lemas_contexto[int(i * paso)] for i in range(max_lemas_contexto)
            ]
        if lemas_contexto:
            grupos.append({
                "contexto_id": contexto_id,
                "plataforma": str(grupo["plataforma"].iloc[0]),
                "n_mensajes_utiles": int(len(grupo)),
                "n_lemas": len(lemas_contexto),
                "lemas": " ".join(lemas_contexto),
            })

    corpus_df = pd.DataFrame(grupos)
    print("[2b/8] Preparando corpus por conversacion...")
    print(f"         Mensajes genericos/vacios descartados: {descartados_genericos}")
    print(f"         Duplicados exactos descartados: {descartados_duplicados}")
    print(f"         Conversaciones modeladas: {len(corpus_df)}")
    return [str(value).split() for value in corpus_df["lemas"]], corpus_df


# ===========================================================================
# 3. Diccionario gensim + corpus BoW
# ===========================================================================

def construir_diccionario_y_corpus(
    docs: list[list[str]],
    no_below: int = 5,
    no_above: float = 0.5,
) -> tuple:
    """
    Construye el diccionario gensim filtrando extremos y el corpus BoW.
    """
    from gensim.corpora import Dictionary

    print(f"[3/8] Construyendo diccionario gensim (no_below={no_below}, no_above={no_above})...")
    dictionary = Dictionary(docs)
    dictionary.filter_extremes(no_below=no_below, no_above=no_above)
    print(f"         Vocabulario despues de filtrar extremos: {len(dictionary)} terminos")

    corpus = [dictionary.doc2bow(d) for d in docs]
    docs_no_vacios = sum(1 for bow in corpus if len(bow) > 0)
    print(f"         Documentos con contenido: {docs_no_vacios}/{len(corpus)}")
    return dictionary, corpus


# ===========================================================================
# 4. Barrido de LDA: elegir K por coherencia c_v
# ===========================================================================

def barrido_lda(
    corpus: list,
    dictionary,
    docs: list[list[str]],
    k_min: int = 25,
    k_max: int = 35,
    passes: int = 10,
    iterations: int = 100,
    seed: int = 42,
    selection_mode: str = "coherence",
    coherence_ratio: float = 0.90,
) -> tuple[list[dict], dict, object]:
    """
    Entrena LDA para cada K en [k_min, k_max] y devuelve:
      - lista de resultados del barrido
      - el modelo seleccionado segun el criterio configurado
      - el dictionary (para no perderlo)
    """
    from gensim.models import LdaModel
    from gensim.models.coherencemodel import CoherenceModel

    print(f"[4/8] Barrido LDA: K en [{k_min}..{k_max}], passes={passes}, iterations={iterations}...")

    resultados: list[dict] = []
    modelos: dict[int, object] = {}

    for k in range(k_min, k_max + 1):
        print(f"         Entrenando LDA con K={k}...")
        lda = LdaModel(
            corpus=corpus,
            id2word=dictionary,
            num_topics=k,
            passes=passes,
            iterations=iterations,
            random_state=seed,
            alpha="auto",
            eta="auto",
            chunksize=2000,
        )
        cm = CoherenceModel(
            model=lda, texts=docs, dictionary=dictionary, coherence="c_v"
        )
        cv = float(cm.get_coherence())
        perplexity = float(lda.log_perplexity(corpus))
        print(f"         K={k}: c_v={cv:.4f}, perplexity={perplexity:.2f}")
        resultados.append({
            "k": k,
            "coherencia_c_v": cv,
            "perplexity": perplexity,
        })
        modelos[k] = lda

    mejor_cv = max(resultados, key=lambda r: r["coherencia_c_v"])
    if selection_mode == "coherence":
        mejor = dict(mejor_cv)
        criterio = "maxima coherencia c_v"
    else:
        umbral = mejor_cv["coherencia_c_v"] * coherence_ratio
        candidatos = [r for r in resultados if r["coherencia_c_v"] >= umbral]
        mejor = dict(max(candidatos, key=lambda r: r["k"]))
        criterio = (
            f"mayor K con coherencia >= {coherence_ratio:.0%} del maximo "
            f"({umbral:.4f})"
        )
    mejor["criterio_seleccion"] = criterio
    mejor["coherencia_maxima_barrido"] = mejor_cv["coherencia_c_v"]
    mejor["k_maxima_coherencia"] = mejor_cv["k"]
    print(
        f"         K seleccionado: {mejor['k']} "
        f"(c_v={mejor['coherencia_c_v']:.4f}; {criterio})"
    )
    return resultados, mejor, modelos[mejor["k"]]


# ===========================================================================
# 5. Asignacion hard: cada termino va al tema con mayor P(term|tema)
# ===========================================================================

def asignar_terminos_a_temas(
    lda_model,
    dictionary,
) -> tuple[pd.DataFrame, dict[int, list[tuple[str, float]]]]:
    """
    Para cada termino del diccionario, encuentra el tema con mayor
    P(term|tema). Devuelve:
      - DataFrame con (termino, tema_id, peso)
      - dict tema_id -> [(termino, peso), ...] ordenado descendente
    """
    print("[5/8] Asignando terminos a temas (hard assignment por P(term|tema) max)...")

    K = lda_model.num_topics
    # Matriz (Vocab x K) de P(term|tema)
    # lda_model.show_topics devuelve la matriz terminos -> pesos por tema
    # Construimos manualmente recorriendo cada termino
    vocab_size = len(dictionary)
    # phi = lda_model.get_topics()  # shape (K, V)
    phi = lda_model.get_topics()  # type: ignore[attr-defined]

    rows = []
    terminos_por_tema: dict[int, list[tuple[str, float]]] = defaultdict(list)
    n_filtrados_peso = 0
    peso_minimo = 0.001

    for term_id in range(vocab_size):
        termino = dictionary.get(term_id)
        pesos = phi[:, term_id]  # shape (K,)
        tema_id = int(np.argmax(pesos))
        peso = float(pesos[tema_id])
        if peso < peso_minimo:
            n_filtrados_peso += 1
            continue
        rows.append({"termino": termino, "tema_id": tema_id, "peso": peso})
        terminos_por_tema[tema_id].append((termino, peso))

    print(f"         Terminos descartados por peso < {peso_minimo}: {n_filtrados_peso}")

    # Ordenar cada tema por peso descendente
    for tema_id in terminos_por_tema:
        terminos_por_tema[tema_id].sort(key=lambda x: -x[1])

    # Tamano de cada tema
    conteo = Counter(r["tema_id"] for r in rows)
    print(f"         Distribucion de terminos por tema:")
    for tema_id in sorted(conteo):
        print(f"           Tema {tema_id}: {conteo[tema_id]} terminos")

    df_asig = pd.DataFrame(rows).sort_values(["tema_id", "peso"], ascending=[True, False])
    return df_asig, terminos_por_tema


# ===========================================================================
# 6. Cohesion intracluster: coocurrencias ventana=3 dentro de cada tema
# ===========================================================================

def coocurrencia_intracluster(
    docs: list[list[str]],
    terminos_por_tema: dict[int, list[tuple[str, float]]],
    ventana: int = 3,
) -> dict[int, list[tuple[str, str, int]]]:
    """
    Para cada tema, calcula coocurrencias (ventana pequena) solo entre
    terminos que pertenecen al tema.

    Devuelve: dict tema_id -> [(source, target, count), ...]
    """
    print(f"[6/8] Cohesion intracluster (ventana={ventana})...")

    resultado: dict[int, list[tuple[str, str, int]]] = {}

    for tema_id, terminos in terminos_por_tema.items():
        set_tema = {t for t, _ in terminos}
        aristas: Counter[tuple[str, str]] = Counter()
        for d in docs:
            # Filtra el doc a solo terminos del tema
            toks = [t for t in d if t in set_tema]
            n = len(toks)
            if n < 2:
                continue
            for i in range(n):
                for j in range(i + 1, min(i + ventana + 1, n)):
                    a, b = toks[i], toks[j]
                    if a == b:
                        continue
                    # Orden alfabetico para clave canonica
                    key = (a, b) if a < b else (b, a)
                    aristas[key] += 1
        lista = [(a, b, c) for (a, b), c in aristas.items() if c > 0]
        lista.sort(key=lambda x: -x[2])
        resultado[tema_id] = lista
        print(f"         Tema {tema_id}: {len(lista)} aristas internas")

    return resultado


# ===========================================================================
# 7. Vinculos extracluster: ventana amplia, terminos del tema vs resto
# ===========================================================================

def coocurrencia_extracluster(
    docs: list[list[str]],
    terminos_por_tema: dict[int, list[tuple[str, float]]],
    ventana: int = 12,
    top_por_tema: int = 200,
    excluir: set[str] | None = None,
) -> dict[int, list[tuple[str, str, int]]]:
    """
    Para cada termino de cada tema, encuentra con que otros terminos
    (de cualquier tema o "sin tema") coocurre en ventana amplia.

    Reglas:
      - Las aristas se cuentan UNA sola vez por par (a, b), sin duplicar
        A->B y B->A.
      - Si ambos terminos del par pertenecen al MISMO cluster, se excluye
        (eso seria cohesion intracluster, no extracluster).
      - Si el par contiene un termino en `excluir`, se descarta por completo.
      - Los pares se ordenan por coocurrencia descendente y se recortan
        a top_por_tema.

    Devuelve: dict tema_id -> [(termino_interno, termino_externo, count), ...]
    """
    print(f"[7/8] Vinculos extracluster (ventana={ventana}, top_por_tema={top_por_tema})...")
    if excluir:
        print(f"         Terminos excluidos del calculo: {sorted(excluir)}")

    # Mapa termino -> tema_id (para saber a que cluster pertenece)
    termino_a_tema: dict[str, int] = {}
    for tema_id, terminos in terminos_por_tema.items():
        for t, _ in terminos:
            termino_a_tema[t] = tema_id

    # Para cada tema, sus terminos como set
    sets_por_tema: dict[int, set[str]] = {
        t: {term for term, _ in terminos}
        for t, terminos in terminos_por_tema.items()
    }

    # Para cada par de terminos coocurrentes, contamos en que cluster origen caen
    # estructura: dict[tema_id_origen] -> Counter{(interno, externo): count}
    resultado: dict[int, list[tuple[str, str, int]]] = {}
    # Primero acumulamos: para cada par (a, b) de la ventana,
    # si a es de tema T y b NO es de tema T -> registramos en T.
    # Esto evita duplicar A->B / B->A.
    from collections import defaultdict
    pares_por_tema: dict[int, Counter] = defaultdict(Counter)

    for d in docs:
        n = len(d)
        if n < 2:
            continue
        for i, ti in enumerate(d):
            if excluir and ti in excluir:
                continue
            tema_ti = termino_a_tema.get(ti)
            if tema_ti is None:
                continue
            # Ventana alrededor de i
            j_min = max(0, i - ventana)
            j_max = min(n, i + ventana + 1)
            for j in range(j_min, j_max):
                if j == i:
                    continue
                tj = d[j]
                if tj == ti:
                    continue
                if excluir and tj in excluir:
                    continue
                tema_tj = termino_a_tema.get(tj)
                # Excluir si ambos son del mismo cluster
                if tema_tj == tema_ti:
                    continue
                # Guardar (interno, externo) en el cluster del interno
                pares_por_tema[tema_ti][(ti, tj)] += 1

    for tema_id in terminos_por_tema:
        pares = pares_por_tema.get(tema_id, Counter())
        lista = [(a, b, c) for (a, b), c in pares.items() if c > 0]
        lista.sort(key=lambda x: -x[2])
        resultado[tema_id] = lista[:top_por_tema]
        print(f"         Tema {tema_id}: {len(resultado[tema_id])} vinculos externos (deduplicados)")

    return resultado


def matriz_vinculos_entre_temas(
    extra: dict[int, list[tuple[str, str, int]]],
    termino_a_tema: dict[str, int],
    K: int,
) -> tuple[list[list[int]], list[list[tuple[str, str, int]]]]:
    """
    Construye la matriz K x K de vinculos entre temas.

    matriz[i][j] = suma de pesos de coocurrencia de los terminos del
    cluster i con terminos que pertenecen al cluster j (excluyendo
    el caso i==j, que ya esta cubierto por intracluster).

    Ademas devuelve los top-3 pares (interno, externo, peso) por cada
    par (i, j) para inspeccion.
    """
    from collections import defaultdict
    # Por cada par (i, j), acumular los pesos
    pesos: dict[tuple[int, int], int] = defaultdict(int)
    pares: dict[tuple[int, int], list[tuple[str, str, int]]] = defaultdict(list)

    for tema_i, lista in extra.items():
        for interno, externo, peso in lista:
            tema_j = termino_a_tema.get(externo)
            if tema_j is None:
                # termino_externo no esta asignado a ningun tema: lo agrupamos
                # en una "categoria" -1 ("fuera de cluster")
                continue  # por ahora solo contamos entre-temas
            pesos[(tema_i, tema_j)] += peso
            pares[(tema_i, tema_j)].append((interno, externo, peso))

    matriz: list[list[int]] = [[0] * K for _ in range(K)]
    for (i, j), w in pesos.items():
        if 0 <= i < K and 0 <= j < K:
            matriz[i][j] = w

    top_pares: list[list[tuple[str, str, int]]] = [[] for _ in range(K * K)]
    for (i, j), lst in pares.items():
        idx = i * K + j
        lst_sorted = sorted(lst, key=lambda x: -x[2])[:3]
        top_pares[idx] = lst_sorted

    return matriz, top_pares


# ===========================================================================
# 8. Persistencia + reporte
# ===========================================================================

def guardar_resultados(
    out_dir: Path,
    barrido: list[dict],
    mejor: dict,
    lda_model,
    df_asig: pd.DataFrame,
    terminos_por_tema: dict[int, list[tuple[str, float]]],
    intra: dict[int, list[tuple[str, str, int]]],
    extra: dict[int, list[tuple[str, str, int]]],
    matriz_entre: list[list[int]] | None = None,
    top_pares_entre: list[list[tuple[str, str, int]]] | None = None,
    n_docs: int = 0,
) -> None:
    """Guarda todos los outputs en out_dir/."""
    out_dir.mkdir(parents=True, exist_ok=True)
    intra_dir = out_dir / "intracluster"
    extra_dir = out_dir / "extracluster"
    intra_dir.mkdir(exist_ok=True)
    extra_dir.mkdir(exist_ok=True)
    for old_path in [*intra_dir.glob("tema_*.csv"), *extra_dir.glob("tema_*.csv")]:
        old_path.unlink()

    # 1) Barrido
    pd.DataFrame(barrido).to_csv(out_dir / "lda_barrido.csv", index=False, encoding="utf-8")

    # 2) Mejor modelo (resumen)
    resumen_modelo = {
        "k": mejor["k"],
        "coherencia_c_v": mejor["coherencia_c_v"],
        "perplexity": mejor["perplexity"],
        "alpha": "auto",
        "eta": "auto",
        "passes": 10,
        "iterations": 100,
        "criterio_seleccion": mejor.get("criterio_seleccion", "maxima coherencia c_v"),
        "coherencia_maxima_barrido": mejor.get("coherencia_maxima_barrido"),
        "k_maxima_coherencia": mejor.get("k_maxima_coherencia"),
    }
    with open(out_dir / "lda_mejor_modelo.json", "w", encoding="utf-8") as f:
        json.dump(resumen_modelo, f, indent=2, ensure_ascii=False)

    # 3) Asignacion
    df_asig.to_csv(out_dir / "lda_asignacion.csv", index=False, encoding="utf-8")

    # 4) Resumen por tema
    rows = []
    K = mejor["k"]
    for tema_id in range(K):
        terminos = terminos_por_tema.get(tema_id, [])
        top_str = ", ".join(f"{t}({p:.3f})" for t, p in terminos[:20])
        rows.append({
            "tema_id": tema_id,
            "n_terminos": len(terminos),
            "top_20_terminos": top_str,
        })
    pd.DataFrame(rows).to_csv(out_dir / "temas_terminos.csv", index=False, encoding="utf-8")

    # 5) Intracluster por tema
    for tema_id, aristas in intra.items():
        pd.DataFrame(aristas, columns=["source", "target", "weight"]).to_csv(
            intra_dir / f"tema_{tema_id:02d}.csv", index=False, encoding="utf-8"
        )

    # 6) Extracluster por tema
    for tema_id, pares in extra.items():
        pd.DataFrame(pares, columns=["termino_interno", "termino_externo", "weight"]).to_csv(
            extra_dir / f"tema_{tema_id:02d}.csv", index=False, encoding="utf-8"
        )

    # 7) Resumen estructurado
    resumen = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "fase": "2 - modelado de temas con LDA",
        "corpus": {"n_docs": n_docs},
        "lda_barrido": barrido,
        "lda_mejor": resumen_modelo,
        "temas": {
            tema_id: {
                "n_terminos": len(terminos_por_tema[tema_id]),
                "n_aristas_internas": len(intra.get(tema_id, [])),
                "n_vinculos_externos": len(extra.get(tema_id, [])),
                "top_10_terminos": [t for t, _ in terminos_por_tema[tema_id][:10]],
            }
            for tema_id in range(K)
        },
    }
    if matriz_entre is not None:
        resumen["matriz_entre_temas"] = matriz_entre
    with open(out_dir / "resumen_fase2_lda.json", "w", encoding="utf-8") as f:
        json.dump(resumen, f, indent=2, ensure_ascii=False)

    # 8) Matriz entre temas (CSV legible)
    if matriz_entre is not None:
        df_mat = pd.DataFrame(
            matriz_entre,
            index=[f"T{i}" for i in range(K)],
            columns=[f"T{i}" for i in range(K)],
        )
        df_mat.to_csv(out_dir / "matriz_entre_temas.csv", encoding="utf-8")
        # Top pares por (i, j) en un solo CSV
        rows = []
        for i in range(K):
            for j in range(K):
                if i == j:
                    continue
                idx = i * K + j
                for interno, externo, peso in (top_pares_entre or [[]] * (K * K))[idx]:
                    rows.append({
                        "de_tema": i, "a_tema": j,
                        "termino_interno": interno, "termino_externo": externo,
                        "weight": peso,
                    })
        if rows:
            pd.DataFrame(rows).to_csv(
                out_dir / "matriz_entre_temas_top.csv",
                index=False, encoding="utf-8",
            )


def generar_reporte(
    out_dir: Path,
    barrido: list[dict],
    mejor: dict,
    terminos_por_tema: dict[int, list[tuple[str, float]]],
    intra: dict[int, list[tuple[str, str, int]]],
    extra: dict[int, list[tuple[str, str, int]]],
    matriz_entre: list[list[int]] | None = None,
    top_pares_entre: list[list[tuple[str, str, int]]] | None = None,
) -> None:
    """Genera reporte legible en markdown."""
    lines = [
        "# Reporte SNA - Fase 2: Modelado de Temas (LDA)",
        "",
        f"_Generado: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}_",
        "",
        "## 1. Barrido de K",
        "",
        "| K | Coherencia c_v | Perplexity |",
        "|---|---------------:|-----------:|",
    ]
    for r in barrido:
        lines.append(
            f"| {r['k']} | {r['coherencia_c_v']:.4f} | {r['perplexity']:.2f} |"
        )
    lines.append("")
    lines.append(f"**K seleccionado: {mejor['k']}** (c_v = {mejor['coherencia_c_v']:.4f})")
    lines.append("")
    lines.append(f"**Criterio:** {mejor.get('criterio_seleccion', 'maxima coherencia c_v')}")
    lines.append("")

    K = mejor["k"]
    lines.append("## 2. Temas descubiertos")
    lines.append("")
    for tema_id in range(K):
        terminos = terminos_por_tema.get(tema_id, [])
        top_10 = ", ".join(t for t, _ in terminos[:10])
        n_intra = len(intra.get(tema_id, []))
        n_extra = len(extra.get(tema_id, []))
        lines.append(f"### Tema {tema_id} ({len(terminos)} terminos)")
        lines.append("")
        lines.append(f"**Top 10:** {top_10}")
        lines.append("")
        lines.append(f"- Aristas internas (coocurrencia ventana=3): **{n_intra}**")
        lines.append(f"- Vinculos externos (coocurrencia ventana=12): **{n_extra}**")
        lines.append("")

    lines.append("---")
    lines.append("")
    lines.append("**Archivos generados:**")
    lines.append("- `lda_barrido.csv` - resultados del barrido de K configurado")
    lines.append("- `lda_mejor_modelo.json` - metadata del modelo optimo")
    lines.append("- `lda_asignacion.csv` - termino, tema_id, peso")
    lines.append("- `temas_terminos.csv` - top 20 terminos por tema")
    lines.append("- `intracluster/tema_XX.csv` - coocurrencias internas (ventana=3)")
    lines.append("- `extracluster/tema_XX.csv` - vinculos externos (ventana=12)")
    lines.append("- `matriz_entre_temas.csv` - matriz KxK de vinculos entre temas")
    lines.append("- `matriz_entre_temas_top.csv` - top-3 pares por par de temas")
    lines.append("- `resumen_fase2_lda.json` - resumen estructurado")

    # Seccion 3: Matriz entre temas
    if matriz_entre is not None:
        lines.append("")
        lines.append("## 3. Matriz de vinculos entre temas (K x K)")
        lines.append("")
        lines.append("Cada celda `M[i][j]` = suma de coocurrencias de terminos del tema i "
                     "con terminos del tema j (ventana=12, deduplicada).")
        lines.append("")
        lines.append("| De \\\\ Hacia | " + " | ".join(f"T{j}" for j in range(K)) + " |")
        lines.append("|" + "|".join(["---"] * (K + 1)) + "|")
        for i in range(K):
            fila = " | ".join(str(matriz_entre[i][j]) for j in range(K))
            lines.append(f"| T{i} | {fila} |")
        lines.append("")

    with open(out_dir / "reporte_fase2_lda.md", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


# ===========================================================================
# CLI
# ===========================================================================

def parsear_fecha(fecha_str: str) -> str:
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(fecha_str, fmt).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            continue
    return fecha_str[:10] if fecha_str and len(fecha_str) >= 10 else ""


def main() -> None:
    parser = argparse.ArgumentParser(
        description="SNA Fase 2: modelado de temas con LDA + cohesion intracluster"
    )
    parser.add_argument("--input-csv", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--input", choices=["csv", "txt"], default="csv",
        help="Tipo de entrada: csv (default) o txt",
    )
    parser.add_argument(
        "--txt-path", required=False, default=None,
        help="Ruta al archivo .txt (solo si --input txt)",
    )
    parser.add_argument("--k-min", "--K-min", dest="k_min", type=int, default=25)
    parser.add_argument("--k-max", "--K-max", dest="k_max", type=int, default=35)
    parser.add_argument(
        "--selection-mode", choices=["informative", "coherence"], default="coherence",
        help=(
            "informative elige el mayor K dentro de la tolerancia de coherencia; "
            "coherence elige exclusivamente la mayor c_v"
        ),
    )
    parser.add_argument(
        "--coherence-ratio", type=float, default=0.90,
        help="Fraccion de la mejor coherencia aceptable en modo informative",
    )
    parser.add_argument("--passes", type=int, default=10)
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--no-below", type=int, default=5)
    parser.add_argument("--no-above", type=float, default=0.6)
    parser.add_argument(
        "--diccionario-espanol",
        type=Path,
        default=DEFAULT_SPANISH_DICTIONARY,
        help="Diccionario Hunspell usado para conservar documentos en español.",
    )
    parser.add_argument(
        "--diccionario-ingles",
        type=Path,
        default=DEFAULT_ENGLISH_DICTIONARY,
        help="Diccionario inglés usado para detectar documentos no españoles.",
    )
    parser.add_argument(
        "--min-spanish-share",
        type=float,
        default=DEFAULT_MIN_SPANISH_SHARE,
        help="Proporción mínima de lemas españoles por documento.",
    )
    parser.add_argument("--ventana-intra", type=int, default=3)
    parser.add_argument("--ventana-extra", type=int, default=12)
    parser.add_argument(
        "--max-frecuencia-contexto", type=int, default=5,
        help="Maximas repeticiones de un lema dentro de cada conversacion",
    )
    parser.add_argument(
        "--max-lemas-contexto", type=int, default=500,
        help="Tamano maximo del documento agregado por conversacion",
    )
    parser.add_argument(
        "--excluir-terminos",
        type=str,
        default="",
        help="Terminos a excluir del calculo de extracluster y matriz. "
             "Separados por coma, sin acentos (ej: gobierno,mexico,cdmx).",
    )
    parser.add_argument(
        "--stop-extra",
        type=str,
        default="",
        help="Terminos extra a agregar a la stoplist SOLO para esta corrida "
             "(se concatenan a la stoplist del repo sin modificarla). "
             "Estos terminos NO apareceran en los top-terminos LDA. "
             "Separados por coma (ej: gobierno,mexico).",
    )
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    if args.k_min < 2 or args.k_max < args.k_min:
        parser.error("El rango K debe cumplir 2 <= k-min <= k-max")
    if not 0.0 < args.coherence_ratio <= 1.0:
        parser.error("--coherence-ratio debe estar en (0, 1]")
    if not 0.0 < args.min_spanish_share <= 1.0:
        parser.error("--min-spanish-share debe estar en (0, 1]")

    out_dir = args.output_dir if args.input == "csv" else args.output_dir.parent / "clusters_txt"

    print("=" * 70)
    print("[SNA Fase 2 - LDA] historico de Tampico")
    print(f"  Entrada: {args.input} -> {args.txt_path if args.input == 'txt' else args.input_csv}")
    print(f"  Salida:  {out_dir}")
    print("=" * 70)

    # 1) Cargar corpus
    if args.input == "txt":
        if not args.txt_path:
            raise SystemExit("--txt-path es obligatorio si --input txt")
        textos = cargar_texto_plano(Path(args.txt_path))
    else:
        df = cargar_corpus(args.input_csv)
        print(f"         Documentos: {len(df)}")

        if "texto_limpio" in df.columns:
            textos = df["texto_limpio"].fillna("").astype(str).tolist()
        elif "texto_original" in df.columns:
            textos = df["texto_original"].fillna("").astype(str).tolist()
        else:
            raise SystemExit("No se encontro columna de texto")

    # 2) Tokenizar y lematizar (con stopwords extra si las hay)
    stop_extra_set: set[str] = set()
    if args.stop_extra:
        stop_extra_set = {t.strip() for t in args.stop_extra.split(",") if t.strip()}
    lemas_mensajes = normalizar_lemas(
        lematizar_textos(textos, stopwords_extra=stop_extra_set)
    )
    docs_vacios = sum(1 for d in lemas_mensajes if not d)
    print(f"         Documentos sin lemas: {docs_vacios}/{len(lemas_mensajes)}")

    if args.input == "csv":
        spanish_vocabulary = load_language_vocabulary(
            str(args.diccionario_espanol)
        )
        english_vocabulary = load_language_vocabulary(
            str(args.diccionario_ingles)
        )
        language_hints = (
            df["idioma_detectado"].fillna("").astype(str).tolist()
            if "idioma_detectado" in df.columns
            else [""] * len(df)
        )
        language_mask = [
            is_spanish_message(
                lemmas,
                spanish_vocabulary,
                english_vocabulary,
                language_hint=hint,
                min_spanish_share=args.min_spanish_share,
            )
            for lemmas, hint in zip(
                lemas_mensajes, language_hints, strict=True
            )
        ]
        docs_before_language = len(df)
        df = df.loc[language_mask].reset_index(drop=True)
        lemas_mensajes = [
            lemmas
            for lemmas, keep in zip(
                lemas_mensajes, language_mask, strict=True
            )
            if keep
        ]
        print(
            "         Filtro de español: "
            f"{len(df)}/{docs_before_language} documentos conservados"
        )

    corpus_modelado_df: pd.DataFrame | None = None
    if args.input == "csv":
        lemas_docs, corpus_modelado_df = preparar_corpus_modelado(
            df,
            lemas_mensajes,
            max_frecuencia_contexto=args.max_frecuencia_contexto,
            max_lemas_contexto=args.max_lemas_contexto,
        )
    else:
        lemas_docs = lemas_mensajes

    # 3) Diccionario + corpus
    dictionary, corpus = construir_diccionario_y_corpus(
        lemas_docs, no_below=args.no_below, no_above=args.no_above
    )

    # 4) Barrido LDA
    barrido, mejor, lda_model = barrido_lda(
        corpus, dictionary, lemas_docs,
        k_min=args.k_min, k_max=args.k_max,
        passes=args.passes, iterations=args.iterations,
        seed=args.seed,
        selection_mode=args.selection_mode,
        coherence_ratio=args.coherence_ratio,
    )

    # 5) Asignacion hard
    df_asig, terminos_por_tema = asignar_terminos_a_temas(lda_model, dictionary)

    # 6) Cohesion intracluster
    intra = coocurrencia_intracluster(
        lemas_docs, terminos_por_tema, ventana=args.ventana_intra
    )

    # 7) Vinculos extracluster
    excluir_set: set[str] = set()
    if args.excluir_terminos:
        excluir_set = {t.strip() for t in args.excluir_terminos.split(",") if t.strip()}
    extra = coocurrencia_extracluster(
        lemas_docs, terminos_por_tema,
        ventana=args.ventana_extra, top_por_tema=200,
        excluir=excluir_set,
    )

    # 7b) Matriz entre temas: K x K con pesos agregados de coocurrencia
    K = mejor["k"]
    termino_a_tema: dict[str, int] = {}
    for tema_id, terminos in terminos_por_tema.items():
        for t, _ in terminos:
            termino_a_tema[t] = tema_id
    matriz_entre, top_pares_entre = matriz_vinculos_entre_temas(
        extra, termino_a_tema, K
    )
    print(f"         Matriz entre temas ({K}x{K}) calculada")

    # 8) Guardar
    guardar_resultados(
        out_dir, barrido, mejor, lda_model,
        df_asig, terminos_por_tema, intra, extra,
        matriz_entre=matriz_entre, top_pares_entre=top_pares_entre,
        n_docs=len(lemas_docs),
    )
    generar_reporte(
        out_dir, barrido, mejor, terminos_por_tema, intra, extra,
        matriz_entre=matriz_entre, top_pares_entre=top_pares_entre,
    )

    if args.input == "csv":
        cache = pd.DataFrame({
            "documento_id": df["id"] if "id" in df.columns else df.index.astype(str),
            "plataforma": df.get("plataforma", ""),
            "usuario": df.get("usuario", ""),
            "tipo_registro": df.get("tipo_registro", ""),
            "lemas": [" ".join(lemas) for lemas in lemas_mensajes],
        })
        cache.to_csv(out_dir / "documentos_lematizados.csv", index=False, encoding="utf-8")
        if corpus_modelado_df is not None:
            corpus_modelado_df.to_csv(
                out_dir / "corpus_modelado.csv", index=False, encoding="utf-8"
            )

    print("=" * 70)
    print(f"[SNA Fase 2 - LDA] OK resultados en {out_dir}")
    print(f"           reporte: {out_dir / 'reporte_fase2_lda.md'}")
    print("=" * 70)


if __name__ == "__main__":
    main()
