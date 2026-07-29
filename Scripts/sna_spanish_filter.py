#!/usr/bin/env python3
"""Filtro reproducible de español para los corpus del SNA."""

from __future__ import annotations

import re
import unicodedata
from functools import lru_cache
from pathlib import Path
from typing import Any


DEFAULT_SPANISH_DICTIONARY = Path("/usr/share/hunspell/es_ES.dic")
DEFAULT_ENGLISH_DICTIONARY = Path("/usr/share/dict/american-english")
DEFAULT_MIN_SPANISH_SHARE = 0.40

NOISE_WORDS = {
    "quot", "amp", "etc", "bla", "http", "https", "www", "com", "nan",
    "rt", "htt", "youtu", "youtube", "facebook", "twitter",
}
SPANISH_LOCAL_TERMS = {
    "altamira", "amlo", "americo", "americovillarreal", "brugada", "cdmx",
    "chucho", "chuchonader", "ciudadmadero", "conagua", "dif", "edomex",
    "gobtam", "imss", "inegi", "jaibo", "jaibos", "jesusnader", "madero",
    "mexico", "monica", "monicavillarreal", "morena", "nader", "nasrallah",
    "prian", "tamaulipas", "tampico", "tampicogob", "tampicotecuida",
    "tampicovacontodo", "unam", "villarreal", "zonaconurbada",
}
ENGLISH_FALSE_FRIENDS = {
    "dean", "end", "home", "ice", "man", "mass", "name", "show", "single",
    "tell", "tour", "true", "world",
}
TOKEN_RE = re.compile(r"[a-záéíóúüñ]{3,}", re.IGNORECASE)


def normalize_language_word(value: Any) -> str:
    normalized = unicodedata.normalize("NFKD", str(value).strip().lower())
    return "".join(char for char in normalized if not unicodedata.combining(char))


@lru_cache(maxsize=4)
def load_language_vocabulary(dictionary_path: str) -> frozenset[str]:
    """Carga un diccionario normalizado y descarta banderas de Hunspell."""
    path = Path(dictionary_path)
    if not path.is_file():
        raise FileNotFoundError(
            f"No existe el diccionario de idioma requerido: {path}"
        )
    words: set[str] = set()
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            word = line.strip().split("/", 1)[0]
            key = normalize_language_word(word)
            if key:
                words.add(key)
    return frozenset(words)


def spanish_root_candidates(key: str) -> set[str]:
    """Genera bases simples para plurales que Hunspell guarda sin expandir."""
    candidates = {key}
    if len(key) > 3 and key.endswith("s"):
        candidates.add(key[:-1])
    if len(key) > 4 and key.endswith("es"):
        candidates.add(key[:-2])
    if len(key) > 4 and key.endswith("ces"):
        candidates.add(f"{key[:-3]}z")
    return candidates


def is_spanish_word(
    value: Any,
    spanish_vocabulary: frozenset[str],
    english_vocabulary: frozenset[str],
) -> bool:
    """Acepta solo vocabulario español conocido o excepciones locales."""
    key = normalize_language_word(value)
    if (
        not key
        or key in NOISE_WORDS
        or key in ENGLISH_FALSE_FRIENDS
        or not re.fullmatch(r"[a-zñ]+", key)
    ):
        return False
    if key in SPANISH_LOCAL_TERMS:
        return True
    return bool(spanish_root_candidates(key) & spanish_vocabulary)


def spanish_message_stats(
    lemmas: Any,
    spanish_vocabulary: frozenset[str],
    english_vocabulary: frozenset[str],
) -> dict[str, float | int]:
    """Mide evidencia española, inglesa y desconocida en lemas de un mensaje."""
    tokens = [
        normalize_language_word(token)
        for token in TOKEN_RE.findall(str(lemmas or ""))
    ]
    tokens = [token for token in tokens if token and token not in NOISE_WORDS]
    spanish = 0
    english = 0
    for token in tokens:
        if is_spanish_word(token, spanish_vocabulary, english_vocabulary):
            spanish += 1
        elif (
            token in ENGLISH_FALSE_FRIENDS
            or bool(spanish_root_candidates(token) & english_vocabulary)
        ):
            english += 1
    total = len(tokens)
    return {
        "total": total,
        "spanish": spanish,
        "english": english,
        "unknown": max(0, total - spanish - english),
        "spanish_share": spanish / total if total else 0.0,
    }


def is_spanish_message(
    lemmas: Any,
    spanish_vocabulary: frozenset[str],
    english_vocabulary: frozenset[str],
    *,
    language_hint: Any = "",
    min_spanish_share: float = DEFAULT_MIN_SPANISH_SHARE,
) -> bool:
    """Decide si un mensaje aporta evidencia suficiente de estar en español."""
    hint = normalize_language_word(language_hint)
    if hint and hint not in {"indeterminado", "desconocido", "unknown", "nan"}:
        if hint not in {"es", "spa", "spanish", "espanol"}:
            return False

    stats = spanish_message_stats(
        lemmas, spanish_vocabulary, english_vocabulary
    )
    total = int(stats["total"])
    spanish = int(stats["spanish"])
    english = int(stats["english"])
    if total == 0 or spanish == 0:
        return False
    if total <= 2:
        return spanish == total and english == 0
    return (
        float(stats["spanish_share"]) >= min_spanish_share
        and spanish >= english
    )
