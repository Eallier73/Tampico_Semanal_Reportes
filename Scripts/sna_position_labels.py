#!/usr/bin/env python3
"""Titulos legibles para posiciones discursivas a partir de sus palabras."""

from __future__ import annotations

import unicodedata


def _normalize(value: str) -> str:
    raw = unicodedata.normalize("NFKD", str(value).strip().lower())
    return "".join(char for char in raw if not unicodedata.combining(char))


LABEL_RULES: list[tuple[str, set[str], int]] = [
    (
        "Felicitaciones y expresiones de afecto",
        {"feliz", "felicidad", "felicidades", "bendicion", "bendiga", "querido", "abrazo", "amor", "carino", "gracias", "saludo", "saludos", "orgullo"},
        2,
    ),
    (
        "Respaldo y defensa de la gestión",
        {"apoyo", "apoyar", "respaldo", "respaldar", "defensa", "defender", "excelente", "adelante", "compromiso", "transformacion"},
        1,
    ),
    (
        "Críticas y acusaciones de corrupción",
        {"corrupto", "corrupta", "corrupcion", "ratero", "ratera", "ladron", "ladrona", "robo", "mentira", "mentiroso", "narco", "criminal", "verguenza"},
        1,
    ),
    (
        "Reclamos de seguridad y justicia",
        {"seguridad", "justicia", "delito", "violencia", "victima", "denuncia", "fiscalia", "policia", "desaparecido", "responsable", "miedo"},
        2,
    ),
    (
        "Quejas y solicitudes sobre servicios públicos",
        {"agua", "basura", "alumbrado", "calle", "bache", "drenaje", "comapa", "recoleccion", "vialidad", "servicio", "colonia", "avenida"},
        2,
    ),
    (
        "Salud, atención médica y asistencia",
        {"salud", "hospital", "medico", "atencion", "ayuda", "enfermedad", "clinica", "paciente", "rescate", "auxilio"},
        2,
    ),
    (
        "Obras, vivienda y desarrollo urbano",
        {"obra", "infraestructura", "proyecto", "vivienda", "infonavit", "puente", "construir", "desarrollo", "urbano", "inversion"},
        2,
    ),
    (
        "Actividades culturales y eventos públicos",
        {"cultura", "cultural", "evento", "concierto", "musica", "arte", "museo", "exposicion", "festival", "tradicion"},
        2,
    ),
    (
        "Deporte, competencia y resultados",
        {"deporte", "deportivo", "futbol", "equipo", "partido", "liga", "campeon", "torneo", "medalla", "seleccion"},
        2,
    ),
    (
        "Clima, lluvias y prevención de riesgos",
        {"clima", "lluvia", "calor", "temperatura", "tormenta", "inundacion", "riesgo", "emergencia", "proteccion", "alerta"},
        2,
    ),
    (
        "Debate político y partidista",
        {"gobierno", "morena", "pan", "pri", "presidente", "presidenta", "alcalde", "alcaldesa", "politico", "eleccion", "voto"},
        2,
    ),
    (
        "Noticias, columnas y agenda informativa",
        {"noticia", "noticias", "informativo", "columna", "articulo", "resumen", "prensa", "medio", "television", "publicar"},
        2,
    ),
    (
        "Familia, cuidados y vida comunitaria",
        {"familia", "madre", "padre", "mama", "papa", "hijo", "hija", "nino", "nina", "joven", "comunidad"},
        2,
    ),
    (
        "Economía, pagos y actividad comercial",
        {"dinero", "pago", "pesos", "millon", "comercio", "venta", "empresa", "negocio", "impuesto", "precio", "banco"},
        2,
    ),
    (
        "Expresiones religiosas y de fe",
        {"amen", "virgen", "santo", "jesus", "jesucristo", "misa", "iglesia", "bendito"},
        2,
    ),
]


def classify_position_name(words: list[str], topic_title: str = "") -> str:
    normalized = [_normalize(word) for word in words if str(word).strip()]
    leading = set(normalized[:10])
    for title, vocabulary, minimum in LABEL_RULES:
        if len(leading & vocabulary) >= minimum:
            return title

    useful = [word for word in normalized if word][:3]
    if not useful:
        return "Conversación sin suficientes palabras distintivas"
    subject = str(topic_title).strip()
    if subject:
        emphasis = useful[0]
        return f"Perspectiva sobre {subject.lower()}, con énfasis en {emphasis}"
    if len(useful) == 1:
        return f"Conversación sobre {useful[0]}"
    return "Conversación sobre " + ", ".join(useful[:-1]) + f" y {useful[-1]}"
