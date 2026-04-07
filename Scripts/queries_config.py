"""
================================================================================
CONFIGURACIÓN CENTRALIZADA DE QUERIES Y PARÁMETROS
================================================================================

Este archivo contiene todas las queries, canales, términos y parámetros
utilizados por los extractores. Cambiar aquí las consultas afecta a todos
los scripts que las importan.

Estructura:
- YOUTUBE: canales y queries de búsqueda
- TWITTER: queries de búsqueda
- MEDIOS: sitios y términos de búsqueda
- FACEBOOK: páginas target

================================================================================
"""

from __future__ import annotations

# ============================================================================
# YOUTUBE
# ============================================================================

YOUTUBE_CHANNELS = [
    "monicavtampico",
]
"""Canales de YouTube a monitorear (sin @)"""

YOUTUBE_SEARCH_QUERIES = [
    "presidenta municipal de Tampico",
    "Presidenta municipal de Tampico",
    "Gobierno de Tampico",
    "gobierno de Tampico",
]
"""Consultas para buscar videos en YouTube"""

YOUTUBE_DEFAULT_MAX_VIDEOS_QUERY = 200
"""Máximo de videos por query de búsqueda"""

YOUTUBE_DEFAULT_MAX_VIDEOS_CHANNEL = 300
"""Máximo de videos por canal"""


# ============================================================================
# TWITTER / X
# ============================================================================

TWITTER_SEARCH_QUERIES = [
    "to:MonicaVTampico",
    "from:MonicaVTampico",
    "to:TampicoGob",
    "from:TampicoGob",
    "@TampicoGob",
    "@MonicaVTampico",
    "monica villarreal",
    "gobierno de tampico",
    "tampico",
]
"""Queries para buscar tweets en Twitter/X"""

TWITTER_DEFAULT_MAX_TWEETS = 3000
"""Máximo de tweets por query"""

TWITTER_DEFAULT_MAX_REPLIES_PER_TWEET = 200
"""Máximo de respuestas a extraer por tweet"""

TWITTER_DEFAULT_MAX_REPLY_SCROLLS = 8
"""Máximo de scrolls para cargar respuestas"""


# ============================================================================
# MEDIOS (Google News RSS)
# ============================================================================

MEDIOS_SITES = [
    "site:oem.com.mx",
    "site:milenio.com",
]
"""Sitios de medios a monitorear para noticias"""

MEDIOS_SEARCH_TERMS = [
    '"Monica Villarreal"',
    '"gobierno de tampico"',
    '"tampico"',
]
"""Términos de búsqueda para noticias"""

MEDIOS_DEFAULT_MODE_QUERIES = "combinado"
"""Modo de construcción de queries: 'compacto' o 'combinado'"""

MEDIOS_DEFAULT_PAUSE_BETWEEN_REQUESTS = 2.0
"""Pausa en segundos entre requests de trafilatura"""

MEDIOS_DEFAULT_PAUSE_BETWEEN_RSS_QUERIES = 3.0
"""Pausa en segundos entre queries RSS"""


# ============================================================================
# FACEBOOK
# ============================================================================

FACEBOOK_PAGES = [
    "TampicoGob",
    "monicavtampico",
]
"""Páginas de Facebook a monitorear (handles o URLs)"""

FACEBOOK_COMMENTS_DEFAULT_MAX_COMMENTS = 200
"""Máximo de comentarios por post en Facebook"""

FACEBOOK_COMMENTS_DEFAULT_BATCH_SIZE = 25
"""Tamaño de batch para la descarga de comentarios"""

FACEBOOK_POSTS_DEFAULT_MAX_POSTS = 100
"""Máximo de posts por página"""

FACEBOOK_POSTS_DEFAULT_BATCH_SIZE = 10
"""Tamaño de batch para descarga de posts"""


# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================

def get_youtube_config() -> dict:
    """Retorna configuración completa de YouTube"""
    return {
        "channels": YOUTUBE_CHANNELS,
        "search_queries": YOUTUBE_SEARCH_QUERIES,
        "max_videos_query": YOUTUBE_DEFAULT_MAX_VIDEOS_QUERY,
        "max_videos_channel": YOUTUBE_DEFAULT_MAX_VIDEOS_CHANNEL,
    }


def get_twitter_config() -> dict:
    """Retorna configuración completa de Twitter"""
    return {
        "search_queries": TWITTER_SEARCH_QUERIES,
        "max_tweets": TWITTER_DEFAULT_MAX_TWEETS,
        "max_replies_per_tweet": TWITTER_DEFAULT_MAX_REPLIES_PER_TWEET,
        "max_reply_scrolls": TWITTER_DEFAULT_MAX_REPLY_SCROLLS,
    }


def get_medios_config() -> dict:
    """Retorna configuración completa de Medios"""
    return {
        "sites": MEDIOS_SITES,
        "search_terms": MEDIOS_SEARCH_TERMS,
        "mode_queries": MEDIOS_DEFAULT_MODE_QUERIES,
        "pause_requests": MEDIOS_DEFAULT_PAUSE_BETWEEN_REQUESTS,
        "pause_rss_queries": MEDIOS_DEFAULT_PAUSE_BETWEEN_RSS_QUERIES,
    }


def get_facebook_config() -> dict:
    """Retorna configuración completa de Facebook"""
    return {
        "pages": FACEBOOK_PAGES,
        "comments_max_comments": FACEBOOK_COMMENTS_DEFAULT_MAX_COMMENTS,
        "comments_batch_size": FACEBOOK_COMMENTS_DEFAULT_BATCH_SIZE,
        "posts_max_posts": FACEBOOK_POSTS_DEFAULT_MAX_POSTS,
        "posts_batch_size": FACEBOOK_POSTS_DEFAULT_BATCH_SIZE,
    }


def get_all_config() -> dict:
    """Retorna configuración completa de todos los extractores"""
    return {
        "youtube": get_youtube_config(),
        "twitter": get_twitter_config(),
        "medios": get_medios_config(),
        "facebook": get_facebook_config(),
    }


if __name__ == "__main__":
    # Mostrar configuración cuando se ejecuta directamente
    import json
    config = get_all_config()
    print(json.dumps(config, indent=2, ensure_ascii=False))
