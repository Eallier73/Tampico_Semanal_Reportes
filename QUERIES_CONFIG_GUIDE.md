# Configuración Centralizada de Queries

Este documento explica cómo gestionar las queries y parámetros de los extractores desde un único lugar.

## 📍 Ubicación

El archivo de configuración se encuentra en:
```
Scripts/queries_config.py
```

## 📋 Contenido

Actualmente contiene configuración para:

### 1. **YouTube** (`YOUTUBE_*`)
- `YOUTUBE_CHANNELS`: Lista de canales a monitorear
- `YOUTUBE_SEARCH_QUERIES`: Queries de búsqueda
- `YOUTUBE_DEFAULT_MAX_VIDEOS_QUERY`: Máximo videos por query
- `YOUTUBE_DEFAULT_MAX_VIDEOS_CHANNEL`: Máximo videos por canal

**Cambiar:**
```python
YOUTUBE_CHANNELS = [
    "monicavtampico",
    "otro_canal",  # Agregar aquí
]

YOUTUBE_SEARCH_QUERIES = [
    "mi nueva query aqui",
]
```

### 2. **Twitter/X** (`TWITTER_*`)
- `TWITTER_SEARCH_QUERIES`: Queries de búsqueda
- `TWITTER_DEFAULT_MAX_TWEETS`: Máximo tweets
- `TWITTER_DEFAULT_MAX_REPLIES_PER_TWEET`: Máximo respuestas por tweet
- `TWITTER_DEFAULT_MAX_REPLY_SCROLLS`: Máximo scrolls

**Cambiar:**
```python
TWITTER_SEARCH_QUERIES = [
    "nueva query",
    "otra query",
]
```

### 3. **Medios** (`MEDIOS_*`)
- `MEDIOS_SITES`: Sitios a monitorear
- `MEDIOS_SEARCH_TERMS`: Términos de búsqueda
- `MEDIOS_DEFAULT_MODE_QUERIES`: Modo de queries (compacto/combinado)
- Pausas entre requests

**Cambiar:**
```python
MEDIOS_SITES = [
    "site:nuevositio.com.mx",
]

MEDIOS_SEARCH_TERMS = [
    '"nuevo termino"',
]
```

### 4. **Facebook** (`FACEBOOK_*`)
- `FACEBOOK_PAGES`: Páginas a monitorear
- `FACEBOOK_COMMENTS_DEFAULT_MAX_COMMENTS`: Máximo comentarios
- `FACEBOOK_POSTS_DEFAULT_MAX_POSTS`: Máximo posts

**Cambiar:**
```python
FACEBOOK_PAGES = [
    "nueva_pagina",
    "otra_pagina",
]
```

## 🔗 Cómo usar en los extractores

### Opción 1: Importar en los scripts (Recomendado)

```python
# En el script extractor (ej: 1_extractors_youtube.py)
from queries_config import YOUTUBE_SEARCH_QUERIES, YOUTUBE_CHANNELS

DEFAULT_YOUTUBE_CHANNELS = YOUTUBE_CHANNELS
DEFAULT_YOUTUBE_QUERIES = YOUTUBE_SEARCH_QUERIES
```

### Opción 2: Ver configuración actual

Ejecutar directamente para ver toda la configuración en formato JSON:
```bash
python Scripts/queries_config.py
```

Salida:
```json
{
  "youtube": {
    "channels": ["monicavtampico"],
    "search_queries": ["presidenta municipal de Tampico", ...],
    ...
  },
  ...
}
```

## 🎯 Flujo de cambio recomendado

1. **Identificar qué cambiar** en `queries_config.py`
2. **Modificar el valor** correspondiente
3. **Guardar** el archivo
4. **Actualizar** los scripts para que importen de `queries_config.py` (si aún no lo hacen)
5. **Probar** el cambio

## 📝 Ejemplo práctico

### Agregar un nuevo sitio de medios:

```python
# Antes:
MEDIOS_SITES = [
    "site:oem.com.mx",
    "site:milenio.com",
]

# Después:
MEDIOS_SITES = [
    "site:oem.com.mx",
    "site:milenio.com",
    "site:nuevositio.com.mx",  # ⬅️ Agregado
]
```

### Cambiar términos de búsqueda:

```python
# Antes:
MEDIOS_SEARCH_TERMS = [
    '"Monica Villarreal"',
    '"gobierno de tampico"',
    '"tampico"',
]

# Después:
MEDIOS_SEARCH_TERMS = [
    '"Monica Villarreal"',
    '"gobierno de tampico"',
    '"tampico"',
    '"nueva palabra clave"',  # ⬅️ Agregado
]
```

## 🔄 Funciones auxiliares

El archivo proporciona funciones para obtener toda la configuración:

```python
from queries_config import get_youtube_config, get_twitter_config, get_medios_config, get_all_config

# Obtener config de YouTube
yt_config = get_youtube_config()

# Obtener config de todos los extractores
all_config = get_all_config()
```

## ⚙️ Migración gradual de scripts

Los extractores pueden migrar gradualmente a usar `queries_config.py`:

1. ✅ `00_orquestador_general.py` - Puede importar de aquí
2. ⏳ `1_extractors_youtube.py` - Por migrar
3. ⏳ `2_extractors_twitter.py` - Por migrar
4. ⏳ `3_extractors_medios.py` - Por migrar
5. ⏳ `4_extractors_facebook_comentarios.py` - Por migrar
6. ⏳ `5_extractors_facebook_posts.py` - Por migrar

## 📌 Notas

- El archivo está en Python para facilitar importaciones en otros scripts
- Todos los valores son facilmente editables
- No requiere herramientas especiales para cambiar
- Los cambios se aplican inmediatamente a los scripts que importan de aquí
- Se recomienda versionarlo en git para tracking de cambios
