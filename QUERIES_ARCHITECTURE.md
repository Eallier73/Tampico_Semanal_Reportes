# Centralizacion de Queries - Mapa Visual

Esta es una guía visual de dónde se encuentran todas las queries y cómo se conectan los archivos.

## 📦 Arquitectura

```
Tampico_Semanal_Reportes/
│
├── Scripts/
│   ├── 00_orquestador_general.py  ← Importa de queries_config.py
│   │
│   ├── queries_config.py           ← ✨ ARCHIVO CENTRAL DE QUERIES ✨
│   │   ├── YOUTUBE_*
│   │   ├── TWITTER_*
│   │   ├── MEDIOS_*
│   │   └── FACEBOOK_*
│   │
│   ├── 1_extractors_youtube.py     ← (Por actualizar para usar queries_config)
│   ├── 2_extractors_twitter.py     ← (Por actualizar para usar queries_config)
│   ├── 3_extractors_medios.py      ← (Por actualizar para usar queries_config)
│   ├── 4_extractors_facebook_comentarios.py  ← (Por actualizar)
│   └── 5_extractors_facebook_posts.py        ← (Por actualizar)
│
└── QUERIES_CONFIG_GUIDE.md         ← Documentación de uso
```

## 🔄 Flujo de datos

```
Usuario modifica queries_config.py
            ↓
00_orquestador_general.py importa cambios
            ↓
Orquestador construye comandos con queries actualizadas
            ↓
Extractores se ejecutan con nuevas queries
            ↓
Datos se descargan y procesan
```

## 📋 Tabla de Contenidos

| Extractor | Archivo | Queries en queries_config.py | Estado |
|-----------|---------|------------------------------|--------|
| **YouTube** | `1_extractors_youtube.py` | `YOUTUBE_CHANNELS`, `YOUTUBE_SEARCH_QUERIES` | ⏳ Por actualizar |
| **Twitter** | `2_extractors_twitter.py` | `TWITTER_SEARCH_QUERIES` | ⏳ Por actualizar |
| **Medios** | `3_extractors_medios.py` | `MEDIOS_SITES`, `MEDIOS_SEARCH_TERMS` | ⏳ Por actualizar |
| **Facebook Comentarios** | `4_extractors_facebook_comentarios.py` | `FACEBOOK_PAGES` | ⏳ Por actualizar |
| **Facebook Posts** | `5_extractors_facebook_posts.py` | `FACEBOOK_PAGES` | ⏳ Por actualizar |
| **Orquestador** | `00_orquestador_general.py` | Todos (importados) | ✅ Actualizado |

## 📍 Ubicación de cada query

### YouTube
```python
# En: Scripts/queries_config.py

YOUTUBE_CHANNELS = [
    "monicavtampico",
]

YOUTUBE_SEARCH_QUERIES = [
    "presidenta municipal de Tampico",
    "Presidenta municipal de Tampico",
    "Gobierno de Tampico",
    "gobierno de Tampico",
]
```
**Usado por:** Orquestador (✅), 1_extractors_youtube.py (⏳)

---

### Twitter/X
```python
# En: Scripts/queries_config.py

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
```
**Usado por:** Orquestador (✅), 2_extractors_twitter.py (⏳)

---

### Medios (Google News RSS)
```python
# En: Scripts/queries_config.py

MEDIOS_SITES = [
    "site:oem.com.mx",
    "site:milenio.com",
]

MEDIOS_SEARCH_TERMS = [
    '"Monica Villarreal"',
    '"gobierno de tampico"',
    '"tampico"',
]
```
**Usado por:** Orquestador (✅), 3_extractors_medios.py (⏳)

---

### Facebook
```python
# En: Scripts/queries_config.py

FACEBOOK_PAGES = [
    "TampicoGob",
    "monicavtampico",
]
```
**Usado por:** Orquestador (✅), 4_extractors_facebook_comentarios.py (⏳), 5_extractors_facebook_posts.py (⏳)

---

## 🔨 Cómo actualizar un extractor

### Ejemplo: Actualizar `1_extractors_youtube.py`

**Paso 1:** Agregar import al inicio del archivo
```python
from queries_config import YOUTUBE_CHANNELS, YOUTUBE_SEARCH_QUERIES
```

**Paso 2:** Reemplazar las definiciones locales
```python
# ANTES:
DEFAULT_YOUTUBE_CHANNELS = ["monicavtampico"]
DEFAULT_YOUTUBE_QUERIES = ["presidenta municipal...", ...]

# DESPUÉS:
DEFAULT_YOUTUBE_CHANNELS = YOUTUBE_CHANNELS
DEFAULT_YOUTUBE_QUERIES = YOUTUBE_SEARCH_QUERIES
```

**Paso 3:** Verificar que el script sigue funcionando normalmente

## 🎯 Ventajas de esta centralización

| Ventaja | Descripción |
|---------|------------|
| 📝 **Mantenimiento** | Todas las queries en un solo lugar |
| 🔄 **Consistencia** | Mismo conjunto de queries en todos los extractores |
| 🚀 **Rápido cambio** | Modificar queries sin editar múltiples scripts |
| 📊 **Auditoría** | Fácil ver historial de cambios en git |
| 🧪 **Testing** | Probar nuevas queries sin tocar código de extractores |
| 👥 **Colaboración** | Cambios centralizados para todo el equipo |

## 📌 Roadmap de migración

1. **Fase 1** (✅ Completada): Crear `queries_config.py` y actualizar `00_orquestador_general.py`
2. **Fase 2** (Próxima): Actualizar extractores individuales para importar de `queries_config.py`
   - 1_extractors_youtube.py
   - 2_extractors_twitter.py
   - 3_extractors_medios.py
   - 4_extractors_facebook_comentarios.py
   - 5_extractors_facebook_posts.py

3. **Fase 3**: Validar que todos los extractores usan la configuración centralizada

## 🔗 Referencias

- [Guía de uso de queries_config.py](./QUERIES_CONFIG_GUIDE.md)
- [`Scripts/queries_config.py`](./Scripts/queries_config.py)
- [`Scripts/00_orquestador_general.py`](./Scripts/00_orquestador_general.py)
