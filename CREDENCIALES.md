# Configuración de Credenciales API

## Descripción General

Las credenciales de API (YouTube API key, Apify token y Claude API key) se cargan automáticamente desde el archivo `.env.local` que está ignorado en Git.

## Estructura de Archivos

```
Proyecto/
├── .env.example          ← Template (versionado en Git)
├── .env.local           ← Credenciales reales (ignorado en Git) ⚠️
└── Scripts/
    └── 00_orquestador_general.py  ← Carga las variables
```

## Paso 1: Crear .env.local

Copia el template y agrega tus credenciales:

```bash
cp .env.example .env.local
```

Luego edita `.env.local`:
```env
YOUTUBE_API_KEY=tu_youtube_api_key
APIFY_TOKEN=tu_apify_token
CLAUDE_API_KEY=tu_claude_api_key
```

## Paso 2: Obtener Credenciales

### YouTube API Key
1. Ve a [Google Cloud Console](https://console.developers.google.com/)
2. Crea un nuevo proyecto
3. Habilita la API de YouTube Data v3
4. Crea una credencial de tipo "API Key"
5. Copia la key a `.env.local`

### Apify Token
1. Ve a [Apify Account](https://my.apify.com/account/integrations/api)
2. En "Your personal API token", copia tu token
3. Pégalo en `.env.local`

### Claude API Key
1. Ve a [Anthropic Console](https://console.anthropic.com/)
2. Crea o selecciona una API key
3. Copia el valor en `CLAUDE_API_KEY` dentro de `.env.local`

## Cómo Funciona

### Carga Automática
El orquestador y el script de Claude cargan las variables al iniciar:

```python
from dotenv import load_dotenv
load_dotenv('.env.local')  # Carga YOUTUBE_API_KEY, APIFY_TOKEN y CLAUDE_API_KEY
```

### En Modo Genérico
Usa automáticamente las credenciales del `.env.local`:
```
Usuario ejecuta: python3 Scripts/00_orquestador_general.py
→ Se carga .env.local
→ Selecciona "2) PARA TODAS LAS REDES"
→ Extrae datos sin pedir credenciales
```

### En Modo Específico
Pide confirmación de credenciales:
```
Usuario ejecuta: python3 Scripts/00_orquestador_general.py
→ Se carga .env.local
→ Selecciona "1) POR RED"
→ Para YouTube: "¿YouTube API key?" (sugiere la del .env.local)
→ Para Facebook: "¿Apify token?" (sugiere el del .env.local)
→ Para Claude: "¿Claude API key?" (sugiere la del .env.local)
```

## Seguridad

✅ **Protegido:**
- `.env.local` está en `.gitignore` → NO se sube a GitHub
- Credenciales nunca se guardan en commits
- Solo existen en tu máquina local

⚠️ **Importante:**
- NUNCA comitees `.env.local`
- Si accidentalmente lo hiciste, invalida inmediatamente esas keys
- Usa variables de entorno del sistema si trabajas en servidores

## Verificación

Para verificar que las credenciales se cargan correctamente:

```bash
cd Scripts/
python3 -c "import os; print(f'YouTube: {os.getenv(\"YOUTUBE_API_KEY\", \"NO ENCONTRADO\")}'); print(f'Apify: {os.getenv(\"APIFY_TOKEN\", \"NO ENCONTRADO\")}'); print(f'Claude: {os.getenv(\"CLAUDE_API_KEY\", \"NO ENCONTRADO\")}')"
```

## Dependencias

Este sistema requiere `python-dotenv`:

```bash
pip install python-dotenv
```

Ya está incluido en `requirements.txt`.

## Preguntas Frecuentes

**¿Qué pasa si .env.local no existe?**
- El orquestador fallará si necesita las credenciales
- Copia `.env.example` a `.env.local` y llena los valores

**¿Puedo usar variables de entorno del sistema?**
- Sí, si `.env.local` no existe, usa `$YOUTUBE_API_KEY`, `$APIFY_TOKEN` y `$CLAUDE_API_KEY`
- Prioridad: `.env.local` > variables de sistema

**¿Cómo uso esto en un servidor?**
- NO copies `.env.local` al servidor (eso es inseguro)
- En su lugar, configura las variables en el entorno:
  ```bash
  export YOUTUBE_API_KEY="..."
  export APIFY_TOKEN="..."
  export CLAUDE_API_KEY="..."
  python3 Scripts/00_orquestador_general.py
  ```

**¿Las credenciales se logging o se guardan en logs?**
- No, se pasan directamente como variables de entorno
- Los extractores las usan sin imprimirlas
