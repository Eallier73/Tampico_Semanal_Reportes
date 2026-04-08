#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
8_influencia_temas.py
====================
Analiza corpus semanal de Tampico y estima la influencia de temas (0-6)
sobre la polaridad global mediante regresión Ridge + Logística + Correlación.

ENTRADAS
--------
- Archivos de Datos/{week}/: material_institucional.txt, material_comentarios.txt

METODOLOGÍA
-----------
1. Carga palabras positivas/negativas/neutras por tema (0-6)
2. Construye matriz de features: polaridad por tema en cada documento
3. Calcula polaridad global del documento
4. Modela influencia de temas mediante:
   - Ridge Regression: coeficientes de influencia
   - Regresión Logística: dirección de polaridad
   - Correlación: asociación tema-polaridad

SALIDAS
-------
Influencia_Temas/{week}/
├── tecnico/
│   ├── influencia_temas.csv
│   └── polaridad_documentos.csv
└── ejecutivo/
    ├── 00_resumen_ejecutivo.md
    ├── 01_kpis_polaridad_por_tema.csv
    ├── 02_top_hallazgos_polaridad.csv
    └── 03_alertas_polaridad.csv
"""

import os
import re
import math
import argparse
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd
import sys

# =========================
# IMPORTAR FUNCIONES REPO
# =========================
sys.path.insert(0, str(Path(__file__).parent))
from output_naming import build_report_tag, build_output_dir, ensure_tagged_name

DEFAULT_STOPWORDS_PATH = (
    Path(__file__).resolve().parent
    / "diccionarios"
    / "stopwords"
    / "stop_list_espanol.txt"
)

# =========================
# CONFIGURACIÓN TAMPICO
# =========================
TOPIC_IDS = [0, 1, 2, 3, 4, 6]
NEGATIVE_TOPICS = [1, 2]  # Temas donde palabras negativas = polaridad negativa

NOMBRES_TEMAS = {
    0: "Servicios e Infraestructura Pública",
    1: "Seguridad y Criminalidad",
    2: "Medio Ambiente y Servicios de Agua",
    3: "Obra Pública e Inversión",
    4: "Educación y Cultura",
    6: "Gobernanza y Administración Pública"
}

TEMAS_KEYWORDS = {
    0: {
        'agua': {
            'positivas': [
                'agua limpia', 'agua potable', 'potable', 'abasto', 'suministro',
                'cisterna', 'cisterna', 'tinaco', 'tinacos', 'bombeo',
                'servicio agua', 'recibo agua', 'red hidraulica', 'presion',
            ],
            'negativas': [
                'fuga', 'fugas', 'inundacion', 'inundaciones', 'agua sucia',
                'agua turbia', 'aguas negras', 'contaminada', 'corte de agua',
                'corte', 'cortan', 'derrame', 'desabasto', 'desbordamiento',
                'encharcamiento', 'escurrimientos', 'estiaje', 'filtrada',
                'no potable', 'poca presion', 'seca', 'sin agua', 'sequia',
                'charco', 'charcos', 'sed',
            ],
            'neutras': [
                'agua', 'comapa', 'conagua', 'drenaje', 'drenajes', 'ductos',
                'alcantarillado', 'alcantarillas', 'cano', 'tuberia', 'tuberias',
                'tubo', 'tubos', 'hidraulica', 'hidraulico', 'hidrica', 'hidrico',
                'pozo', 'pozos', 'piscinas', 'grifo', 'llave', 'bomba',
                'lluvia', 'lluvias', 'temporada de lluvias', 'temporal',
            ],
        },
        'basura': {
            'positivas': [
                'aseo', 'limpia', 'limpiar', 'limpieza', 'limpiesa', 'limpian',
                'limpio', 'barrido', 'barrer', 'barrendero', 'recogen', 'recoger',
                'recoleccion', 'recolectar', 'reciclaje', 'servicio limpieza',
                'descacharrizacion', 'arreglo de calles',
            ],
            'negativas': [
                'basura acumulada', 'basura quemada', 'basura regada',
                'basura tirada', 'amontonada', 'apesta', 'asqueroso',
                'cascajo', 'chatarra', 'contaminacion', 'desechos', 'escombro',
                'desperdicio', 'falta recoleccion', 'fetido', 'mal olor',
                'monton', 'montones', 'moscas', 'olor', 'peste', 'pestilencia',
                'pestilente', 'sucio', 'sucia', 'sucias', 'sucios', 'suciedad',
                'sobras', 'tiradero', 'resto',
            ],
            'neutras': [
                'basura', 'basurero', 'bolsa', 'bolsas', 'camion',
                'camion de basura', 'contenedor', 'contenedores', 'lata',
                'plastico', 'residuos', 'tirar', 'vidrio',
            ],
        },
        'alumbrado': {
            'positivas': [
                'encendida', 'encendidas', 'encendido', 'encendidos',
                'iluminadas', 'iluminado', 'iluminados', 'visible',
                'visibilidad', 'sensor', 'sensores', 'energia',
                'iluminacion publica', 'servicio de alumbrado',
                'luz de la calle', 'luz en la calle', 'corriente',
            ],
            'negativas': [
                'insuficiente', 'roto', 'oscuro', 'oscura', 'oscuras', 'oscuros',
                'oscuridad', 'fundido', 'fundidos', 'fundida', 'fundidas',
                'apagado', 'apagados', 'apagada', 'apagadas',
                'deficiente', 'penumbra', 'penumbras', 'sombrio',
                'tiniebla', 'tinieblas',
            ],
            'neutras': [
                'alumbrado', 'luz', 'luces', 'luminaria', 'luminarias',
                'iluminacion', 'iluminación', 'lampara', 'lamparas',
                'poste', 'postes', 'poste de luz', 'postes de luz',
                'electricidad', 'electrico', 'foco', 'focos',
                'farol', 'farola', 'farolas', 'faroles',
                'alumbrar', 'iluminar', 'esquinas',
            ],
        },
    },
    1: {
        'delitos_patrimoniales': {
            'positivas': [
                'recuperacion', 'decomiso', 'aseguramiento', 'detenido',
                'detenidos', 'detencion', 'captura', 'capturado', 'capturados',
            ],
            'negativas': [
                'robo', 'robos', 'ladron', 'ladrones', 'rata', 'ratas',
                'ratero', 'rateros', 'ratera', 'asalto', 'asaltos',
                'asaltante', 'asaltantes', 'atraco', 'atracos',
                'cristalazo', 'cristalazos', 'carjacking',
                'extorsion', 'secuestro', 'secuestros',
            ],
            'neutras': [
                'carpeta', 'averiguacion', 'denuncia', 'investigacion',
            ],
        },
        'violencia_crimen_organizado': {
            'positivas': [
                'operativo', 'operativos', 'decomiso', 'captura',
                'capturado', 'capturados', 'aseguramiento', 'detencion',
            ],
            'negativas': [
                'homicidio', 'homicidios', 'asesinato', 'asesinatos',
                'balacera', 'balaceras', 'tiroteo', 'disparos',
                'ejecutado', 'ejecutados', 'levanton', 'levantones',
                'sicario', 'sicarios', 'narco', 'narcos', 'narcomenudeo',
                'halcon', 'halcones', 'narcomantas', 'narcomanta',
                'muerto', 'muertos', 'cadaver', 'encobijado', 'embolsado',
                'descuartizado', 'crimen', 'criminal', 'criminales',
                'delincuencia', 'delincuente', 'delincuentes',
                'pandilla', 'pandillas', 'pandillero', 'pandilleros',
            ],
            'neutras': [
                'ministerio', 'fiscal', 'fiscalia', 'mp',
                'perito', 'peritos', 'forense',
            ],
        },
        'violencia_personas': {
            'positivas': [
                'rescate', 'auxilio', 'denuncia', 'alerta',
            ],
            'negativas': [
                'violacion', 'feminicidio', 'feminicidios',
                'violencia', 'acoso', 'intimidacion',
                'madriza', 'golpiza', 'apuñalado', 'baleado',
                'herido', 'heridos', 'victima', 'victimas',
                'amenaza', 'amenazas', 'desaparecido', 'desaparecidos',
                'desaparecida', 'peligro', 'peligroso', 'peligrosa',
                'peligrosos',
            ],
            'neutras': [
                'ambulancia', 'emergencia',
            ],
        },
        'prevencion_vigilancia': {
            'positivas': [
                'seguridad', 'vigilancia', 'proteccion', 'patrullaje',
                'prevencion', 'disuasion', 'ronda', 'rondines',
                'custodia', 'blindaje', 'resguardo', 'reaccion',
                'videovigilancia', 'camaras', 'alarma',
            ],
            'negativas': [
                'inseguridad', 'vandalismo', 'impunidad', 'impune',
            ],
            'neutras': [
                'policia', 'guardia', 'patrulla', 'patrullas',
                'agente', 'agentes', 'elemento', 'elementos',
                'uniformado', 'uniformados', 'comandante', 'comisario',
                'destacamento', 'cuartel', 'base', 'sector', 'sirena',
            ],
        },
    },
    2: {
        'drenaje': {
            'positivas': [
                'sanitario', 'desague',
            ],
            'negativas': [
                'negras', 'fuga', 'olor', 'olores', 'pestilencia',
                'inundado', 'charco', 'desbordamiento', 'contaminacion',
                'contaminado', 'desbordamiento',
            ],
            'neutras': [
                'drenaje', 'alcantarilla', 'alcantarillado', 'aguas',
                'coladera', 'zanja',
            ],
        },
        'pavimento': {
            'positivas': [
                'pavimentacion', 'reparacion', 'repavimentacion',
                'pavimentar', 'libramiento',
            ],
            'negativas': [
                'bache', 'baches', 'hoyo', 'hoyos', 'hundimiento',
                'hundimientos', 'agrietado', 'agrietados', 'grieta',
                'grietas', 'cuarteado', 'desnivelado', 'desnivel',
                'desniveles', 'crater', 'crateres', 'socavon',
                'socavones', 'fractura', 'escombro', 'escombros',
                'mojado', 'derrumbe', 'escalon',
            ],
            'neutras': [
                'pavimento', 'asfalto', 'concreto', 'cemento',
                'arena', 'piedra', 'piedras',
            ],
        },
        'vialidad': {
            'positivas': [
                'transitar', 'circulacion', 'peatonal',
            ],
            'negativas': [
                'congestionamiento', 'embotellamiento', 'choque',
                'choques', 'emergencia', 'encharcamiento',
                'trafico',
            ],
            'neutras': [
                'calle', 'calles', 'avenida', 'avenidas', 'banqueta',
                'banquetas', 'camino', 'carretera', 'carreteras',
                'vialidad', 'vialidades', 'vias', 'cruce', 'crucero',
                'esquina', 'transito', 'semaforo', 'semaforos',
                'camellones', 'vial', 'peatones', 'rodada', 'puente',
                'boulevard', 'autopista', 'anden', 'ruta', 'rutas',
                'tope', 'topes', 'tapa', 'tapas', 'senales',
                'pozo', 'pozos', 'vehicular',
            ],
        },
    },
    3: {
        'obras': {
            'positivas': [
                'remodelacion', 'ampliacion', 'rehabilitacion', 'mejoramiento',
                'reconstruccion', 'renovacion', 'modernizacion', 'inauguracion',
                'avance', 'progreso', 'terminada', 'terminado', 'concluida',
                'concluido', 'entrega', 'entregada', 'entregado', 'beneficio',
                'inversion', 'equipamiento', 'restauracion', 'rescate',
                'dignificacion', 'transformacion',
            ],
            'negativas': [
                'inconclusa', 'inconcluso', 'inconclusas', 'abandono',
                'abandonada', 'abandonado', 'parada', 'parado', 'detenida',
                'detenido', 'retraso', 'retrasada', 'atrasada', 'atrasado',
                'demolicion', 'derrumbe', 'desplome', 'irregular',
                'fantasma', 'moches', 'sobreprecio', 'sobrecosto',
                'desperdicio', 'estancada', 'estancado', 'deficiente',
                'inconformidad', 'peligro', 'riesgo', 'fraude',
                'simulacion', 'opaca', 'opaco',
            ],
            'neutras': [
                'obra', 'obras', 'construccion', 'proyecto', 'proyectos',
                'infraestructura', 'desarrollo', 'mantenimiento',
                'edificio', 'edificacion', 'puente', 'licitacion',
                'contrato', 'presupuesto', 'planeacion', 'programa',
            ],
        },
    },
    4: {
        'educacion': {
            'positivas': [
                'escuela', 'escuelas', 'educacion', 'formacion',
                'maestro', 'maestros', 'docente', 'docentes',
                'estudiante', 'estudiantes', 'aprendizaje', 'capacitacion',
                'beca', 'becas', 'programa educativo', 'calidad educativa',
            ],
            'negativas': [
                'analfabetismo', 'desercion', 'deficiencia', 'bajo rendimiento',
                'falta de maestros', 'falta de recursos', 'descuido',
            ],
            'neutras': [
                'alumno', 'alumnos', 'escolar', 'academico',
                'colegio', 'instituto', 'universitario', 'universidad',
            ],
        },
        'cultura': {
            'positivas': [
                'culture', 'evento cultural', 'evento', 'eventos',
                'musica', 'arte', 'pintura', 'teatro', 'danza',
                'festival', 'feria', 'celebracion', 'concierto',
            ],
            'negativas': [
                'censura', 'prohibicion', 'falta de acceso',
            ],
            'neutras': [
                'cultura', 'cultural', 'patrimonio', 'tradicion',
                'biblioteca', 'museo', 'centro cultural',
            ],
        },
    },
    6: {
        'gobierno': {
            'positivas': [
                'transparencia', 'rendicion de cuentas', 'participacion ciudadana',
                'decision correcta', 'gestion eficiente', 'resultado positivo',
                'avance', 'exito', 'logro',
            ],
            'negativas': [
                'corrupcion', 'malversacion', 'nepotismo', 'favoritismo',
                'falta de transparencia', 'falta de rendicion', 'impunidad',
                'ineficiencia', 'incompetencia', 'negligencia',
            ],
            'neutras': [
                'gobierno', 'administracion', 'alcaldia', 'municipio',
                'funcionario', 'funcionarios', 'decision', 'politica',
                'decreto', 'ordenanza', 'regidores', 'bienestar social',
            ],
        },
    }
}

# =========================
# FUNCIONES UTILIDAD
# =========================
def log(msg: str):
    """Imprime mensaje con timestamp."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")

def normalizar_texto(texto: str) -> str:
    """Normaliza texto: minúsculas, sin acentos, sin puntuación."""
    texto = texto.lower().strip()
    # Quitar acentos
    acentos = {
        'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
        'ñ': 'n', 'ü': 'u'
    }
    for ac, sin_ac in acentos.items():
        texto = texto.replace(ac, sin_ac)
    # Quitar puntuación excepto espacios
    texto = re.sub(r'[^a-z0-9\s]', '', texto)
    return texto.strip()

def tokenizar(texto: str, stoplist: set) -> list:
    """Tokeniza texto y elimina stopwords."""
    texto_norm = normalizar_texto(texto)
    tokens = [t for t in texto_norm.split() if t and t not in stoplist]
    return tokens

def read_wordlist(path: Path) -> set:
    """Lee lista de palabras desde archivo."""
    words = set()
    if path.exists():
        with path.open('r', encoding='utf-8', errors='ignore') as f:
            for linea in f:
                w = normalizar_texto(linea.strip())
                if w:
                    words.add(w)
    return words

def safe_corr(x: np.ndarray, y: np.ndarray) -> float:
    """Calcula correlación de Pearson de forma segura."""
    if len(x) < 2 or len(y) < 2:
        return 0.0
    try:
        return float(np.corrcoef(x, y)[0, 1])
    except:
        return 0.0

def clasificar_nivel(valor_abs: float) -> str:
    """Clasifica nivel de impacto por valor absoluto."""
    if valor_abs > 0.05:
        return "Alta"
    elif valor_abs > 0.02:
        return "Media"
    else:
        return "Baja"

def clasificar_confianza(ridge: float, logit: float, corr: float) -> str:
    """Clasifica confianza en el coeficiente."""
    acuerdo = sum([
        abs(ridge) > 0.01,
        abs(logit) > 0.01,
        abs(corr) > 0.2
    ])
    if acuerdo >= 3:
        return "Alta"
    elif acuerdo >= 2:
        return "Media"
    else:
        return "Baja"

def direccion_impacto(coef_ridge: float, correlacion: float, intensidad_media: float) -> str:
    """Determina dirección de impacto (polaridad negativa o positiva)."""
    if intensidad_media < -0.001:
        return "Polaridad negativa"
    elif intensidad_media > 0.001:
        return "Polaridad positiva"
    elif correlacion < 0:
        return "Polaridad negativa"
    else:
        return "Polaridad positiva"

def fit_logistic(X: np.ndarray, y: np.ndarray, lam: float = 1.0, lr: float = 0.1, iters: int = 100) -> np.ndarray:
    """Regresión logística con descenso de gradiente."""
    n, p = X.shape
    w = np.zeros(p)
    
    for _ in range(iters):
        z = X @ w
        pred = 1.0 / (1.0 + np.exp(-z))
        err = pred - y
        grad = X.T @ err / n + lam * w
        w -= lr * grad
    
    return w

# =========================
# HELPER: Extracción de keywords y polaridad
# =========================
def obtener_subtemas(tema_id: int) -> list:
    """Retorna lista de subtemas para un tema dado."""
    tema_data = TEMAS_KEYWORDS.get(tema_id, {})
    if isinstance(tema_data, dict):
        return list(tema_data.keys())
    return []

def extraer_keywords_subtema(tema_id: int, subtema_nombre: str) -> set:
    """Extrae keywords de un subtema específico."""
    tema_data = TEMAS_KEYWORDS.get(tema_id, {})
    keywords = set()
    
    if not isinstance(tema_data, dict):
        return keywords
    
    subtema_data = tema_data.get(subtema_nombre, {})
    if isinstance(subtema_data, dict):
        for tipo in ['positivas', 'negativas', 'neutras']:
            for w in subtema_data.get(tipo, []):
                nw = normalizar_texto(w)
                if nw:
                    if " " in nw:
                        keywords.update(nw.split())
                    else:
                        keywords.add(nw)
    
    return keywords

def extraer_polaridad_subtema(tema_id: int, subtema_nombre: str) -> tuple:
    """Extrae polaridad de un subtema específico."""
    tema_data = TEMAS_KEYWORDS.get(tema_id, {})
    pos_set = set()
    neg_set = set()
    
    if not isinstance(tema_data, dict):
        return pos_set, neg_set
    
    subtema_data = tema_data.get(subtema_nombre, {})
    if isinstance(subtema_data, dict):
        for w in subtema_data.get('positivas', []):
            nw = normalizar_texto(w)
            if nw:
                if " " in nw:
                    pos_set.update(nw.split())
                else:
                    pos_set.add(nw)
        for w in subtema_data.get('negativas', []):
            nw = normalizar_texto(w)
            if nw:
                if " " in nw:
                    neg_set.update(nw.split())
                else:
                    neg_set.add(nw)
    
    return pos_set, neg_set

def extraer_keywords_tema(tema_id: int) -> set:
    """Extrae todas las keywords de un tema."""
    tema_data = TEMAS_KEYWORDS.get(tema_id, {})
    keywords = set()
    
    if isinstance(tema_data, dict):
        for subcat, contenido in tema_data.items():
            if isinstance(contenido, dict):
                for tipo in ['positivas', 'negativas', 'neutras']:
                    for w in contenido.get(tipo, []):
                        nw = normalizar_texto(w)
                        if nw:
                            if " " in nw:
                                keywords.update(nw.split())
                            else:
                                keywords.add(nw)
    
    return keywords

def extraer_polaridad_tema(tema_id: int) -> tuple:
    """Extrae sets de palabras positivas y negativas por tema."""
    tema_data = TEMAS_KEYWORDS.get(tema_id, {})
    pos_set = set()
    neg_set = set()
    
    if isinstance(tema_data, dict):
        for subcat, contenido in tema_data.items():
            if isinstance(contenido, dict):
                for w in contenido.get('positivas', []):
                    nw = normalizar_texto(w)
                    if nw:
                        if " " in nw:
                            pos_set.update(nw.split())
                        else:
                            pos_set.add(nw)
                for w in contenido.get('negativas', []):
                    nw = normalizar_texto(w)
                    if nw:
                        if " " in nw:
                            neg_set.update(nw.split())
                        else:
                            neg_set.add(nw)
    
    return pos_set, neg_set

# =========================
# FEATURES
# =========================
def contar_hits_tema(tokens: list, keywords_set: set) -> int:
    """Cuenta hits (coincidencias) de keywords en tokens."""
    return sum(1 for t in tokens if t in keywords_set)

def calcular_polaridad_tema_simple(tokens: list, pos_set: set, neg_set: set) -> dict:
    """Calcula polaridad simple de un tema: positivas - negativas."""
    count_pos = sum(1 for t in tokens if t in pos_set)
    count_neg = sum(1 for t in tokens if t in neg_set)
    score = count_pos - count_neg
    
    return {
        'positivas': count_pos,
        'negativas': count_neg,
        'score': score
    }

def calcular_polaridad_documento(tokens: list, topic_pos: dict, topic_neg: dict) -> float:
    """Calcula polaridad global del documento."""
    score_total = 0.0
    for tema_id in TOPIC_IDS:
        pos_set = topic_pos.get(tema_id, set())
        neg_set = topic_neg.get(tema_id, set())
        result = calcular_polaridad_tema_simple(tokens, pos_set, neg_set)
        score_total += result['score']
    
    return float(score_total)

def build_feature_matrix(docs: list, stoplist: set):
    """
    Construye matriz de features y vector de polaridad global.
    
    Returns:
        X: matriz de features (n_docs x n_features)
        y: vector de polaridad global
        df_docs: DataFrame con info de documentos
        feature_info: lista de (tema_id, subtema_nombre, nombre_display)
        hits_raw: conteos de hits por feature
        docs_con_hits: documentos que mencionan cada feature
    """
    topic_ids = TOPIC_IDS[:]
    
    # Construir lista de features (temas + subtemas)
    feature_info = []
    feature_sets = []
    feature_pos = []
    feature_neg = []
    
    for t in topic_ids:
        subtemas = obtener_subtemas(t)
        
        if subtemas:
            for subtema_nombre in subtemas:
                feature_info.append((t, subtema_nombre, f"{NOMBRES_TEMAS[t]} - {subtema_nombre.capitalize()}"))
                feature_sets.append(extraer_keywords_subtema(t, subtema_nombre))
                pos, neg = extraer_polaridad_subtema(t, subtema_nombre)
                feature_pos.append(pos)
                feature_neg.append(neg)
        else:
            feature_info.append((t, None, NOMBRES_TEMAS[t]))
            feature_sets.append(extraer_keywords_tema(t))
            pos, neg = extraer_polaridad_tema(t)
            feature_pos.append(pos)
            feature_neg.append(neg)
    
    n_features = len(feature_info)
    
    # Polaridad por tema (para cálculo de y global)
    topic_pos = {}
    topic_neg = {}
    for t in topic_ids:
        topic_pos[t], topic_neg[t] = extraer_polaridad_tema(t)
    
    # Matrices
    X = np.zeros((len(docs), n_features), dtype=float)
    y = np.zeros(len(docs), dtype=float)
    hits_raw = np.zeros(n_features, dtype=int)
    docs_con_hits = np.zeros(n_features, dtype=int)

    rows_docs = []
    for i, raw in enumerate(docs):
        tokens = tokenizar(raw, stoplist)
        doc_len = max(len(tokens), 1)

        # Polaridad global del documento
        y[i] = calcular_polaridad_documento(tokens, topic_pos, topic_neg) / doc_len

        # Features por tema/subtema
        for j, (tema_id, subtema_nombre, nombre_display) in enumerate(feature_info):
            pos_set = feature_pos[j]
            neg_set = feature_neg[j]
            
            # Contar hits
            hits_ct = contar_hits_tema(tokens, feature_sets[j])
            if hits_ct > 0:
                docs_con_hits[j] += 1
                hits_raw[j] += hits_ct
            
            # Polaridad normalizada
            result = calcular_polaridad_tema_simple(tokens, pos_set, neg_set)
            score_normalizado = result['score'] / doc_len
            
            X[i, j] = score_normalizado

        # Info del documento
        total_pos = sum(calcular_polaridad_tema_simple(tokens, topic_pos[t], topic_neg[t])['positivas'] for t in topic_ids)
        total_neg = sum(calcular_polaridad_tema_simple(tokens, topic_pos[t], topic_neg[t])['negativas'] for t in topic_ids)
        
        rows_docs.append({
            "doc_id": i,
            "texto": raw[:100],  # primeros 100 caracteres
            "tokens": doc_len,
            "positivas": total_pos,
            "negativas": total_neg,
            "polaridad_score": y[i]
        })

    return X, y, pd.DataFrame(rows_docs), feature_info, hits_raw, docs_con_hits

# =========================
# MODELADO + ANÁLISIS
# =========================
def analizar_influencia(docs: list, stoplist: set, output_dir: Path, report_tag: str):
    """Analiza influencia de temas sobre polaridad."""
    
    out_tec = output_dir / "tecnico"
    out_exec = output_dir / "ejecutivo"
    out_tec.mkdir(parents=True, exist_ok=True)
    out_exec.mkdir(parents=True, exist_ok=True)

    if len(docs) < 20:
        log(f"⚠️  Muy pocos documentos ({len(docs)}). Se omite análisis.")
        return False

    X, y, df_docs, feature_info, hits_raw, docs_con_hits = build_feature_matrix(docs, stoplist)

    # Estandarización
    X_mean = X.mean(axis=0)
    X_std = X.std(axis=0) + 1e-8
    Xs = (X - X_mean) / X_std

    y_mean = y.mean()
    y_std = y.std() + 1e-8
    ys = (y - y_mean) / y_std

    # Ridge Regression
    lam = 1.0
    XtX = Xs.T @ Xs + lam * np.eye(Xs.shape[1])
    Xty = Xs.T @ ys
    coef_ridge = np.linalg.solve(XtX, Xty)

    # Regresión Logística
    y_bin = (y > np.median(y)).astype(float)
    coef_logit = fit_logistic(Xs, y_bin, lam=1.0, lr=0.1, iters=600)

    # Correlación
    corrs = [safe_corr(Xs[:, j], ys) for j in range(Xs.shape[1])]

    # Métricas
    hits = hits_raw
    docs_con_feature = docs_con_hits
    intensidad = []
    for j in range(X.shape[1]):
        mask = X[:, j] != 0
        intensidad.append(float(X[mask, j].mean()) if mask.any() else 0.0)

    # Contribución %
    abs_sum = np.sum(np.abs(coef_ridge)) + 1e-9
    contrib = (np.abs(coef_ridge) / abs_sum) * 100.0

    # ========== TÉCNICO ==========
    df_inf = pd.DataFrame({
        "tema_id": [fi[0] for fi in feature_info],
        "subtema": [fi[1] if fi[1] else "" for fi in feature_info],
        "nombre_completo": [fi[2] for fi in feature_info],
        "coef_ridge": coef_ridge,
        "coef_logit": coef_logit,
        "correlacion": corrs,
        "docs_con_feature": docs_con_feature,
        "intensidad_media": intensidad,
        "contribucion_pct": contrib
    })

    df_inf.to_csv(out_tec / "influencia_temas.csv", index=False, encoding="utf-8")
    df_docs.to_csv(out_tec / "polaridad_documentos.csv", index=False, encoding="utf-8")

    # ========== EJECUTIVO: Agregación por tema principal ==========
    n_docs = len(df_docs)
    TEMAS_REPORTE = TOPIC_IDS
    
    exec_rows_temas = []
    total_hits = max(float(hits.sum()), 1.0)
    
    for t in TEMAS_REPORTE:
        tema_rows = df_inf[df_inf["tema_id"] == t]
        
        if len(tema_rows) == 0:
            continue
        
        coef_ridge_tema = tema_rows["coef_ridge"].mean()
        coef_logit_tema = tema_rows["coef_logit"].mean()
        corr_tema = tema_rows["correlacion"].mean()
        intensidad_tema = tema_rows["intensidad_media"].mean()
        contrib_tema = tema_rows["contribucion_pct"].sum()
        hits_tema = tema_rows["docs_con_feature"].sum()
        part = (float(hits_tema) / total_hits) * 100.0
        
        impacto = clasificar_nivel(abs(coef_ridge_tema))
        direccion = direccion_impacto(coef_ridge_tema, corr_tema, intensidad_tema)
        confianza = clasificar_confianza(coef_ridge_tema, coef_logit_tema, corr_tema)
        
        exec_rows_temas.append({
            "tema_id": int(t),
            "tema": NOMBRES_TEMAS[t],
            "participacion_pct": round(part, 2),
            "intensidad_media": round(intensidad_tema, 4),
            "impacto_en_polaridad": impacto,
            "direccion": direccion,
            "confianza": confianza,
            "contribucion_pct": round(contrib_tema, 2),
            "coef_ridge": round(coef_ridge_tema, 6),
            "coef_logit": round(coef_logit_tema, 6),
            "correlacion": round(corr_tema, 6),
            "num_subtemas": len(tema_rows)
        })
    
    df_exec_temas = pd.DataFrame(exec_rows_temas).sort_values("tema_id")
    
    # DataFrame ejecutivo de subtemas
    exec_rows_subtemas = []
    for idx, row in df_inf.iterrows():
        impacto = clasificar_nivel(abs(row["coef_ridge"]))
        direccion = direccion_impacto(row["coef_ridge"], row["correlacion"], row["intensidad_media"])
        confianza = clasificar_confianza(row["coef_ridge"], row["coef_logit"], row["correlacion"])
        part = (float(row["docs_con_feature"]) / total_hits) * 100.0 if total_hits > 0 else 0.0
        
        exec_rows_subtemas.append({
            "tema_id": int(row["tema_id"]),
            "subtema": row["subtema"],
            "nombre_completo": row["nombre_completo"],
            "participacion_pct": round(part, 2),
            "intensidad_media": round(row["intensidad_media"], 4),
            "impacto_en_polaridad": impacto,
            "direccion": direccion,
            "confianza": confianza,
            "contribucion_pct": round(row["contribucion_pct"], 2),
            "coef_ridge": round(row["coef_ridge"], 6),
            "coef_logit": round(row["coef_logit"], 6),
            "correlacion": round(row["correlacion"], 6)
        })
    
    df_exec_subtemas = pd.DataFrame(exec_rows_subtemas).sort_values(["tema_id", "subtema"])

    # Guardar CSVs ejecutivos
    df_exec_temas.to_csv(out_exec / "01_kpis_polaridad_por_tema.csv", index=False, encoding="utf-8")
    df_exec_subtemas.to_csv(out_exec / "01b_kpis_polaridad_por_subtema.csv", index=False, encoding="utf-8")

    # Top hallazgos
    df_hall = df_exec_subtemas.copy().sort_values("contribucion_pct", ascending=False).head(15)
    df_hall["ranking_contribucion"] = range(1, len(df_hall) + 1)
    df_hall["mensaje"] = df_hall.apply(
        lambda r: (
            f"{r['nombre_completo']}: {r['direccion']} | impacto {r['impacto_en_polaridad']} | "
            f"confianza {r['confianza']} | contribución {r['contribucion_pct']:.2f}%."
        ),
        axis=1
    )
    df_hall.to_csv(out_exec / "02_top_hallazgos_polaridad.csv", index=False, encoding="utf-8")

    # Alertas por tema
    def nivel_alerta(row):
        if row["direccion"] == "Polaridad negativa" and row["impacto_en_polaridad"] == "Alta":
            return "Alta"
        if row["direccion"] == "Polaridad negativa" and row["impacto_en_polaridad"] == "Media":
            return "Media"
        if row["direccion"] == "Polaridad negativa":
            return "Baja"
        return "Oportunidad"

    df_alertas = df_exec_temas.copy()
    df_alertas["nivel_alerta"] = df_alertas.apply(nivel_alerta, axis=1)
    df_alertas["motivo"] = df_alertas.apply(
        lambda r: (
            f"Dirección {r['direccion']}; impacto {r['impacto_en_polaridad']}; "
            f"contribución {r['contribucion_pct']:.2f}% ({r['num_subtemas']} subtemas)."
        ),
        axis=1
    )
    df_alertas = df_alertas[
        ["tema_id", "tema", "nivel_alerta", "direccion",
         "impacto_en_polaridad", "confianza", "contribucion_pct", "num_subtemas", "motivo"]
    ].sort_values("tema_id")
    df_alertas.to_csv(out_exec / "03_alertas_polaridad.csv", index=False, encoding="utf-8")

    # Resumen ejecutivo markdown
    media_polaridad = float(df_docs["polaridad_score"].mean())

    md = []
    md.append(f"# Resumen Ejecutivo - Análisis de Influencia de Temas")
    md.append(f"_Semana: {report_tag}_\n")
    md.append(f"- Documentos analizados: **{n_docs}**")
    md.append(f"- Score promedio de polaridad (normalizado): **{media_polaridad:.4f}**")
    md.append(f"- Total de features analizadas: **{len(feature_info)}** ({len(TEMAS_REPORTE)} temas)\n")
    
    md.append("## Tabla de temas principales")
    md.append("| Tema | Participación % | Contribución % | Dirección | Impacto | Confianza |")
    md.append("| --- | ---: | ---: | --- | --- | --- |")
    for _, r in df_exec_temas.sort_values("tema_id").iterrows():
        md.append(
            f"| {r['tema']} | {r['participacion_pct']:.2f} | {r['contribucion_pct']:.2f} | "
            f"{r['direccion']} | {r['impacto_en_polaridad']} | {r['confianza']} |"
        )
    
    md.append("\n## Top 10 Subtemas más influyentes")
    md.append("| # | Subtema | Contribución % | Dirección | Impacto |")
    md.append("| ---: | --- | ---: | --- | --- |")
    for idx, (_, r) in enumerate(df_exec_subtemas.sort_values("contribucion_pct", ascending=False).head(10).iterrows(), 1):
        md.append(
            f"| {idx} | {r['nombre_completo']} | {r['contribucion_pct']:.2f} | "
            f"{r['direccion']} | {r['impacto_en_polaridad']} |"
        )

    (out_exec / "00_resumen_ejecutivo.md").write_text("\n".join(md), encoding="utf-8")

    log(f"✅ Análisis completado -> {output_dir}")
    return True

# =========================
# MAIN
# =========================
def weekly_input_dir(base_dir: Path, since: str) -> Path:
    """Retorna directorio semanal de entrada (Datos)."""
    base_path = Path(base_dir)
    week_tag = build_report_tag(since, "Datos")
    if base_path.name == week_tag:
        return base_path
    return base_path / week_tag


def weekly_output_dir(base_dir: Path, since: str) -> Path:
    """Retorna directorio semanal de salida (Influencia_Temas)."""
    base_path = Path(base_dir)
    week_tag = build_report_tag(since, "Influencia_Temas")
    if base_path.name == week_tag:
        return base_path
    return base_path / week_tag


def main():
    parser = argparse.ArgumentParser(description="Analisis de influencia de temas sobre polaridad en Tampico")
    parser.add_argument("--since", required=True, help="Fecha inicio (YYYY-MM-DD)")
    parser.add_argument("--before", required=True, help="Fecha fin (YYYY-MM-DD)")
    parser.add_argument("--input-dir", help="Directorio con archivos material_*.txt (opcional)")
    parser.add_argument("--output-dir", help="Directorio de salida (opcional)")
    parser.add_argument(
        "--stopwords-path",
        default=str(DEFAULT_STOPWORDS_PATH),
        help=f"Ruta a archivo de stopwords (default: {DEFAULT_STOPWORDS_PATH})",
    )
    
    args = parser.parse_args()
    
    # Construir rutas dinámicas
    report_tag = build_report_tag(args.since, "Influencia_Temas")
    
    if args.input_dir:
        input_base_dir = Path(args.input_dir)
    else:
        input_base_dir = Path(__file__).parent.parent / "Datos"
    input_dir = weekly_input_dir(input_base_dir, args.since)
    
    if args.output_dir:
        output_base_dir = Path(args.output_dir)
    else:
        output_base_dir = Path(__file__).parent.parent / "Influencia_Temas"
    output_dir = weekly_output_dir(output_base_dir, args.since)
    
    log("=" * 70)
    log("ANÁLISIS DE INFLUENCIA DE TEMAS - TAMPICO")
    log("=" * 70)
    log(f"Período: {args.since} a {args.before}")
    log(f"Input: {input_dir}")
    log(f"Output: {output_dir}")
    log("")
    
    # Cargar archivos material
    material_inst_path = input_dir / "material_institucional.txt"
    material_com_path = input_dir / "material_comentarios.txt"
    
    docs = []
    if material_inst_path.exists():
        with material_inst_path.open('r', encoding='utf-8', errors='ignore') as f:
            docs.extend([ln.strip() for ln in f if ln.strip()])
        log(f"✅ Cargado: {material_inst_path.name} ({len(docs)} docs)")
    else:
        log(f"⚠️  No encontrado: {material_inst_path.name}")
    
    if material_com_path.exists():
        with material_com_path.open('r', encoding='utf-8', errors='ignore') as f:
            docs.extend([ln.strip() for ln in f if ln.strip()])
        log(f"✅ Cargado: {material_com_path.name} ({len(docs)} docs total)")
    else:
        log(f"⚠️  No encontrado: {material_com_path.name}")
    
    if len(docs) == 0:
        log("❌ ERROR: No se encontraron documentos para analizar")
        return False
    
    log(f"📊 Total de documentos: {len(docs)}\n")
    
    # Cargar stopwords
    stopwords_path = Path(args.stopwords_path)
    stoplist = read_wordlist(stopwords_path)
    if not stoplist:
        log(f"⚠️  No se encontraron stopwords en: {stopwords_path}. Usando lista vacia")
    log(f"✅ Stopwords: {len(stoplist)} palabras\n")
    
    # Analizar
    log("🔄 Iniciando análisis de influencia...")
    try:
        success = analizar_influencia(docs, stoplist, output_dir, report_tag)
        if success:
            log("✅ Análisis completado exitosamente")
            log(f"📁 Resultados en: {output_dir}")
        else:
            log("⚠️  El análisis no produjo resultados")
            return False
    except Exception as e:
        log(f"❌ Error durante análisis: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    log("🏁 Proceso finalizado.")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
