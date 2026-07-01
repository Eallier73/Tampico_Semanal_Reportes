"""
SNA guiado: variante independiente de la red de palabras + cuentas.

Agrega temas rastreados y polaridad sin modificar 12d_red_cuentas.py ni su
HTML. Escribe exclusivamente en clusters/red_guiada/.
Genera un HTML aparte con:
  - Los 1203 nodos de palabras del 12c (color por tema, tamaño por grado)
  - Top N cuentas (default 500) conectadas a sus top K palabras (default 5)
  - Panel de filtros: toggle cuentas, plataforma, min_msgs, top K, peso min

Entradas (en SNA/Resultados/historico/):
  - clusters/red_completa/nodos_metricas.csv
  - clusters/red_completa/aristas_clasificadas.csv
  - cuentas_clusters/cuentas_resumen.csv
  - cuentas_clusters/palabras_x_cuenta.csv

Salida:
  - clusters/red_completa/red_tampico_cuentas.html
  - clusters/red_completa/metricas_cuentas.json
"""

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path

from sna_guiada_common import (
    DEFAULT_NEGATIVE_DICTIONARY,
    DEFAULT_POSITIVE_DICTIONARY,
    DEFAULT_TOPIC_DICTIONARY,
    aggregate_words,
    annotate_words,
    inject_guided_layer,
    load_lexicons,
    write_annotation_outputs,
)


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BASE = REPO_ROOT / "SNA" / "Resultados" / "historico"


def vis_assets() -> tuple[str, str]:
    """Carga vis-network desde PyVis y lo incrusta en el HTML."""
    import pyvis

    lib = Path(pyvis.__file__).resolve().parent / "lib" / "vis-9.1.2"
    css = (lib / "vis-network.css").read_text(encoding="utf-8")
    js = (lib / "vis-network.min.js").read_text(encoding="utf-8")
    return f"<style>{css}</style>", f"<script>{js}</script>"

# Paleta por tema (mismo orden cromatico que el resto del proyecto)
TEMA_COLORS = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
    "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
    "#aec7e8", "#ffbb78", "#98df8a",
]

# Color para cuentas (gris neutro distinguible)
CUENTA_COLOR = "#555555"
# Color de aristas cuenta->palabra
ARISTA_CUENTA_COLOR = "#999999"

# Paleta por plataforma
PLATAFORMA_COLORS = {
    "YouTube": "#ff0000",
    "Twitter": "#1da1f2",
    "X": "#000000",
    "Medios": "#666666",
    "Facebook": "#1877f2",
    "Instagram": "#e1306c",
    "TikTok": "#ff0050",
}


def cargar_nodos(path):
    """palabra -> {tema, color_tema, grado_total, rol, etc.}"""
    out = {}
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            palabra = row["palabra"].strip()
            if not palabra:
                continue
            out[palabra] = {
                "tema": int(row["tema_id"]),
                "color_tema": row.get("color_tema") or TEMA_COLORS[int(row["tema_id"]) % len(TEMA_COLORS)],
                "grado": int(row["grado_total"]),
                "rol": row["rol"],
                "sub": int(row["sub_id"]),
            }
    return out


def cargar_aristas(path, palabras_set, peso_min=1):
    """(source, target, weight, tipo, color)"""
    out = []
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            s = row["source"].strip()
            t = row["target"].strip()
            w = float(row["weight"])
            if w < peso_min:
                continue
            if s not in palabras_set or t not in palabras_set:
                continue
            tipo = row.get("tipo", "intra_sub")
            # Color segun tipo
            if tipo == "intra_sub":
                color = "#4daf4a"  # verde
            elif tipo == "intra_cluster":
                color = "#377eb8"  # azul
            else:  # inter / extra
                color = "#e41a1c"  # rojo
            out.append({"source": s, "target": t, "weight": w, "tipo": tipo, "color": color})
    return out


def cargar_cuentas_resumen(path, min_msgs=5):
    """usuario -> {plataformas, n_msgs, n_palabras, ...}"""
    out = {}
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                n_msgs = int(row["n_msgs"])
            except (ValueError, TypeError):
                continue
            if n_msgs < min_msgs:
                continue
            usuario = row["usuario"].strip()
            if not usuario:
                continue
            plataformas = (row.get("plataformas") or "").split("|")
            out[usuario] = {
                "plataformas": [p for p in plataformas if p],
                "n_msgs": n_msgs,
                "n_palabras": int(row.get("n_palabras") or 0),
                "tema_dom": row.get("tema_dominante", ""),
                "sub_dom": row.get("sub_dominante", ""),
            }
    return out


def cargar_palabras_x_cuenta(path, cuentas_set):
    """
    usuario -> [(palabra, conteo, tema, sub, plataforma), ...]  ordenado por conteo desc
    """
    out = defaultdict(list)
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            usuario = row["usuario"].strip()
            if usuario not in cuentas_set:
                continue
            palabra = row["palabra"].strip()
            conteo = int(row["conteo"])
            tema = int(row["tema"])
            try:
                sub = int(row["sub"])
            except (ValueError, TypeError):
                sub = -1
            plataforma = row.get("plataforma", "")
            out[usuario].append({
                "palabra": palabra, "conteo": conteo, "tema": tema,
                "sub": sub, "plataforma": plataforma,
            })
    # Ordenar por conteo desc
    for u in out:
        out[u].sort(key=lambda x: -x["conteo"])
    return out


def top_cuentas(cuentas, palabras_x_cuenta, palabras_validas, n_top=500, k_palabras=5, peso_min=1):
    """
    Selecciona top n cuentas por n_msgs y devuelve:
      - cuentas_sel: dict con metadata
      - aristas_cuenta: lista de {cuenta, palabra, peso}
    palabras_validas: set de palabras que están en el grafo (nodos_palabras)
    """
    # Ordena por n_msgs desc
    orden = sorted(cuentas.items(), key=lambda x: -x[1]["n_msgs"])[:n_top]
    cuentas_sel = dict(orden)

    aristas_cuenta = []
    for usuario in cuentas_sel:
        pals = palabras_x_cuenta.get(usuario, [])
        # Filtrar por: palabra debe estar en el grafo + peso minimo + top K
        pals = [p for p in pals if p["palabra"] in palabras_validas and p["conteo"] >= peso_min]
        pals = pals[:k_palabras]
        for p in pals:
            aristas_cuenta.append({
                "cuenta": usuario,
                "palabra": p["palabra"],
                "peso": p["conteo"],
                "tema_palabra": p["tema"],
            })
    return cuentas_sel, aristas_cuenta


def plataformas_unicas(cuentas_sel):
    """Set ordenado de plataformas presentes en las cuentas seleccionadas"""
    plats = set()
    for c in cuentas_sel.values():
        plats.update(c["plataformas"])
    return sorted(plats)


def build_pyvis_html(nodos_palabras, aristas_palabras, cuentas_sel,
                     aristas_cuenta, plataformas):
    """
    Construye el HTML final. Patron limpio: panel HTML puro al inicio,
    un solo <script> con todo el JS al final del body (despues de pyvis).
    """
    vis_css, vis_js = vis_assets()
    # Serializar data para JS
    # Nodos: id, label, title, color, size, type, group, value
    nodes_json = []
    # Palabras primero
    for palabra, meta in nodos_palabras.items():
        nodes_json.append({
            "id": palabra,
            "label": palabra,
            "title": f"<b>{palabra}</b> (palabra) — tema T{meta['tema']:02d}",
            "color": meta["color_tema"],
            "size": 18 + min(20, meta["grado"] // 3),
            "group": f"tema_{meta['tema']}",
            "kind": "palabra",
            "rol": meta["rol"],
            "sub": meta["sub"],
            "grado": meta["grado"],
            "tema": meta["tema"],
        })
    # Cuentas (con prefijo "c_" para evitar choques con ids de palabras)
    for usuario, meta in cuentas_sel.items():
        # Color: gris por defecto, o color de plataforma si solo usa una
        if len(meta["plataformas"]) == 1:
            color = PLATAFORMA_COLORS.get(meta["plataformas"][0], CUENTA_COLOR)
            plataforma_label = meta["plataformas"][0]
        else:
            color = CUENTA_COLOR
            plataforma_label = "|".join(meta["plataformas"])
        nodes_json.append({
            "id": f"c_{usuario}",
            "label": usuario,
            "title": f"<b>{usuario}</b> (cuenta) — {meta['n_msgs']} msgs en {plataforma_label}",
            "color": {"background": color, "border": "#222"},
            "shape": "box",
            "size": 14 + min(20, meta["n_msgs"] // 3),
            "group": "cuenta",
            "kind": "cuenta",
            "n_msgs": meta["n_msgs"],
            "plataformas": meta["plataformas"],
            "tema_dom": meta["tema_dom"],
        })

    edges_json = []
    for a in aristas_palabras:
        edges_json.append({
            "from": a["source"],
            "to": a["target"],
            "value": a["weight"],
            "color": {"color": a["color"], "opacity": 0.4},
            "title": f"{a['weight']:.0f} coocurrencias ({a['tipo']})",
            "kind": "palabra",
        })
    for a in aristas_cuenta:
        edges_json.append({
            "from": f"c_{a['cuenta']}",
            "to": a["palabra"],
            "value": a["peso"],
            "color": {"color": ARISTA_CUENTA_COLOR, "opacity": 0.6},
            "title": f"uso: {a['peso']} veces (cuenta → palabra)",
            "kind": "cuenta",
            "dashes": True,
        })

    # HTML template
    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Red historica de Tampico - palabras y cuentas</title>
{vis_css}
{vis_js}
<style>
  body {{ margin: 0; font-family: -apple-system, sans-serif; background: #0a0a0a; color: #eee; }}
  #topBar {{
    position: fixed; top: 0; left: 0; right: 0; z-index: 10;
    background: rgba(20,20,20,0.95); padding: 10px 20px;
    display: flex; gap: 24px; align-items: center; flex-wrap: wrap;
    border-bottom: 1px solid #333; max-height: 70vh; overflow-y: auto;
  }}
  #topBar h4 {{ margin: 8px 0 4px; font-size: 11px; color: #aaa; text-transform: uppercase; }}
  #topBar .grp {{
    background: rgba(255,255,255,0.05); padding: 8px 12px;
    border-radius: 6px; min-width: 180px;
  }}
  #topBar label {{ display: block; font-size: 12px; margin: 3px 0; cursor: pointer; }}
  #topBar input[type=range] {{ width: 140px; vertical-align: middle; }}
  #topBar .swatch {{ display: inline-block; width: 10px; height: 10px; border-radius: 2px; margin-right: 4px; vertical-align: middle; }}
  #statsBar {{
    position: fixed; bottom: 0; left: 0; right: 0; z-index: 10;
    background: rgba(20,20,20,0.95); padding: 6px 20px;
    border-top: 1px solid #333; font-size: 12px;
  }}
  #mynetwork {{
    position: fixed; top: 0; left: 0; right: 0; bottom: 0;
    width: 100vw; height: 100vh; background: #0a0a0a;
  }}
  #nodeTooltip {{
    position: fixed; pointer-events: none; z-index: 20;
    background: #1a1a1a; color: #fff; padding: 8px 10px;
    border-radius: 6px; border: 1px solid #444;
    font-size: 12px; max-width: 300px; display: none;
  }}
  a.bak {{ color: #4daf4a; }}
</style>
</head>
<body>

<div id="topBar">
  <div class="grp" style="background: #1a2a1a;">
    <a class="bak" href="red_tampico_historico.html">volver a red de palabras</a>
  </div>

  <div class="grp">
    <h4>Mostrar</h4>
    <label><input type="checkbox" id="showCuentas" checked> Cuentas (top por msgs)</label>
    <label><input type="checkbox" id="showPalabras" checked> Palabras</label>
  </div>

  <div class="grp">
    <h4>Cuentas</h4>
    <label>Selección calculada al generar la red.</label>
    <label>Los filtros visibles sí actúan sobre nodos y conexiones.</label>
  </div>

  <div class="grp">
    <h4>Plataforma (cuentas)</h4>
    {''.join(f'<label><input type="checkbox" class="plat" data-plat="{p}" checked> <span class="swatch" style="background:{PLATAFORMA_COLORS.get(p, CUENTA_COLOR)}"></span> {p}</label>' for p in plataformas)}
  </div>

  <div class="grp">
    <h4>Palabras (por tipo)</h4>
    <label><input type="checkbox" class="rol" data-rol="broker" checked> <span class="swatch" style="background:#d62728"></span> Puentes (broker)</label>
    <label><input type="checkbox" class="rol" data-rol="hub_endogamico" checked> <span class="swatch" style="background:#ff7f0e"></span> Núcleo (hub)</label>
    <label><input type="checkbox" class="rol" data-rol="conector_provincial" checked> <span class="swatch" style="background:#2ca02c"></span> Conector local</label>
    <label><input type="checkbox" class="rol" data-rol="periferico" checked> <span class="swatch" style="background:#7f7f7f"></span> Periférico</label>
  </div>

  <div class="grp">
    <h4>Conexiones palabras</h4>
    <label><input type="checkbox" class="edge" data-edge="#4daf4a" checked> <span class="swatch" style="background:#4daf4a"></span> Vecinas (verde)</label>
    <label><input type="checkbox" class="edge" data-edge="#377eb8" checked> <span class="swatch" style="background:#377eb8"></span> Mismo tema (azul)</label>
    <label><input type="checkbox" class="edge" data-edge="#e41a1c" checked> <span class="swatch" style="background:#e41a1c"></span> Entre temas (rojo)</label>
  </div>

  <div class="grp">
    <h4>Visual</h4>
    <label>Separación: <span id="springV">80</span>
      <input type="range" id="spring" min="20" max="400" value="80"></label>
    <label>Reorganizar
      <button id="resetPhysics" style="margin-left:6px">↻</button></label>
  </div>
</div>

<div id="statsBar">
  <b>palabras</b>: <span id="visNodos">0</span> |
  <b>cuentas</b>: <span id="visCtas">0</span> |
  <b>conexiones</b>: <span id="visEdges">0</span>
</div>

<div id="mynetwork"></div>
<div id="nodeTooltip"></div>

<!-- DATA INYECTADA -->
<script>
window.__NODES__ = {json.dumps(nodes_json, ensure_ascii=False)};
window.__EDGES__ = {json.dumps(edges_json, ensure_ascii=False)};
window.__PALABRAS_INDEX__ = {json.dumps({p: True for p in nodos_palabras}, ensure_ascii=False)};
</script>

<!-- APP -->
<script>
(function() {{
  // === DATA ===
  var ALL_NODES = new vis.DataSet(window.__NODES__);
  var ALL_EDGES = new vis.DataSet(window.__EDGES__);
  var PALABRAS_SET = window.__PALABRAS_INDEX__;
  // Index cuenta -> [edge ids que la involucran]
  var EDGE_BY_NODE = {{}};
  ALL_EDGES.forEach(function(e) {{
    EDGE_BY_NODE[e.from] = EDGE_BY_NODE[e.from] || [];
    EDGE_BY_NODE[e.to] = EDGE_BY_NODE[e.to] || [];
    EDGE_BY_NODE[e.from].push(e.id);
    EDGE_BY_NODE[e.to].push(e.id);
  }});
  // Index cuenta -> palabras
  var CUENTA_PALABRAS = {{}};
  ALL_EDGES.forEach(function(e) {{
    if (e.kind === 'cuenta') {{
      // e.from = cuenta, e.to = palabra
      CUENTA_PALABRAS[e.from] = CUENTA_PALABRAS[e.from] || [];
      CUENTA_PALABRAS[e.from].push(e.to);
    }}
  }});

  // === NETWORK ===
  var container = document.getElementById('mynetwork');
  var data = {{ nodes: ALL_NODES, edges: ALL_EDGES }};
  var options = {{
    nodes: {{ borderWidth: 1 }},
    edges: {{
      smooth: {{ type: 'continuous' }},
      font: {{ size: 0 }}
    }},
    physics: {{
      enabled: true,
      barnesHut: {{ gravitationalConstant: -8000, springLength: 80, springConstant: 0.04 }},
      stabilization: {{ iterations: 200 }}
    }},
    interaction: {{
      hover: true, tooltipDelay: 100,
      navigationButtons: true, keyboard: {{ enabled: false }}
    }}
  }};
  var network = new vis.Network(container, data, options);
  window.network = network;  // para debug

  // === TOOLTIP ===
  var tt = document.getElementById('nodeTooltip');
  var ttNodeId = null;
  function showTooltip(nodeId, x, y) {{
    var n = ALL_NODES.get(nodeId);
    if (!n) return;
    var lines = [];
    lines.push('<b>' + n.label + '</b>');
    if (n.kind === 'palabra') {{
      lines.push('palabra — tema T' + n.tema.toString().padStart(2, '0') +
                 ' / sub S' + n.sub + ' — ' + n.grado + ' enlaces');
      lines.push('rol: ' + n.rol.replace('_', ' '));
      // top 3 cuentas que la usan
      var ctas = [];
      ALL_EDGES.forEach(function(e) {{
        if (e.kind === 'cuenta' && e.to === n.id) {{
          ctas.push({{ user: e.from.replace(/^c_/, ''), peso: e.value }});
        }}
      }});
      ctas.sort(function(a, b) {{ return b.peso - a.peso; }});
      if (ctas.length) {{
        lines.push('top cuentas: ' + ctas.slice(0, 3).map(function(c) {{
          return c.user + ' (' + c.peso + ')';
        }}).join(', '));
      }}
    }} else {{
      lines.push('cuenta — ' + n.n_msgs + ' mensajes');
      lines.push('plataformas: ' + n.plataformas.join(', '));
      if (n.tema_dom) lines.push('tema dominante: ' + n.tema_dom);
      // top 3 palabras que usa
      var pals = [];
      ALL_EDGES.forEach(function(e) {{
        if (e.kind === 'cuenta' && e.from === n.id) {{
          pals.push({{ palabra: e.to, peso: e.value }});
        }}
      }});
      pals.sort(function(a, b) {{ return b.peso - a.peso; }});
      if (pals.length) {{
        lines.push('top palabras: ' + pals.slice(0, 5).map(function(p) {{
          return p.palabra + ' (' + p.peso + ')';
        }}).join(', '));
      }}
    }}
    tt.innerHTML = lines.join('<br>');
    tt.style.display = 'block';
    tt.style.left = Math.min(x + 14, window.innerWidth - 320) + 'px';
    tt.style.top = Math.min(y + 14, window.innerHeight - 100) + 'px';
    ttNodeId = nodeId;
  }}
  function hideTooltip() {{ tt.style.display = 'none'; ttNodeId = null; }}
  network.on('hoverNode', function(p) {{
    showTooltip(p.node, p.pointer.DOM.x, p.pointer.DOM.y);
  }});
  network.on('blurNode', hideTooltip);
  network.on('dragEnd', hideTooltip);
  network.on('zoom', hideTooltip);
  network.on('click', function(p) {{
    if (p.nodes.length) showTooltip(p.nodes[0], p.pointer.DOM.x, p.pointer.DOM.y);
    else hideTooltip();
  }});

  // === FILTROS ===
  function getActiveRols() {{
    var s = {{}};
    document.querySelectorAll('input.rol:checked').forEach(function(c) {{
      s[c.dataset.rol] = true;
    }});
    return s;
  }}
  function getActiveEdges() {{
    var s = {{}};
    document.querySelectorAll('input.edge:checked').forEach(function(c) {{
      s[c.dataset.edge.toLowerCase()] = true;
    }});
    return s;
  }}
  function getActivePlats() {{
    var s = {{}};
    document.querySelectorAll('input.plat:checked').forEach(function(c) {{
      s[c.dataset.plat] = true;
    }});
    return s;
  }}
  function getShowFlags() {{
    return {{
      cuentas: document.getElementById('showCuentas').checked,
      palabras: document.getElementById('showPalabras').checked,
    }};
  }}

  function rebuildVisibility() {{
    var rols = getActiveRols();
    var edges = getActiveEdges();
    var plats = getActivePlats();
    var show = getShowFlags();

    // Visibilidad de nodos
    var nodeUpdates = [];
    var nPal = 0, nCta = 0;
    ALL_NODES.forEach(function(n) {{
      var visible = true;
      if (n.kind === 'palabra') {{
        visible = show.palabras && rols[n.rol];
        if (visible) nPal++;
      }} else {{
        // cuenta: visible si su plataforma esta activa y show.cuentas
        var ctaPlats = n.plataformas;
        var anyPlat = ctaPlats.some(function(p) {{ return plats[p]; }});
        visible = show.cuentas && anyPlat;
        if (visible) nCta++;
      }}
      if (n.hidden === visible) {{  // solo actualizo si cambia
        // no-op
      }}
      nodeUpdates.push({{ id: n.id, hidden: !visible }});
    }});
    ALL_NODES.update(nodeUpdates);

    // Visibilidad de aristas
    var edgeUpdates = [];
    var nEdge = 0;
    ALL_EDGES.forEach(function(e) {{
      var fromVisible = !ALL_NODES.get(e.from).hidden;
      var toVisible = !ALL_NODES.get(e.to).hidden;
      var visible = fromVisible && toVisible;
      if (e.kind === 'palabra') {{
        // filtro por color de arista
        var c = e.color && e.color.color ? e.color.color.toLowerCase() : '';
        if (!edges[c]) visible = false;
      }}
      // aristas cuenta->palabra: si no hay visibilidad de cuenta, igual oculto
      if (visible) nEdge++;
      edgeUpdates.push({{ id: e.id, hidden: !visible }});
    }});
    ALL_EDGES.update(edgeUpdates);

    document.getElementById('visNodos').textContent = nPal;
    document.getElementById('visCtas').textContent = nCta;
    document.getElementById('visEdges').textContent = nEdge;
  }}

  // Listeners
  document.querySelectorAll('input.rol, input.edge, input.plat').forEach(function(c) {{
    c.addEventListener('change', rebuildVisibility);
  }});
  document.getElementById('showCuentas').addEventListener('change', rebuildVisibility);
  document.getElementById('showPalabras').addEventListener('change', rebuildVisibility);

  // Sliders de visual
  document.getElementById('spring').addEventListener('input', function(e) {{
    document.getElementById('springV').textContent = e.target.value;
    network.setOptions({{ physics: {{ barnesHut: {{ springLength: parseInt(e.target.value) }} }} }});
  }});
  document.getElementById('resetPhysics').addEventListener('click', function() {{
    network.setOptions({{ physics: {{ enabled: true, stabilization: {{ iterations: 200 }} }} }});
    network.stabilize(200);
  }});
  // Init
  window.__rebuild = rebuildVisibility;  // debug
  rebuildVisibility();
  // Stabilize hook
  network.once('stabilizationIterationsDone', function() {{
    rebuildVisibility();
  }});
}})();
</script>
</body>
</html>
"""
    return html


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-dir", type=Path, default=DEFAULT_BASE)
    ap.add_argument("--top-n", type=int, default=500)
    ap.add_argument("--min-msgs", type=int, default=5)
    ap.add_argument("--top-k", type=int, default=5)
    ap.add_argument("--peso-min", type=int, default=1)
    ap.add_argument("--peso-arista", type=float, default=4.0,
                    help="peso minimo para aristas palabra-palabra (filtro base)")
    ap.add_argument("--diccionario-temas", type=Path, default=DEFAULT_TOPIC_DICTIONARY)
    ap.add_argument("--diccionario-positivo", type=Path, default=DEFAULT_POSITIVE_DICTIONARY)
    ap.add_argument("--diccionario-negativo", type=Path, default=DEFAULT_NEGATIVE_DICTIONARY)
    args = ap.parse_args()

    base = args.base_dir
    nodos_path = base / "clusters" / "red_completa" / "nodos_metricas.csv"
    aristas_path = base / "clusters" / "red_completa" / "aristas_clasificadas.csv"
    ctas_res_path = base / "cuentas_clusters" / "cuentas_resumen.csv"
    palxcta_path = base / "cuentas_clusters" / "palabras_x_cuenta.csv"
    out_dir = base / "clusters" / "red_guiada"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_html = out_dir / "red_tampico_cuentas_guiada.html"
    out_json = out_dir / "metricas_cuentas_guiada.json"

    print(f"[1/5] Cargando nodos (palabras) desde {nodos_path}")
    nodos = cargar_nodos(nodos_path)
    print(f"      {len(nodos)} palabras")

    print(f"[2/5] Cargando aristas (palabra-palabra) desde {aristas_path} (peso>={args.peso_arista})")
    aristas = cargar_aristas(aristas_path, set(nodos.keys()), peso_min=args.peso_arista)
    print(f"      {len(aristas)} aristas tras filtro")

    print(f"[3/5] Cargando cuentas (>= {args.min_msgs} msgs) desde {ctas_res_path}")
    todas_cuentas = cargar_cuentas_resumen(ctas_res_path, min_msgs=args.min_msgs)
    print(f"      {len(todas_cuentas)} cuentas pasan el filtro de msgs")

    print(f"[4/5] Cargando palabras_x_cuenta desde {palxcta_path}")
    # Pre-filtrar set de cuentas para que cargar_palabras_x_cuenta sea rapido
    palxcta = cargar_palabras_x_cuenta(palxcta_path, set(todas_cuentas.keys()))

    print(f"[4b] Seleccionando top {args.top_n} cuentas por msgs, top {args.top_k} palabras")
    # Set de palabras validas: solo las que estan en el grafo
    palabras_validas = set(nodos.keys())
    print(f"      {len(palabras_validas)} palabras validas en el grafo")

    cuentas_sel, aristas_cuenta = top_cuentas(
        todas_cuentas, palxcta, palabras_validas, n_top=args.top_n, k_palabras=args.top_k, peso_min=args.peso_min
    )
    print(f"      {len(cuentas_sel)} cuentas, {len(aristas_cuenta)} aristas cuenta->palabra")

    plataformas = plataformas_unicas(cuentas_sel)
    print(f"      plataformas presentes: {plataformas}")

    print(f"[5/5] Generando HTML en {out_html}")
    html = build_pyvis_html(nodos, aristas, cuentas_sel, aristas_cuenta, plataformas)
    out_html.write_text(html, encoding="utf-8")

    print("      aplicando temas rastreados y polaridad")
    lexicons = load_lexicons(
        args.diccionario_temas,
        args.diccionario_positivo,
        args.diccionario_negativo,
    )
    annotations = annotate_words(nodos.keys(), lexicons)
    for usuario in cuentas_sel:
        weighted_words = [
            (row["palabra"], row["conteo"])
            for row in palxcta.get(usuario, [])
        ]
        annotations[f"c_{usuario}"] = aggregate_words(
            weighted_words,
            lexicons,
            kind="cuenta",
            label=usuario,
        )
    inject_guided_layer(
        out_html,
        annotations,
        lexicons.category_colors,
        "Cuentas, temas rastreados y polaridad",
        panel_top_px=228,
    )
    write_annotation_outputs(
        out_dir,
        annotations,
        "cuentas_guiadas",
        {
            "temas": args.diccionario_temas,
            "positivas": args.diccionario_positivo,
            "negativas": args.diccionario_negativo,
        },
    )
    print(f"      tamano: {out_html.stat().st_size} bytes")

    metrics = {
        "corpus": "historico consolidado de Tampico",
        "n_palabras": len(nodos),
        "n_aristas_palabra": len(aristas),
        "n_cuentas_sel": len(cuentas_sel),
        "n_aristas_cuenta": len(aristas_cuenta),
        "plataformas": plataformas,
        "params": {
            key: str(value) if isinstance(value, Path) else value
            for key, value in vars(args).items()
        },
    }
    out_json.write_text(json.dumps(metrics, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"      metricas en {out_json}")
    print("OK.")


if __name__ == "__main__":
    main()
