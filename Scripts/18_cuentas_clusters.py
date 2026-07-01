"""
Fase 4b: Mapea cuentas de usuario a temas LDA y subclusters segun
las palabras que escriben en sus mensajes.

Entradas:
  - SNA/Resultados/historico/clusters/documentos_lematizados.csv
  - SNA/Resultados/historico/clusters/lda_asignacion.csv
  - SNA/Resultados/historico/clusters/subclusters/subclusters_palabras.csv
      (palabra -> tema, sub)

Salidas (en SNA/Resultados/historico/cuentas_clusters/):
  - cuentas_x_tema.csv          (usuario x T00..T12, con conteo y %)
  - cuentas_x_subcluster.csv    (usuario x T00_S0, T00_S1, ...)
  - cuentas_resumen.csv         (1 fila por usuario: tema y subcluster dominante,
                                 # mensajes, # palabras clasificadas)
  - reporte_fase4_cuentas.md    (Top 10 habitantes por tema y por sub)
  - cuentas_sin_tema.csv        (usuarios con 0 palabras clasificadas — ojo)

Como cuenta:
  Para cada mensaje del usuario usa los lemas generados por la fase LDA y cuenta cada
  palabra del vocabulario LDA. Suma por usuario -> vector de pesos sobre
  temas y subclusters. La "pertenencia" se mide en #palabras_clasificadas
  dentro del tema (no en #mensajes: un mensaje largo cuenta mas).

La identidad de cuenta es plataforma::usuario para evitar unir homonimos.

Parametros CLI:
  --top-n N      (default 10)  cuantos habitantes listar en el reporte
  --min-palabras N  (default 5)  filtra usuarios con menos de N palabras
                                clasificadas del reporte (aparecen igual
                                en el CSV completo, solo se omiten del top)
"""

import argparse
import csv
import json
import re
from collections import defaultdict
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CLUSTERS = REPO_ROOT / "SNA" / "Resultados" / "historico" / "clusters"

# ------------------------------------------------------------------
# Tokenizacion: misma logica que el resto del pipeline
# (split por no-alfanumerico, lower, min 3 chars)
# ------------------------------------------------------------------
TOKEN_RE = re.compile(r"[a-záéíóúüñ]{3,}", re.IGNORECASE)


def tokenizar(texto):
    if not texto:
        return []
    return TOKEN_RE.findall(texto.lower())


def cargar_lda(path):
    """palabra -> {tema, peso}"""
    m = {}
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            t = row["termino"].strip().lower()
            if not t:
                continue
            m[t] = {
                "tema": int(row["tema_id"]),
                "peso": float(row["peso"]),
            }
    return m


def cargar_subclusters(path):
    """palabra -> {tema, sub, peso}"""
    m = {}
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            t = row["palabra"].strip().lower()
            if not t:
                continue
            m[t] = {
                "tema": int(row["tema_id"]),
                "sub": int(row["sub_id"]),
                "peso": float(row["peso_lda"]) if row["peso_lda"] else 0.0,
            }
    return m


def cargar_mensajes(path):
    """Carga los lemas por mensaje conservando cuenta y plataforma."""
    out = []
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            usuario = (row.get("usuario") or "").strip()
            if not usuario:
                continue
            out.append({
                "usuario": usuario,
                "plataforma": (row.get("plataforma") or "").strip(),
                "texto": (row.get("lemas") or row.get("texto_limpio") or "").strip(),
                "es_reply": (row.get("es_reply") or "").strip().lower() in ("1", "true", "si", "sí"),
            })
    return out


def acumular(mensajes, lda, subs):
    """
    usuario -> {
        'plataformas': set,
        'n_msgs': int,
        'n_palabras_clasificadas': int,
        'por_tema': {tema: conteo_palabras},
        'por_sub':  {(tema, sub): conteo},
        'por_tema_pesos': {tema: suma_pesos_lda},
        'por_sub_pesos':  {(tema, sub): suma_pesos_lda},
    }
    """
    agg = {}
    for m in mensajes:
        u = f"{m['plataforma']}::{m['usuario']}"
        if u not in agg:
            agg[u] = {
                "plataformas": set(),
                "n_msgs": 0,
                "n_palabras_clasificadas": 0,
                "por_tema": defaultdict(int),
                "por_sub": defaultdict(int),
                "por_tema_pesos": defaultdict(float),
                "por_sub_pesos": defaultdict(float),
                "palabras": defaultdict(int),  # palabra -> conteo
            }
        a = agg[u]
        a["plataformas"].add(m["plataforma"])
        a["n_msgs"] += 1
        # Tokeniza y busca cada token en lda/subs
        toks = tokenizar(m["texto"])
        for tok in toks:
            if tok in subs:
                meta = subs[tok]
                a["por_sub"][(meta["tema"], meta["sub"])] += 1
                a["por_tema"][meta["tema"]] += 1
                a["por_sub_pesos"][(meta["tema"], meta["sub"])] += meta["peso"]
                a["por_tema_pesos"][meta["tema"]] += meta["peso"]
                a["n_palabras_clasificadas"] += 1
                a["palabras"][tok] += 1
            elif tok in lda:
                meta = lda[tok]
                a["por_tema"][meta["tema"]] += 1
                a["por_tema_pesos"][meta["tema"]] += meta["peso"]
                a["n_palabras_clasificadas"] += 1
                a["palabras"][tok] += 1
            # tokens sin asignacion: ruido, no se cuentan
    return agg


def escribir_cuentas_x_tema(agg, temas, out_path):
    """
    CSV ancho: filas=usuarios, cols=T00..T12 (conteo + %)
    Columnas: usuario, plataforma, n_msgs, n_palabras,
              T00, T01, ..., T12,
              T00_pct, ..., T12_pct,
              tema_dominante, peso_dominante
    """
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        header = ["usuario", "plataforma", "n_msgs", "n_palabras"]
        for t in temas:
            header.append(f"T{t:02d}")
        for t in temas:
            header.append(f"T{t:02d}_pct")
        header += ["tema_dominante", "peso_dominante"]
        w.writerow(header)
        for u in sorted(agg):
            a = agg[u]
            total = a["n_palabras_clasificadas"] or 1
            row = [u, "|".join(sorted(a["plataformas"])), a["n_msgs"], a["n_palabras_clasificadas"]]
            for t in temas:
                row.append(a["por_tema"].get(t, 0))
            for t in temas:
                pct = 100.0 * a["por_tema"].get(t, 0) / total
                row.append(f"{pct:.1f}")
            # dominante
            if a["por_tema"]:
                dom_t = max(a["por_tema"], key=a["por_tema"].get)
                dom_w = a["por_tema"][dom_t]
            else:
                dom_t, dom_w = -1, 0
            row += [f"T{dom_t:02d}" if dom_t >= 0 else "", dom_w]
            w.writerow(row)


def escribir_cuentas_x_subcluster(agg, tema_sub_pairs, out_path):
    """
    CSV ancho por subcluster. Columnas: T00_S0, T00_S1, ...
    """
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        header = ["usuario", "plataforma", "n_msgs", "n_palabras"]
        for (t, s) in tema_sub_pairs:
            header.append(f"T{t:02d}_S{s}")
        for (t, s) in tema_sub_pairs:
            header.append(f"T{t:02d}_S{s}_pct")
        header += ["sub_dominante", "conteo_dominante"]
        w.writerow(header)
        for u in sorted(agg):
            a = agg[u]
            total = a["n_palabras_clasificadas"] or 1
            row = [u, "|".join(sorted(a["plataformas"])), a["n_msgs"], a["n_palabras_clasificadas"]]
            for (t, s) in tema_sub_pairs:
                row.append(a["por_sub"].get((t, s), 0))
            for (t, s) in tema_sub_pairs:
                pct = 100.0 * a["por_sub"].get((t, s), 0) / total
                row.append(f"{pct:.1f}")
            if a["por_sub"]:
                dom_key = max(a["por_sub"], key=a["por_sub"].get)
                dom_w = a["por_sub"][dom_key]
                dom_str = f"T{dom_key[0]:02d}_S{dom_key[1]}"
            else:
                dom_str, dom_w = "", 0
            row += [dom_str, dom_w]
            w.writerow(row)


def escribir_resumen(agg, out_path):
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "usuario", "plataformas", "n_msgs", "n_palabras",
            "tema_dominante", "tema_pct",
            "sub_dominante", "sub_pct",
            "n_temas_distintos", "n_subs_distintos",
        ])
        for u in sorted(agg):
            a = agg[u]
            total = a["n_palabras_clasificadas"] or 1
            if a["por_tema"]:
                dom_t = max(a["por_tema"], key=a["por_tema"].get)
                dom_t_pct = 100.0 * a["por_tema"][dom_t] / total
                tema_str = f"T{dom_t:02d}"
            else:
                tema_str, dom_t_pct = "", 0.0
            if a["por_sub"]:
                dom_key = max(a["por_sub"], key=a["por_sub"].get)
                dom_s_pct = 100.0 * a["por_sub"][dom_key] / total
                sub_str = f"T{dom_key[0]:02d}_S{dom_key[1]}"
            else:
                sub_str, dom_s_pct = "", 0.0
            w.writerow([
                u, "|".join(sorted(a["plataformas"])),
                a["n_msgs"], a["n_palabras_clasificadas"],
                tema_str, f"{dom_t_pct:.1f}",
                sub_str, f"{dom_s_pct:.1f}",
                len(a["por_tema"]), len(a["por_sub"]),
            ])


def escribir_palabras_x_cuenta(agg, lda, subs, out_path, min_palabras=5):
    """
    Una fila por (cuenta, palabra) con conteo.
    Solo incluye cuentas con >= min_palabras clasificadas
    (las que no tienen tema/sub claro se omiten para no inflar).
    Sirve para Fase 5 (grafo cuenta->palabra) sin re-tokenizar.
    Columnas: usuario, palabra, conteo, tema, sub, plataforma
    """
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["usuario", "palabra", "conteo", "tema", "sub", "plataforma"])
        filas = 0
        for u in sorted(agg):
            a = agg[u]
            if a["n_palabras_clasificadas"] < min_palabras:
                continue
            plataforma = "|".join(sorted(a["plataformas"]))
            # Para cada palabra de la cuenta, busca tema/sub (subs tiene prioridad
            # sobre lda porque incluye info de sub)
            for palabra, conteo in sorted(a["palabras"].items(), key=lambda x: -x[1]):
                if palabra in subs:
                    t, s = subs[palabra]["tema"], subs[palabra]["sub"]
                elif palabra in lda:
                    t = lda[palabra]["tema"]
                    s = -1
                else:
                    continue
                w.writerow([u, palabra, conteo, t, s, plataforma])
                filas += 1
        return filas


def escribir_sin_tema(agg, out_path):
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["usuario", "n_msgs", "ejemplo_texto"])
        for u in sorted(agg):
            a = agg[u]
            if a["n_palabras_clasificadas"] == 0:
                w.writerow([u, a["n_msgs"], ""])  # ejemplo: se puede llenar


def escribir_reporte_md(agg, temas, tema_sub_pairs, out_path, top_n=10, min_palabras=5):
    """
    Reporte markdown: por cada tema, top N cuentas 'habitantes' (% de
    palabras del usuario que caen en ese tema). Idem por subcluster.
    """
    lineas = ["# Fase 4: Mapeo de cuentas a temas y subclusters\n"]

    # Stats globales
    n_usuarios = len(agg)
    n_con_palabras = sum(1 for a in agg.values() if a["n_palabras_clasificadas"] >= min_palabras)
    lineas.append(f"- Usuarios unicos: {n_usuarios}")
    lineas.append(f"- Con >= {min_palabras} palabras clasificadas: {n_con_palabras}")
    lineas.append(f"- Temas: {len(temas)} (T{min(temas):02d}..T{max(temas):02d})")
    lineas.append(f"- Subclusters: {len(tema_sub_pairs)}\n")

    # Top habitantes por tema
    lineas.append("## Top habitantes por tema\n")
    lineas.append("Habitante = cuenta que mas % de sus palabras clasificadas "
                  "aporta a ese tema. Excluye cuentas con < "
                  f"{min_palabras} palabras.\n")
    for t in temas:
        ranking = []
        for u, a in agg.items():
            if a["n_palabras_clasificadas"] < min_palabras:
                continue
            c = a["por_tema"].get(t, 0)
            if c == 0:
                continue
            pct = 100.0 * c / a["n_palabras_clasificadas"]
            ranking.append((pct, c, u, a["n_msgs"]))
        ranking.sort(reverse=True)
        if not ranking:
            lineas.append(f"### T{t:02d}\n_sin cuentas con peso_\n")
            continue
        lineas.append(f"### T{t:02d} ({len(ranking)} cuentas aportan)\n")
        lineas.append("| # | cuenta | msgs | palabras_tema | %_del_usuario |")
        lineas.append("|---|--------|-----:|--------------:|--------------:|")
        for i, (pct, c, u, n_msgs) in enumerate(ranking[:top_n], 1):
            lineas.append(f"| {i} | {u} | {n_msgs} | {c} | {pct:.1f}% |")
        lineas.append("")

    # Top por subcluster
    lineas.append("## Top habitantes por subcluster\n")
    for (t, s) in tema_sub_pairs:
        ranking = []
        for u, a in agg.items():
            if a["n_palabras_clasificadas"] < min_palabras:
                continue
            c = a["por_sub"].get((t, s), 0)
            if c == 0:
                continue
            pct = 100.0 * c / a["n_palabras_clasificadas"]
            ranking.append((pct, c, u, a["n_msgs"]))
        ranking.sort(reverse=True)
        if not ranking:
            continue
        lineas.append(f"### T{t:02d}_S{s} ({len(ranking)} cuentas)\n")
        lineas.append("| # | cuenta | msgs | palabras_sub | %_del_usuario |")
        lineas.append("|---|--------|-----:|-------------:|--------------:|")
        for i, (pct, c, u, n_msgs) in enumerate(ranking[:top_n], 1):
            lineas.append(f"| {i} | {u} | {n_msgs} | {c} | {pct:.1f}% |")
        lineas.append("")

    Path(out_path).write_text("\n".join(lineas), encoding="utf-8")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--clusters-dir", type=Path, default=DEFAULT_CLUSTERS)
    ap.add_argument("--input-lemas", type=Path)
    ap.add_argument("--output-dir", type=Path)
    ap.add_argument("--top-n", type=int, default=10)
    ap.add_argument("--min-palabras", type=int, default=5)
    args = ap.parse_args()

    clusters = args.clusters_dir
    csv_path = args.input_lemas or clusters / "documentos_lematizados.csv"
    lda_path = clusters / "lda_asignacion.csv"
    sub_path = clusters / "subclusters" / "subclusters_palabras.csv"
    out_dir = args.output_dir or clusters.parent / "cuentas_clusters"
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[1/5] Cargando LDA desde {lda_path}")
    lda = cargar_lda(lda_path)
    print(f"      vocabulario LDA: {len(lda)} terminos, "
          f"temas: {sorted({v['tema'] for v in lda.values()})}")

    print(f"[2/5] Cargando subclusters desde {sub_path}")
    subs = cargar_subclusters(sub_path)
    print(f"      subclusters_palabras: {len(subs)} terminos")
    temas = sorted({v["tema"] for v in lda.values()})
    tema_sub_pairs = sorted({(v["tema"], v["sub"]) for v in subs.values()})
    print(f"      subclusters: {len(tema_sub_pairs)} (T00_S0..T{max(temas):02d}_S{max(s for (_, s) in tema_sub_pairs)})")

    print(f"[3/5] Cargando mensajes desde {csv_path}")
    mensajes = cargar_mensajes(csv_path)
    print(f"      mensajes con usuario: {len(mensajes)}")

    print("[4/5] Acumulando palabras por usuario...")
    agg = acumular(mensajes, lda, subs)
    n_usuarios = len(agg)
    n_sin_palabras = sum(1 for a in agg.values() if a["n_palabras_clasificadas"] == 0)
    print(f"      usuarios unicos: {n_usuarios}")
    print(f"      sin palabras clasificadas: {n_sin_palabras}")

    print(f"[5/5] Escribiendo salidas en {out_dir}/")
    escribir_cuentas_x_tema(agg, temas, out_dir / "cuentas_x_tema.csv")
    escribir_cuentas_x_subcluster(agg, tema_sub_pairs, out_dir / "cuentas_x_subcluster.csv")
    escribir_resumen(agg, out_dir / "cuentas_resumen.csv")
    n_palxc = escribir_palabras_x_cuenta(
        agg, lda, subs, out_dir / "palabras_x_cuenta.csv", min_palabras=args.min_palabras
    )
    escribir_sin_tema(agg, out_dir / "cuentas_sin_tema.csv")
    escribir_reporte_md(agg, temas, tema_sub_pairs, out_dir / "reporte_fase4_cuentas.md",
                        top_n=args.top_n, min_palabras=args.min_palabras)
    for fname in ["cuentas_x_tema.csv", "cuentas_x_subcluster.csv",
                  "cuentas_resumen.csv", "palabras_x_cuenta.csv",
                  "cuentas_sin_tema.csv", "reporte_fase4_cuentas.md"]:
        size = (out_dir / fname).stat().st_size
        extra = f"  ({n_palxc} filas)" if fname == "palabras_x_cuenta.csv" else ""
        print(f"      - {fname}  ({size} bytes){extra}")

    # Guardar metadata
    meta = {
        "corpus": "historico consolidado de Tampico",
        "n_mensajes": len(mensajes),
        "n_usuarios_unicos": n_usuarios,
        "n_usuarios_sin_palabras": n_sin_palabras,
        "n_temas": len(temas),
        "n_subs": len(tema_sub_pairs),
        "vocab_lda": len(lda),
        "vocab_subs": len(subs),
    }
    (out_dir / "metadata.json").write_text(json.dumps(meta, indent=2, ensure_ascii=False))
    print("OK.")


if __name__ == "__main__":
    main()
