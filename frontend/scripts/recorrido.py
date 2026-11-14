"""Recorre las rutas del front contra la API real: errores de consola y HTTP, y una captura por ruta.

    python frontend/scripts/recorrido.py <carpeta> [light|dark] [ruta1,ruta2]

Necesita Vite en 5135, la API en 8035 (túnel) y Playwright de Python.
"""
import json, sys, urllib.request
from pathlib import Path
from playwright.sync_api import sync_playwright

BASE = "http://localhost:5135"
OUT = Path(sys.argv[1] if len(sys.argv) > 1 else "capturas")
MODO = sys.argv[2] if len(sys.argv) > 2 else "light"
SOLO = sys.argv[3].split(",") if len(sys.argv) > 3 else None
OUT.mkdir(exist_ok=True)
sys.stdout.reconfigure(encoding="utf-8")  # la consola de Windows es cp1252

def api(path):
    with urllib.request.urlopen(f"http://127.0.0.1:8035{path}") as r:
        return json.load(r)

turdidae = next(r["key"] for r in api("/search?q=Turdidae&limit=5")["results"] if r["rank"] == "family")
rutas = {
    "home": "/",
    "especie-cepola": "/species/2379498",
    "especie-robin": "/species/9510564",
    "familia-cepolidae": "/taxon/family/6755596",
    "reino-animalia": "/taxon/kingdom/1",
    "galeria-owstonia": "/taxon/genus/2379470/gallery",
    "sonidos-turdidae": f"/taxon/family/{turdidae}/sounds",
    "infografia-cepolidae": "/taxon/family/6755596/infographic",
    "busqueda": "/search?q=cepola",
    "mapa": "/map",
    "mapa-mx": "/map/country/MX",
}

with sync_playwright() as p:
    b = p.chromium.launch()
    for nombre, ruta in rutas.items():
        if SOLO and nombre not in SOLO:
            continue
        pg = b.new_page(viewport={"width": 1440, "height": 900})
        pg.add_init_script(f"localStorage.setItem('linneo-theme', '{MODO}')")
        errores = []
        pg.on("console", lambda m, e=errores: m.type == "error" and e.append("console: " + m.text[:200]))
        pg.on("pageerror", lambda x, e=errores: e.append("pageerror: " + str(x)[:200]))
        pg.on("response", lambda r, e=errores: r.status >= 400 and e.append(f"{r.status} {r.url[:120]}"))
        try:
            pg.goto(BASE + ruta, wait_until="networkidle", timeout=30000)
        except Exception as ex:
            errores.append("goto: " + str(ex)[:150])
        pg.wait_for_timeout(1500)
        pg.screenshot(path=str(OUT / f"{nombre}-{MODO}.png"))
        texto = pg.inner_text("body")[:160].replace("\n", " | ")
        print(f"## {nombre} {ruta}\n   texto: {texto}")
        for e in dict.fromkeys(errores):
            print("   ✗", e)
        pg.close()
    b.close()
