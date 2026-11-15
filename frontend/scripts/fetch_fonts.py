"""Baja las fuentes del tema (OFL) para servirlas desde el sitio y no desde Bunny/jsDelivr.

    python -I frontend/scripts/fetch_fonts.py

Escribe frontend/public/fonts/*.woff2, sus licencias (OFL-*.txt) y
frontend/src/theme/fonts.css con los @font-face y sus unicode-range.
"""
import re
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "public" / "fonts"
CSS = ROOT / "src" / "theme" / "fonts.css"
BUNNY = ("https://fonts.bunny.net/css?family=libre-caslon-display:400|"
         "libre-caslon-text:400,400i,700|fira-code:400&display=swap")
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36"
# Subconjuntos que se usan: latin y latin-ext para Caslon (nombres comunes en muchos idiomas), solo latin para Fira (cifras).
WANT = {"Libre Caslon Display": {"latin", "latin-ext"}, "Libre Caslon Text": {"latin", "latin-ext"}, "Fira Code": {"latin"}}
LICENSES = {"libre-caslon-display": "https://cdn.jsdelivr.net/npm/@fontsource/libre-caslon-display@5.2.5/LICENSE",
            "libre-caslon-text": "https://cdn.jsdelivr.net/npm/@fontsource/libre-caslon-text@5.2.5/LICENSE",
            "fira-code": "https://cdn.jsdelivr.net/npm/@fontsource/fira-code@5.2.5/LICENSE"}


def get(url):
    with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": UA}), timeout=30) as r:
        return r.read()


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    OUT.mkdir(parents=True, exist_ok=True)
    css = get(BUNNY).decode("utf-8")
    faces = re.findall(r"/\* ([\w-]+) \*/\s*@font-face\s*\{(.*?)\}", css, re.S)
    out = ["/* Fuentes del tema, servidas desde /fonts (OFL; licencias junto a los archivos).",
           "   Generado por scripts/fetch_fonts.py: no editar a mano. */"]
    for subset, body in faces:
        family = re.search(r"font-family:\s*'([^']+)'", body).group(1)
        if subset not in WANT.get(family, ()):
            continue
        style = re.search(r"font-style:\s*(\w+)", body).group(1)
        weight = re.search(r"font-weight:\s*(\d+)", body).group(1)
        src = re.search(r"url\((https://[^)]+\.woff2)\)", body).group(1)
        rng = re.search(r"unicode-range:\s*([^;]+);", body).group(1).strip()
        name = src.rsplit("/fonts/", 1)[1].replace("@5.3.0/", "-").replace("/", "-")
        data = get(src)
        if not data.startswith(b"wOF2"):
            raise SystemExit(f"{src} no es woff2")
        (OUT / name).write_bytes(data)
        out.append(f"@font-face {{ font-family: '{family}'; font-style: {style}; font-weight: {weight}; font-display: swap;\n"
                   f"  src: url('/fonts/{name}') format('woff2'); unicode-range: {rng}; }}")
        print(f"{name:52} {len(data) // 1024:>4} KB")
    for slug, url in LICENSES.items():
        (OUT / f"OFL-{slug}.txt").write_bytes(get(url))
    CSS.write_text("\n".join(out) + "\n", encoding="utf-8")
    print("→", CSS)


if __name__ == "__main__":
    main()
