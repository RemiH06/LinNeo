"""Genera frontend/src/geo/world.json: los países como trazos SVG ya proyectados.

    python -I frontend/scripts/build_world.py <ne_50m_admin_0_countries.geojson>

Fuente: Natural Earth 1:50m, Admin 0 Countries (dominio público), v5.1.2:
https://raw.githubusercontent.com/nvkelso/natural-earth-vector/v5.1.2/geojson/ne_50m_admin_0_countries.geojson

Proyección Equal Earth (Šavrič, Patterson y Jenny, 2018): conserva áreas, así un
mapa de distribución no infla Groenlandia ni Rusia como Mercator. La simplificación
es Douglas-Peucker por anillo en coordenadas de pantalla (ancho W); se descartan
islas de menos de MIN_AREA px² salvo el anillo mayor de cada país. Solo usa la
biblioteca estándar.
"""
import json
import math
import sys
from pathlib import Path

W = 1000                 # ancho del lienzo en unidades SVG
TOL = 0.8                # tolerancia de simplificación, en unidades SVG
MIN_AREA = 1.5           # islas más chicas que esto (unidades²) se omiten
# Sin código ISO en Natural Earth; GBIF los cuenta dentro de su país.
MERGE = {"Somaliland": "SO", "N. Cyprus": "CY"}
SKIP = {"Siachen Glacier"}

A1, A2, A3, A4 = 1.340264, -0.081106, 0.000893, 0.003796
M = math.sqrt(3) / 2


def eqearth(lon, lat):
    lam, phi = math.radians(lon), math.radians(lat)
    t = math.asin(M * math.sin(phi))
    t2, t6 = t * t, t ** 6
    x = lam * math.cos(t) / (M * (A1 + 3 * A2 * t2 + t6 * (7 * A3 + 9 * A4 * t2)))
    y = t * (A1 + A2 * t2 + t6 * (A3 + A4 * t2))
    return x, y


X_MAX = eqearth(180, 0)[0]
Y_MAX = eqearth(0, 90)[1]
SCALE = W / (2 * X_MAX)
H = round(2 * Y_MAX * SCALE, 1)


def to_screen(lon, lat):
    x, y = eqearth(lon, lat)
    return (x + X_MAX) * SCALE, (Y_MAX - y) * SCALE


def dp(points, tol):
    """Douglas-Peucker iterativo."""
    if len(points) < 3:
        return points
    keep = [False] * len(points)
    keep[0] = keep[-1] = True
    stack = [(0, len(points) - 1)]
    while stack:
        a, b = stack.pop()
        (ax, ay), (bx, by) = points[a], points[b]
        dx, dy = bx - ax, by - ay
        norm = math.hypot(dx, dy) or 1e-12
        best, idx = 0.0, None
        for i in range(a + 1, b):
            px, py = points[i]
            d = abs(dy * px - dx * py + bx * ay - by * ax) / norm
            if d > best:
                best, idx = d, i
        if idx is not None and best > tol:
            keep[idx] = True
            stack += [(a, idx), (idx, b)]
    return [p for p, k in zip(points, keep) if k]


def area(ring):
    return abs(sum(x1 * y2 - x2 * y1 for (x1, y1), (x2, y2) in zip(ring, ring[1:] + ring[:1]))) / 2


def num(v):
    """Un decimal, sin ceros sobrantes: 3.0 → 3, -0.5 → -.5."""
    t = f"{v:.1f}".rstrip("0").rstrip(".")
    t = t.replace("0.", ".", 1) if t.startswith(("0.", "-0.")) else t
    return "0" if t in ("", "-", "-0") else t


def path(ring):
    """Anillo como trazo SVG con coordenadas relativas (números más cortos)."""
    pts = [(round(x, 1), round(y, 1)) for x, y in ring[:-1]]
    out = [f"M{num(pts[0][0])} {num(pts[0][1])}l"]
    deltas = []
    for (x0, y0), (x1, y1) in zip(pts, pts[1:]):
        if (x0, y0) == (x1, y1):
            continue
        dx, dy = num(x1 - x0), num(y1 - y0)
        deltas.append(f"{dx}{'' if dy.startswith('-') else ' '}{dy}")
    return out[0] + " ".join(deltas) + "z"


def rings_of(geom):
    polys = geom["coordinates"] if geom["type"] == "MultiPolygon" else [geom["coordinates"]]
    return [ring for poly in polys for ring in poly]


# Cajas [oeste, sur, este, norte] para acercar el mapa a cada continente.
CONTINENTS = {
    "Africa": (-20, -36, 53, 38), "Asia": (25, -12, 180, 78), "Europe": (-25, 34, 45, 72),
    "North America": (-170, 5, -50, 83), "South America": (-82, -57, -33, 13),
    "Oceania": (110, -50, 180, 20), "Antarctica": (-180, -90, 180, -60),
}


def line(points):
    pts = [(round(x, 1), round(y, 1)) for x, y in points]
    return "M" + "L".join(f"{num(x)} {num(y)}" for x, y in pts)


def frame():
    """Contorno de la Tierra y retícula cada 30°, ya proyectados."""
    lats = [lat / 2 for lat in range(-180, 181, 4)]
    sphere = line([to_screen(-180, lat) for lat in lats] + [to_screen(180, lat) for lat in reversed(lats)]) + "Z"
    meridians = [line([to_screen(lon, lat) for lat in lats]) for lon in range(-150, 180, 30)]
    parallels = [line([to_screen(lon, lat) for lon in range(-180, 181, 5)]) for lat in range(-60, 61, 30)]
    return sphere, "".join(meridians + parallels)


def box_of(w, s_, e, n):
    pts = [to_screen(lon, lat) for lon in (w, (w + e) / 2, e) for lat in (s_, (s_ + n) / 2, n)]
    return [round(min(p[0] for p in pts), 1), round(min(p[1] for p in pts), 1),
            round(max(p[0] for p in pts), 1), round(max(p[1] for p in pts), 1)]


def main(src):
    sys.stdout.reconfigure(encoding="utf-8")   # con -I no cuenta PYTHONIOENCODING y Windows usa cp1252
    data = json.loads(Path(src).read_text(encoding="utf-8"))
    by_iso = {}
    for f in data["features"]:
        p = f["properties"]
        name = p["NAME"]
        if name in SKIP:
            continue
        iso = MERGE.get(name, p["ISO_A2_EH"])
        if not iso or iso == "-99":
            print("sin código, se omite:", name)
            continue
        raw = [[to_screen(lon, lat) for lon, lat in r] for r in rings_of(f["geometry"])]
        rings = [s for s in (dp(r, TOL) for r in raw) if len(s) >= 4]
        if not rings:   # país diminuto (Vaticano, Mónaco): se queda su anillo mayor sin simplificar
            rings = [max(raw, key=area)]
        by_iso.setdefault(iso, []).extend(rings)

    countries = []
    for iso, rings in sorted(by_iso.items()):
        big = max(rings, key=area)
        rings = [r for r in rings if r is big or area(r) >= MIN_AREA]
        d = "".join(path(r) for r in rings)
        xs = [x for r in rings for x, _ in r]
        ys = [y for r in rings for _, y in r]
        bx = [x for x, _ in big]
        by = [y for _, y in big]
        # box: todos los anillos; mbox: el anillo mayor (encuadrar Francia sin la Guayana ni Reunión)
        countries.append({"iso": iso, "d": d,
                          "box": [round(min(xs), 1), round(min(ys), 1), round(max(xs), 1), round(max(ys), 1)],
                          "mbox": [round(min(bx), 1), round(min(by), 1), round(max(bx), 1), round(max(by), 1)]})

    out = Path(__file__).resolve().parents[1] / "src" / "geo" / "world.json"
    out.parent.mkdir(exist_ok=True)
    sphere, graticule = frame()
    world = {"w": W, "h": H, "sphere": sphere, "graticule": graticule,
             "continents": {k: box_of(*v) for k, v in CONTINENTS.items()}, "countries": countries}
    out.write_text(json.dumps(world, separators=(",", ":")), encoding="utf-8")
    print(f"{len(countries)} países, {out.stat().st_size / 1024:.0f} KB → {out}")


if __name__ == "__main__":
    main(sys.argv[1])
