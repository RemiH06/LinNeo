"""Auditoría de accesibilidad del front contra la API real (lo que haría Lighthouse/axe, sin bajar nada).

    python frontend/scripts/auditoria.py [rutas,separadas,por,coma]

Por ruta, en modo día y noche, a 1440 y 390 px: contraste de cada texto visible contra su
fondo efectivo (WCAG 1.4.3), nombre accesible de botones y enlaces, alt de imágenes, orden
de encabezados, letra < 12 px y zonas táctiles < 24 × 24 px (WCAG 2.5.8; un enlace dentro de
un renglón de texto está exento). Los colores se resuelven pintándolos en un canvas, así
entran oklab, color-mix y variables. Necesita Vite en 5135 y la API en 8035.
"""
import sys
from collections import defaultdict
from playwright.sync_api import sync_playwright

sys.stdout.reconfigure(encoding="utf-8")
BASE = "http://localhost:5135"
RUTAS = {
    "inicio": "/", "especie": "/species/5219426", "familia": "/taxon/family/9703", "reino": "/taxon/kingdom/1",
    "galeria": "/taxon/family/9703/gallery", "sonidos": "/taxon/family/5290/sounds", "infografia": "/taxon/family/5290/infographic",
    "busqueda": "/search?q=jaguar", "mapa": "/map", "continente": "/map/Europe", "pais": "/map/country/MX",
}
SOLO = sys.argv[1].split(",") if len(sys.argv) > 1 else None

AUDIT = r"""() => {
  const cv = document.createElement('canvas'); cv.width = cv.height = 1
  const cx = cv.getContext('2d', { willReadFrequently: true })
  const rgba = (css) => {   // cualquier color CSS → [r, g, b, a] en sRGB
    cx.clearRect(0, 0, 1, 1); cx.fillStyle = '#000'; cx.fillStyle = css; cx.fillRect(0, 0, 1, 1)
    const [r, g, b, a] = cx.getImageData(0, 0, 1, 1).data; return [r, g, b, a / 255]
  }
  const lum = ([r, g, b]) => { const f = (c) => { c /= 255; return c <= .03928 ? c / 12.92 : ((c + .055) / 1.055) ** 2.4 }; return .2126 * f(r) + .7152 * f(g) + .0722 * f(b) }
  const ratio = (a, b) => { const [x, y] = [lum(a), lum(b)].sort((p, q) => q - p); return (x + .05) / (y + .05) }
  const over = (top, under) => top.slice(0, 3).map((c, i) => c * top[3] + under[i] * (1 - top[3])).concat(1)
  const visible = (el) => { const r = el.getBoundingClientRect(); const s = getComputedStyle(el)
    return r.width > 0 && r.height > 0 && s.visibility !== 'hidden' && s.display !== 'none' && +s.opacity > 0.05 && !el.closest('[aria-hidden="true"],.sr-only') }
  const bgOf = (el) => {   // fondo efectivo: se compone de arriba abajo hasta algo opaco
    const layers = []; let n = el
    while (n && n.nodeType === 1) {
      const s = getComputedStyle(n)
      if (s.backgroundImage !== 'none' && n !== document.body && n !== document.documentElement && /url\(/.test(s.backgroundImage)) return null  // texto sobre imagen
      const c = rgba(s.backgroundColor); if (c[3] > 0) { layers.push(c); if (c[3] >= 1) break }
      n = n.parentElement
    }
    let acc = rgba(getComputedStyle(document.body).backgroundColor); if (acc[3] < 1) acc = [255, 255, 255, 1]
    for (const l of layers.reverse()) acc = over(l, acc)
    return acc
  }
  const out = { contraste: [], nombre: [], alt: [], encabezados: [], letra: [], tactil: [] }
  const seen = new Set()
  for (const el of document.querySelectorAll('body *')) {
    if (!visible(el) || ['SCRIPT', 'STYLE', 'SVG', 'PATH', 'G'].includes(el.tagName.toUpperCase())) continue
    const own = [...el.childNodes].some((n) => n.nodeType === 3 && n.textContent.trim())
    const s = getComputedStyle(el)
    if (own && !el.closest('svg')) {
      const size = parseFloat(s.fontSize), bold = +s.fontWeight >= 700
      const bg = bgOf(el)
      if (bg) {
        const fg = over(rgba(s.color), bg)
        const need = size >= 24 || (bold && size >= 18.66) ? 3 : 4.5
        const r = ratio(fg, bg)
        const key = el.className + '|' + s.color
        if (r < need && !seen.has(key)) { seen.add(key); out.contraste.push(`${r.toFixed(2)} < ${need}  <${el.tagName.toLowerCase()} class="${el.className}">${el.textContent.trim().slice(0, 30)}`) }
      }
      if (size < 12 && !el.closest('.world-map')) out.letra.push(`${size}px <${el.tagName.toLowerCase()} class="${el.className}">${el.textContent.trim().slice(0, 30)}`)
    }
    const interactive = el.matches('a[href], button, input, select, textarea, [role="button"], [tabindex="0"]')
    if (interactive) {
      const name = (el.getAttribute('aria-label') || el.textContent || el.getAttribute('title') || el.getAttribute('aria-labelledby') ||
        (el.id && document.querySelector(`label[for="${el.id}"]`)?.textContent) || (el.closest('label')?.textContent) || el.getAttribute('placeholder') || '').trim()
      if (!name && el.type !== 'hidden') out.nombre.push(`<${el.tagName.toLowerCase()} class="${el.className}">`)
      // un checkbox o radio dentro de su <label> se activa tocando toda la etiqueta
      const r = (el.matches('input[type=checkbox], input[type=radio]') && el.closest('label') ? el.closest('label') : el).getBoundingClientRect()
      const inlineText = s.display === 'inline' && el.closest('p, li, dd, figcaption, span') && el.tagName === 'A'
      if (!inlineText && (r.width < 24 || r.height < 24) && !el.closest('.world-map') && el.type !== 'range')
        out.tactil.push(`${Math.round(r.width)}×${Math.round(r.height)} <${el.tagName.toLowerCase()} class="${el.className}">${(el.textContent || el.getAttribute('aria-label') || '').trim().slice(0, 24)}`)
    }
    if (el.tagName === 'IMG' && !el.hasAttribute('alt')) out.alt.push(el.src.slice(-40))
  }
  const hs = [...document.querySelectorAll('h1,h2,h3,h4,h5,h6')].filter(visible).map((h) => +h.tagName[1])
  if (hs.filter((h) => h === 1).length !== 1) out.encabezados.push(`${hs.filter((h) => h === 1).length} h1`)
  hs.forEach((h, i) => { if (i && h > hs[i - 1] + 1) out.encabezados.push(`salta de h${hs[i - 1]} a h${h}`) })
  if (!document.documentElement.lang) out.encabezados.push('sin lang en <html>')
  return out
}"""

total = defaultdict(int)
with sync_playwright() as p:
    b = p.chromium.launch()
    for nombre, ruta in RUTAS.items():
        if SOLO and nombre not in SOLO:
            continue
        for modo in ("light", "dark"):
            for w, h in ((1440, 900), (390, 844)):
                pg = b.new_page(viewport={"width": w, "height": h})
                pg.add_init_script(f"localStorage.setItem('linneo-theme', '{modo}')")
                pg.goto(BASE + ruta, wait_until="load", timeout=60000)
                pg.wait_for_timeout(2500)
                res = pg.evaluate(AUDIT)
                fallas = {k: v for k, v in res.items() if v}
                print(f"## {nombre} {modo} {w}: " + (", ".join(f"{k} {len(v)}" for k, v in fallas.items()) or "sin hallazgos"))
                for k, v in fallas.items():
                    total[k] += len(v)
                    for x in list(dict.fromkeys(v))[:20]:
                        print(f"   {k}: {x}")
                pg.close()
    b.close()
print("TOTAL:", dict(total) or "sin hallazgos")
