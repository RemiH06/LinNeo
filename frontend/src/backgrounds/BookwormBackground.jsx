import { useEffect, useRef } from 'react'
import { useTheme } from '../theme/ThemeContext'
import { useKingdom } from '../theme/KingdomContext'
import { KINGDOM_HUE } from '../theme/kingdomColor'

/*
  Fondo de bookworm (vistas de media y nodos taxonomicos): papel/herbario.
  Portado 1:1 del demo bookworm_theme_demo.html, 4 capas:
    1. bw-tri    -- triangulos isometricos estaticos (textura tipo grano de madera)
    2. bw-light  -- komorebi, halos de luz estaticos (calida de dia / fria de noche)
    3. bw-wood   -- veta de madera con agujeros (stencil SVG via mask)
    4. bw-particles -- animado: lluvia (oscuro) u hojas cayendo (claro)
  Si hay un kingdom activo (useKingdom), los colores de las capas 1/2/4 se derivan
  del hue de ese reino en vez de los fijos del demo (igual patron que ElixirGraph).
*/

// Color base de cada capa cuando NO hay reino activo (tal cual el demo original).
const BASE = {
  triLight: '#8A7858', triDark: '#2A3824',
  lightWarmRGB: ['255,220,100', '255,210,80', '255,225,110', '240,200,80', '255,215,90', '255,230,120'],
  lightCoolRGB: ['180,210,255', '160,190,240', '180,210,255'],
  leafColors: ['#6A8050', '#8A6040', '#B08030', '#C89040', '#4A6040'],
  rainColor: '#8AB0C8',
}

function hueFor(kingdom) {
  const h = KINGDOM_HUE[kingdom]
  return (kingdom === 'incertae sedis' || h == null) ? null : h
}
// hsl -> "r,g,b" para reusar en rgba(...) de los gradientes radiales
function hslToRgbStr(h, s, l) {
  s /= 100; l /= 100
  const k = (n) => (n + h / 30) % 12
  const a = s * Math.min(l, 1 - l)
  const f = (n) => l - a * Math.max(-1, Math.min(k(n) - 3, Math.min(9 - k(n), 1)))
  const r = Math.round(f(0) * 255), g = Math.round(f(8) * 255), b = Math.round(f(4) * 255)
  return `${r},${g},${b}`
}

export default function BookwormBackground() {
  const triRef = useRef(null)
  const lightRef = useRef(null)
  const woodRef = useRef(null)
  const particlesRef = useRef(null)
  const { dark } = useTheme()
  const { kingdom } = useKingdom()
  const darkRef = useRef(dark); darkRef.current = dark
  const kingdomRef = useRef(kingdom); kingdomRef.current = kingdom

  // Capas estaticas: triangulos + komorebi + madera. Se reconstruyen en resize
  // y cuando cambia tema/reino (no animan solas).
  useEffect(() => {
    const triCanvas = triRef.current
    const lightCanvas = lightRef.current
    const woodEl = woodRef.current
    const tCtx = triCanvas.getContext('2d')
    const lCtx = lightCanvas.getContext('2d')

    function buildTri() {
      const W = triCanvas.width = window.innerWidth
      const H = triCanvas.height = window.innerHeight
      tCtx.clearRect(0, 0, W, H)
      const s = 48, h = 83.14, hHalf = 41.57
      const cols = Math.ceil(W / s) + 2
      const rows = Math.ceil(H / h) + 2
      const isDark = darkRef.current
      const hue = hueFor(kingdomRef.current)
      const col = hue != null
        ? `hsl(${hue}, ${isDark ? 30 : 25}%, ${isDark ? 22 : 42}%)`
        : (isDark ? BASE.triDark : BASE.triLight)
      const alpha = isDark ? 0.3 : 0.2

      tCtx.strokeStyle = col
      tCtx.lineWidth = 1.5
      tCtx.globalAlpha = alpha
      for (let r = 0; r < rows; r++) {
        const y = r * h - h
        ;[y, y + hHalf, y + h].forEach((ly) => {
          tCtx.beginPath(); tCtx.moveTo(-s, ly); tCtx.lineTo(W + s, ly); tCtx.stroke()
        })
        for (let c = 0; c < cols; c++) {
          const x = c * s - s
          tCtx.beginPath(); tCtx.moveTo(x, y); tCtx.lineTo(x + s / 2, y + hHalf); tCtx.lineTo(x + s, y); tCtx.stroke()
          tCtx.beginPath(); tCtx.moveTo(x, y + h); tCtx.lineTo(x + s / 2, y + hHalf); tCtx.lineTo(x + s, y + h); tCtx.stroke()
        }
      }
      tCtx.globalAlpha = 1
    }

    function buildLight() {
      const W = lightCanvas.width = window.innerWidth
      const H = lightCanvas.height = window.innerHeight
      lCtx.clearRect(0, 0, W, H)
      const isDark = darkRef.current
      const hue = hueFor(kingdomRef.current)
      // posiciones/radios/alphas del demo se mantienen; solo el color (rgb) se
      // deriva del reino si hay uno activo.
      const positions = isDark ? [
        { x: .30, y: .15, r: .40, a: 0.50 },
        { x: .75, y: .40, r: .32, a: 0.40 },
        { x: .50, y: .72, r: .36, a: 0.35 },
      ] : [
        { x: .12, y: .08, r: .42, a: 0.50 },
        { x: .68, y: .06, r: .32, a: 0.45 },
        { x: .38, y: .42, r: .48, a: 0.48 },
        { x: .82, y: .62, r: .30, a: 0.40 },
        { x: .22, y: .78, r: .35, a: 0.42 },
        { x: .55, y: .25, r: .25, a: 0.38 },
      ]
      const palette = hue != null
        ? positions.map(() => hslToRgbStr(hue, isDark ? 55 : 70, isDark ? 70 : 75))
        : (isDark ? BASE.lightCoolRGB : BASE.lightWarmRGB)

      positions.forEach(({ x, y, r, a }, i) => {
        const cx = x * W, cy = y * H, rad = r * Math.min(W, H)
        const rgb = palette[i % palette.length]
        const g = lCtx.createRadialGradient(cx, cy, 0, cx, cy, rad)
        g.addColorStop(0, `rgba(${rgb},${a})`)
        g.addColorStop(0.5, `rgba(${rgb},${(a * 0.3).toFixed(3)})`)
        g.addColorStop(1, `rgba(${rgb},0)`)
        lCtx.fillStyle = g
        lCtx.fillRect(0, 0, W, H)
      })
    }

    function buildWood() {
      const W = window.innerWidth
      const H = window.innerHeight
      const isDark = darkRef.current
      const woodColor = isDark ? 'rgba(9,15,6,1.0)' : 'rgba(215,205,185,1.0)'
      const nh = 20, r = 100
      const holes = Array.from({ length: nh }, () => ({
        cx: r + Math.random() * (W - r * 2),
        cy: r + Math.random() * (H - r * 2),
        r,
      }))
      const ellipses = holes.map(({ cx, cy, r }) =>
        `<ellipse cx="${cx.toFixed(1)}" cy="${cy.toFixed(1)}" rx="${r}" ry="${r}" fill="black"/>`
      ).join('')
      const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}">
  <defs><mask id="m"><rect width="${W}" height="${H}" fill="white"/>${ellipses}</mask></defs>
  <rect width="${W}" height="${H}" fill="${woodColor}" mask="url(#m)"/>
</svg>`
      woodEl.style.backgroundImage = `url("data:image/svg+xml,${encodeURIComponent(svg)}")`
      woodEl.style.backgroundSize = `${W}px ${H}px`
      woodEl.style.backgroundRepeat = 'no-repeat'
    }

    const rebuildAll = () => { buildTri(); buildLight(); buildWood() }
    rebuildAll()
    window.addEventListener('resize', rebuildAll)
    return () => window.removeEventListener('resize', rebuildAll)
  }, [dark, kingdom])

  // Capa animada: particulas (lluvia en oscuro, hojas en claro).
  useEffect(() => {
    const reduce = window.matchMedia('(prefers-reduced-motion: reduce)').matches
    if (reduce) return
    const canvas = particlesRef.current
    const ctx = canvas.getContext('2d')
    let W, H, parts = [], raf

    const hue = hueFor(kingdomRef.current)
    const leafColors = hue != null
      ? [0, 1, 2, 3, 4].map((i) => `hsl(${hue}, ${65 - i * 4}%, ${30 + i * 6}%)`)
      : BASE.leafColors
    const rainColor = hue != null ? `hsl(${hue}, 40%, 70%)` : BASE.rainColor

    const resize = () => { W = canvas.width = window.innerWidth; H = canvas.height = window.innerHeight }
    const makeLeaf = () => ({
      type: 'leaf', x: Math.random() * W, y: -20,
      vx: (Math.random() - .5) * .8, vy: .4 + Math.random() * .6,
      angle: Math.random() * Math.PI * 2, vAngle: (Math.random() - .5) * .04,
      size: 5 + Math.random() * 8, sway: Math.random() * Math.PI * 2,
      swaySpeed: .01 + Math.random() * .02,
      color: leafColors[Math.floor(Math.random() * leafColors.length)],
      alpha: .4 + Math.random() * .4,
    })
    const makeRain = () => ({
      type: 'rain', x: Math.random() * W, y: -10,
      vy: 7 + Math.random() * 5, vx: -.8 + Math.random() * .4,
      len: 10 + Math.random() * 14, alpha: .06 + Math.random() * .12,
    })

    function loop() {
      const isDark = darkRef.current
      if (isDark) { if (Math.random() < .55) parts.push(makeRain()) }
      else { if (Math.random() < .04) parts.push(makeLeaf()) }

      ctx.clearRect(0, 0, W, H)
      parts = parts.filter((p) => {
        if (p.type === 'leaf') {
          p.sway += p.swaySpeed; p.x += p.vx + Math.sin(p.sway) * .5
          p.y += p.vy; p.angle += p.vAngle
          ctx.save(); ctx.translate(p.x, p.y); ctx.rotate(p.angle)
          ctx.globalAlpha = p.alpha; ctx.fillStyle = p.color
          ctx.beginPath(); ctx.ellipse(0, 0, p.size, p.size * .5, 0, 0, Math.PI * 2); ctx.fill()
          ctx.restore()
          return p.y < H + 20
        } else {
          p.x += p.vx; p.y += p.vy
          ctx.save(); ctx.strokeStyle = rainColor; ctx.globalAlpha = p.alpha
          ctx.lineWidth = .8; ctx.beginPath()
          ctx.moveTo(p.x, p.y); ctx.lineTo(p.x + p.vx * 2, p.y + p.len); ctx.stroke()
          ctx.restore()
          return p.y < H + 20
        }
      })
      ctx.globalAlpha = 1
      raf = requestAnimationFrame(loop)
    }

    resize()
    window.addEventListener('resize', resize)
    raf = requestAnimationFrame(loop)
    return () => { cancelAnimationFrame(raf); window.removeEventListener('resize', resize) }
  }, [dark, kingdom])

  return (
    <>
      <canvas ref={triRef} style={{ position: 'fixed', inset: 0, zIndex: 0, pointerEvents: 'none' }} />
      <canvas ref={lightRef} style={{ position: 'fixed', inset: 0, zIndex: 1, pointerEvents: 'none' }} />
      <div ref={woodRef} style={{ position: 'fixed', inset: 0, zIndex: 2, pointerEvents: 'none' }} />
      <canvas ref={particlesRef} style={{ position: 'fixed', inset: 0, zIndex: 3, pointerEvents: 'none' }} />
    </>
  )
}