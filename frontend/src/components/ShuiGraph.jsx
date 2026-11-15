import { useEffect, useRef } from 'react'
import { forceSimulation, forceManyBody, forceCollide } from 'd3-force'
import { useTheme } from '../theme/ThemeContext'
import { kingdomInk, domainColors } from '../theme/kingdomColor'

/*
  Grafo de inicio, como lámina impresa: el centro y su primer nivel son palabras
  en tinta con un punto del pigmento de su reino; el segundo nivel son puntos y
  muestra su nombre al acercar o al pasar encima. Aristas en filete.
  - data: { nodes:[{id,name,rank,key,kingdom}], links:[{source,target}], center, kingdom }
  - onFocus(node | null): recentrar (null = volver a Biota). onOpenSpecies(key).
  Puntero unificado (mouse y toque). La rueda acerca solo con Ctrl, para no
  secuestrar el scroll de la página; los botones de abajo hacen lo mismo.
*/
const LEVEL1_LABELS = 60   // con más hijos que esto, el primer nivel también se vuelve puntos
const ZOOM_LABELS = 1.6    // a partir de este zoom se rotula todo

function cssVar(name) {
  return getComputedStyle(document.body).getPropertyValue(name).trim()
}

export default function ShuiGraph({ data, onFocus, onOpenSpecies }) {
  const canvasRef = useRef(null)
  const wrapRef = useRef(null)
  const apiRef = useRef({})
  const { dark } = useTheme()
  const onFocusRef = useRef(onFocus); onFocusRef.current = onFocus
  const onOpenRef = useRef(onOpenSpecies); onOpenRef.current = onOpenSpecies

  useEffect(() => {
    if (!data?.nodes?.length) return
    const canvas = canvasRef.current
    const wrap = wrapRef.current
    const ctx = canvas.getContext('2d')
    const reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches

    let W = 0, H = 0
    const dpr = window.devicePixelRatio || 1
    const setSize = () => {
      W = wrap.clientWidth; H = wrap.clientHeight
      canvas.width = W * dpr; canvas.height = H * dpr
      canvas.style.width = `${W}px`; canvas.style.height = `${H}px`
    }
    setSize()
    const view = { x: 0, y: 0, k: 1 }

    const nodes = data.nodes.map((n) => ({ ...n }))
    const links = data.links.map((l) => ({ ...l }))
    const byId = Object.fromEntries(nodes.map((n) => [n.id, n]))
    const center = byId[data.center]
    const childrenOf = {}
    for (const l of links) (childrenOf[l.source] ||= []).push(l.target)
    const lvl1 = childrenOf[data.center] || []
    for (const id of lvl1) if (byId[id]) byId[id].level = 1
    if (center) center.level = 0
    const labelLvl1 = lvl1.length <= LEVEL1_LABELS
    const labelAll = nodes.length <= LEVEL1_LABELS
    for (const n of nodes) n.kids = childrenOf[n.id]?.length || 0
    // Orden de rotulado: centro, primer nivel (los de más hijos primero), segundo nivel.
    const byPriority = [...nodes].sort((a, b) => (a.level ?? 2) - (b.level ?? 2) || b.kids - a.kids)

    // Layout radial: cada hijo ocupa un arco proporcional a sus propios hijos.
    const R1 = Math.min(W, H) * 0.27
    const R2 = Math.min(W, H) * 0.45
    const weights = lvl1.map((id) => 1 + (childrenOf[id]?.length || 0))
    const totalW = weights.reduce((a, b) => a + b, 0) || 1
    nodes.forEach((n) => { n.x = W / 2; n.y = H / 2; n.level ??= 2 })
    if (center) { center.fx = W / 2; center.fy = H / 2 }
    let angle = -Math.PI / 2
    lvl1.forEach((id, i) => {
      const span = (weights[i] / totalW) * Math.PI * 2
      const node = byId[id]
      if (node) { node.x = W / 2 + Math.cos(angle + span / 2) * R1; node.y = H / 2 + Math.sin(angle + span / 2) * R1 }
      const kids = childrenOf[id] || []
      const pad = span * 0.12
      kids.forEach((cid, j) => {
        const a = angle + pad + (kids.length === 1 ? 0.5 : j / (kids.length - 1)) * (span - 2 * pad)
        const c = byId[cid]
        if (c) { c.x = W / 2 + Math.cos(a) * R2; c.y = H / 2 + Math.sin(a) * R2 }
      })
      angle += span
    })

    const dotR = (n) => (n.level === 0 ? 6 : n.level === 1 ? 4 : 2.5)
    const inkOf = (n) => (n.rank === 'biota' ? null : kingdomInk(n.rank === 'domain' || n.rank === 'kingdom' ? (n.kingdom || n.name) : (data.kingdom || n.kingdom), dark))
    const italic = (n) => n.rank === 'genus' || n.rank === 'species'
    let hover = null

    const sim = forceSimulation(nodes)
      .force('charge', forceManyBody().strength(-30))
      .force('collide', forceCollide().radius((d) => dotR(d) + (d.level < 2 && labelLvl1 ? 10 : 2)))
      .alpha(reduceMotion ? 0 : 0.4).alphaDecay(0.06)

    function draw() {
      const ink = cssVar('--ink'), ink2 = cssVar('--ink2'), rule = cssVar('--rule'), accent = cssVar('--accent'), paper = cssVar('--paper')
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
      ctx.clearRect(0, 0, W, H)
      ctx.translate(view.x, view.y); ctx.scale(view.k, view.k)

      ctx.lineWidth = 1 / view.k
      ctx.strokeStyle = rule
      ctx.globalAlpha = links.length > 150 ? 0.45 : 1   // con cientos de aristas, filete más tenue
      ctx.beginPath()
      for (const l of links) {
        const s = typeof l.source === 'object' ? l.source : byId[l.source]
        const t = typeof l.target === 'object' ? l.target : byId[l.target]
        if (s && t) { ctx.moveTo(s.x, s.y); ctx.lineTo(t.x, t.y) }
      }
      ctx.stroke()
      ctx.globalAlpha = 1

      for (const n of nodes) {
        const r = dotR(n) / Math.sqrt(view.k)
        const colors = n.rank === 'domain' ? domainColors(n.name, dark) : []
        if (colors.length > 1) {        // dominio con varios reinos: un punto por pigmento
          colors.forEach((c, i) => {
            const a = (i / colors.length) * Math.PI * 2 - Math.PI / 2
            ctx.beginPath(); ctx.arc(n.x + Math.cos(a) * r * 0.7, n.y + Math.sin(a) * r * 0.7, r * 0.6, 0, Math.PI * 2)
            ctx.fillStyle = c; ctx.fill()
          })
        } else {
          ctx.beginPath(); ctx.arc(n.x, n.y, r, 0, Math.PI * 2)
          ctx.fillStyle = inkOf(n) || accent; ctx.fill()
        }
        n.box = null
      }

      // Rótulos por prioridad; uno que chocaría con otro ya puesto se omite
      // (el centro y el que está bajo el cursor siempre van).
      const placed = []
      const overlaps = (b) => placed.some((p) => b.x0 < p.x1 && b.x1 > p.x0 && b.y0 < p.y1 && b.y1 > p.y0)
      const order = hover ? [hover, ...byPriority.filter((n) => n !== hover)] : byPriority
      ctx.lineJoin = 'round'
      for (const n of order) {
        const labeled = labelAll || n.level === 0 || (n.level === 1 && labelLvl1) || view.k >= ZOOM_LABELS || n === hover
        if (!labeled) continue
        const r = dotR(n) / Math.sqrt(view.k)
        const size = (n.level === 0 ? 20 : 13) / view.k
        const family = n.level === 0 && !italic(n) ? "'Libre Caslon Display', serif" : "'Libre Caslon Text', Georgia, serif"
        ctx.font = `${italic(n) ? 'italic ' : ''}400 ${size}px ${family}`
        const x = n.x + r + 4 / view.k, y = n.y + size * 0.32
        const box = { x0: x, x1: x + ctx.measureText(n.name).width, y0: y - size * 0.8, y1: y + size * 0.25 }
        if (n !== hover && n.level !== 0 && overlaps(box)) continue
        placed.push(box)
        // halo de papel para que la palabra se lea sobre las aristas
        ctx.lineWidth = 3 / view.k; ctx.strokeStyle = paper
        ctx.strokeText(n.name, x, y)
        ctx.fillStyle = n.level === 0 || n === hover ? accent : n.level === 1 ? ink : ink2
        ctx.fillText(n.name, x, y)
        n.box = box
      }
      ctx.lineWidth = 1 / view.k
    }
    sim.on('tick', draw)
    if (reduceMotion) { sim.tick(120); draw() }

    const toWorld = (sx, sy) => ({ x: (sx - view.x) / view.k, y: (sy - view.y) / view.k })
    const screenPos = (e) => { const b = canvas.getBoundingClientRect(); return { x: e.clientX - b.left, y: e.clientY - b.top } }
    const nodeAt = (wx, wy) => {
      for (let i = nodes.length - 1; i >= 0; i--) {
        const n = nodes[i]; const r = dotR(n) + 6 / view.k
        if ((n.x - wx) ** 2 + (n.y - wy) ** 2 <= r * r) return n
        const b = n.box
        if (b && wx >= b.x0 && wx <= b.x1 && wy >= b.y0 && wy <= b.y1) return n
      }
      return null
    }
    const zoomAt = (sx, sy, f) => {
      const w = toWorld(sx, sy); const k = Math.max(0.4, Math.min(6, view.k * f))
      view.x = sx - w.x * k; view.y = sy - w.y * k; view.k = k; draw()
    }
    apiRef.current = {
      zoomIn: () => zoomAt(W / 2, H / 2, 1.4),
      zoomOut: () => zoomAt(W / 2, H / 2, 1 / 1.4),
      reset: () => { view.x = 0; view.y = 0; view.k = 1; draw() },
    }

    let press = null   // { node, start, moved, panFrom }
    const activate = (n) => {
      if (n.rank === 'species' && n.key) onOpenRef.current?.(n.key)
      else if (n.rank === 'biota') onFocusRef.current?.(null)
      else if (n.key != null && n.id !== data.center) onFocusRef.current?.(n)
    }
    const onDown = (e) => {
      const sp = screenPos(e); const w = toWorld(sp.x, sp.y)
      canvas.setPointerCapture(e.pointerId)
      press = { node: nodeAt(w.x, w.y), start: sp, moved: false, panFrom: { x: sp.x - view.x, y: sp.y - view.y } }
      if (press.node) { press.node.fx = press.node.x; press.node.fy = press.node.y; sim.alphaTarget(0.15).restart() }
    }
    const onMove = (e) => {
      const sp = screenPos(e)
      if (!press) {
        const w = toWorld(sp.x, sp.y); const n = nodeAt(w.x, w.y)
        canvas.style.cursor = n ? 'pointer' : 'grab'
        if (n !== hover) { hover = n; draw() }
        return
      }
      if (Math.hypot(sp.x - press.start.x, sp.y - press.start.y) > 4) press.moved = true
      if (!press.moved) return
      if (press.node) { const w = toWorld(sp.x, sp.y); press.node.fx = w.x; press.node.fy = w.y }
      else { view.x = sp.x - press.panFrom.x; view.y = sp.y - press.panFrom.y; draw() }
    }
    const onUp = () => {
      if (!press) return
      const n = press.node
      if (n) {
        if (n.id !== data.center) { n.fx = null; n.fy = null }
        sim.alphaTarget(0)
        if (!press.moved) activate(n)
      }
      press = null
    }
    const onWheel = (e) => {
      if (!e.ctrlKey && !e.metaKey) return
      e.preventDefault(); const sp = screenPos(e); zoomAt(sp.x, sp.y, e.deltaY < 0 ? 1.12 : 1 / 1.12)
    }
    const onLeave = () => { if (hover) { hover = null; draw() } }
    canvas.addEventListener('pointerdown', onDown)
    canvas.addEventListener('pointermove', onMove)
    canvas.addEventListener('pointerup', onUp)
    canvas.addEventListener('pointercancel', onUp)
    canvas.addEventListener('pointerleave', onLeave)
    canvas.addEventListener('wheel', onWheel, { passive: false })
    const ro = new ResizeObserver(() => { setSize(); draw() })
    ro.observe(wrap)

    return () => {
      sim.stop(); ro.disconnect()
      canvas.removeEventListener('pointerdown', onDown)
      canvas.removeEventListener('pointermove', onMove)
      canvas.removeEventListener('pointerup', onUp)
      canvas.removeEventListener('pointercancel', onUp)
      canvas.removeEventListener('pointerleave', onLeave)
      canvas.removeEventListener('wheel', onWheel)
    }
  }, [data, dark])

  const centerName = data?.nodes?.find((n) => n.id === data.center)?.name
  return (
    <div className="graph">
      <div ref={wrapRef} className="graph-canvas">
        <canvas ref={canvasRef} role="img"
          aria-label={`Grafo centrado en ${centerName || 'Biota'}; la lista de abajo tiene los mismos enlaces.`} />
      </div>
      <div className="graph-zoom" role="group" aria-label="Zoom del grafo">
        <button type="button" className="ctl" onClick={() => apiRef.current.zoomIn?.()}>Acercar</button>
        <button type="button" className="ctl" onClick={() => apiRef.current.zoomOut?.()}>Alejar</button>
        <button type="button" className="ctl" onClick={() => apiRef.current.reset?.()}>Encuadrar</button>
      </div>
    </div>
  )
}
