import { useEffect, useRef } from 'react'
import { forceSimulation, forceManyBody, forceCollide } from 'd3-force'
import { useTheme } from '../theme/ThemeContext'
import { nodeColor, domainColors } from '../theme/kingdomColor'

/*
  Grafo principal de shui.
  - data: { nodes:[{id,name,rank,key,kingdom}], links:[{source,target}], center }
  - onFocus(node): clic en un nodo -> recentrar (el padre recarga data).
  Colorea cada nodo por su reino (hue) + rango (luminosidad). Biota neutro.
*/
export default function ShuiGraph({ data, onFocus, onOpenSpecies }) {
  const canvasRef = useRef(null)
  const wrapRef = useRef(null)
  const { dark } = useTheme()
  const onFocusRef = useRef(onFocus); onFocusRef.current = onFocus
  const onOpenRef = useRef(onOpenSpecies); onOpenRef.current = onOpenSpecies

  useEffect(() => {
    if (!data || !data.nodes?.length) return
    const canvas = canvasRef.current
    const wrap = wrapRef.current
    const ctx = canvas.getContext('2d')
    const edgeColor = dark ? '#1a2a3a' : '#1E5068'
    const textColor = dark ? '#C0E8F0' : '#E8F8F8'

    let W = wrap.clientWidth
    let H = wrap.clientHeight || 460
    const dpr = window.devicePixelRatio || 1
    const setSize = () => {
      W = wrap.clientWidth; H = wrap.clientHeight || 460
      canvas.width = W * dpr; canvas.height = H * dpr
      canvas.style.width = W + 'px'; canvas.style.height = H + 'px'
    }
    setSize()
    const view = { x: 0, y: 0, k: 1 }

    // clonar nodos/links (d3 muta)
    const nodes = data.nodes.map((n) => ({ ...n }))
    const links = data.links.map((l) => ({ ...l }))
    const byId = Object.fromEntries(nodes.map((n) => [n.id, n]))

    // ── Layout radial proporcional ──
    // centro al medio; nivel 1 en un anillo, cada nodo ocupa un arco proporcional
    // a su numero de hijos; nivel 2 dentro del arco de su padre.
    const center = byId[data.center]
    const childrenOf = {}
    for (const l of links) {
      const s = typeof l.source === 'object' ? l.source.id : l.source
      const t = typeof l.target === 'object' ? l.target.id : l.target
      ;(childrenOf[s] ||= []).push(t)
    }
    const R1 = Math.min(W, H) * 0.26   // radio anillo nivel 1
    const R2 = Math.min(W, H) * 0.44   // radio anillo nivel 2

    const lvl1 = (childrenOf[data.center] || [])
    // peso de cada nodo nivel1 = 1 + numero de hijos (para que ocupe arco proporcional)
    const weights = lvl1.map((id) => 1 + (childrenOf[id]?.length || 0))
    const totalW = weights.reduce((a, b) => a + b, 0) || 1

    // posicionar centro
    nodes.forEach((n) => { n.x = W / 2; n.y = H / 2 })
    if (center) { center.x = W / 2; center.y = H / 2; center.fx = W / 2; center.fy = H / 2 }

    let angle = -Math.PI / 2 // empezar arriba
    lvl1.forEach((id, i) => {
      const span = (weights[i] / totalW) * Math.PI * 2
      const mid = angle + span / 2
      const node = byId[id]
      if (node) {
        node.x = W / 2 + Math.cos(mid) * R1
        node.y = H / 2 + Math.sin(mid) * R1
        node._ang = mid
      }
      // hijos (nivel 2) dentro del arco [angle, angle+span]
      const kids = childrenOf[id] || []
      const pad = span * 0.12
      kids.forEach((cid, j) => {
        const t = kids.length === 1 ? 0.5 : j / (kids.length - 1)
        const a = angle + pad + t * (span - 2 * pad)
        const c = byId[cid]
        if (c) { c.x = W / 2 + Math.cos(a) * R2; c.y = H / 2 + Math.sin(a) * R2 }
      })
      angle += span
    })

    const rankRadius = { root: 13, domain: 12, kingdom: 11, phylum: 8, class: 7, order: 6, family: 5, genus: 4, species: 4 }
    const radiusOf = (n) => rankRadius[n.rank] || 5
    // Reino que tine el subgrafo en foco. Viene del backend (data.kingdom) cuando
    // estas explorando dentro de un kingdom; null en la vista general (Biota).
    // IMPORTANTE: domain y kingdom NUNCA se retinen por esto -- mantienen su propio
    // color fijo siempre. Solo phylum/class/order/family/genus/species heredan el
    // color del kingdom enfocado, sin importar cuanto se navegue dentro de el.
    const focusedKingdom = data.kingdom || null
    const colorOf = (n) => {
      if (n.rank === 'root') return dark ? '#A0E8F8' : '#1878A0' // Biota neutro
      if (n.rank === 'domain' || n.rank === 'kingdom') return nodeColor(n.kingdom || n.name, 'kingdom', dark)
      return nodeColor(focusedKingdom || n.kingdom, n.rank, dark)
    }
    // Nodos Domain con mas de un kingdom hijo (Eukaryota, Prokaryota) se pintan como
    // 'canicas': varios circulos pequenos superpuestos, uno por color de kingdom hijo.
    // Viruses / incertae sedis tienen un solo kingdom hijo -> color unico, sin canicas.
    const drawNode = (n, r) => {
      const isMultiDomain = n.rank === 'domain' && (domainColors(n.name, dark).length > 1)
      if (isMultiDomain) {
        const colors = domainColors(n.name, dark)
        const k = colors.length
        const marbleR = r * 0.62
        const orbit = r * 0.42
        if (n.id === data.center) { ctx.shadowColor = colors[0]; ctx.shadowBlur = 16 }
        colors.forEach((col, i) => {
          const a = (i / k) * Math.PI * 2 - Math.PI / 2
          const mx = n.x + Math.cos(a) * orbit
          const my = n.y + Math.sin(a) * orbit
          ctx.beginPath(); ctx.arc(mx, my, marbleR, 0, Math.PI * 2)
          ctx.fillStyle = col; ctx.fill()
        })
        ctx.shadowBlur = 0
        return
      }
      ctx.beginPath(); ctx.arc(n.x, n.y, r, 0, Math.PI * 2)
      ctx.fillStyle = colorOf(n)
      if (n.id === data.center) { ctx.shadowColor = colorOf(n); ctx.shadowBlur = 16 }
      ctx.fill(); ctx.shadowBlur = 0
    }

    // simulacion muy suave: solo evita solapes y mantiene el radio, sin link force que desordene
    const sim = forceSimulation(nodes)
      .force('charge', forceManyBody().strength(-40))
      .force('collide', forceCollide().radius((d) => radiusOf(d) + 3))
      .alpha(0.4).alphaDecay(0.06)

    function draw() {
      ctx.save()
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
      ctx.clearRect(0, 0, W, H)
      ctx.translate(view.x, view.y); ctx.scale(view.k, view.k)

      // Luz trasera del reino enfocado, detras de todo (estilo SpeciesDetail).
      const centerNode = byId[data.center]
      if (focusedKingdom && centerNode) {
        const glowR = Math.min(W, H) * 0.5
        const grad = ctx.createRadialGradient(centerNode.x, centerNode.y, 0, centerNode.x, centerNode.y, glowR)
        grad.addColorStop(0, nodeColor(focusedKingdom, 'kingdom', dark))
        grad.addColorStop(1, 'transparent')
        ctx.save()
        ctx.globalAlpha = dark ? 0.30 : 0.20
        ctx.fillStyle = grad
        ctx.fillRect(centerNode.x - glowR, centerNode.y - glowR, glowR * 2, glowR * 2)
        ctx.restore()
      }

      ctx.lineWidth = 1 / view.k
      for (const l of links) {
        const s = typeof l.source === 'object' ? l.source : byId[l.source]
        const t = typeof l.target === 'object' ? l.target : byId[l.target]
        if (!s || !t) continue
        ctx.beginPath(); ctx.moveTo(s.x, s.y); ctx.lineTo(t.x, t.y)
        ctx.strokeStyle = edgeColor; ctx.globalAlpha = 0.5; ctx.stroke()
      }
      ctx.globalAlpha = 1
      for (const n of nodes) {
        const r = radiusOf(n)
        drawNode(n, r)
        // etiquetas: centro, reinos/dominios y filos; el resto en zoom alto
        const showLabel = n.rank === 'root' || n.rank === 'domain' || n.rank === 'kingdom' || n.id === data.center || view.k > 1.4 || n.rank === 'phylum'
        if (showLabel) {
          ctx.font = `${n.id === data.center ? 'bold ' : ''}${11 / view.k}px Recursive, monospace`
          ctx.fillStyle = textColor; ctx.textAlign = 'left'
          ctx.fillText(n.name, n.x + r + 3, n.y + 3)
        }
      }
      ctx.restore()
    }
    sim.on('tick', draw)

    const toWorld = (sx, sy) => ({ x: (sx - view.x) / view.k, y: (sy - view.y) / view.k })
    const screenPos = (e) => { const b = canvas.getBoundingClientRect(); return { x: e.clientX - b.left, y: e.clientY - b.top } }
    const nodeAt = (wx, wy) => {
      for (let i = nodes.length - 1; i >= 0; i--) {
        const n = nodes[i]; const r = radiusOf(n) + 5
        if ((n.x - wx) ** 2 + (n.y - wy) ** 2 <= r * r) return n
      }
      return null
    }
    let dragging = null, panning = false, panStart = null, moved = false
    const onDown = (e) => {
      const sp = screenPos(e); const w = toWorld(sp.x, sp.y); const n = nodeAt(w.x, w.y); moved = false
      if (n) { dragging = n; sim.alphaTarget(0.2).restart(); n.fx = n.x; n.fy = n.y }
      else { panning = true; panStart = { x: sp.x - view.x, y: sp.y - view.y } }
    }
    const onMove = (e) => {
      const sp = screenPos(e)
      if (dragging) { const w = toWorld(sp.x, sp.y); dragging.fx = w.x; dragging.fy = w.y; moved = true }
      else if (panning) { view.x = sp.x - panStart.x; view.y = sp.y - panStart.y; moved = true; draw() }
      else { const w = toWorld(sp.x, sp.y); canvas.style.cursor = nodeAt(w.x, w.y) ? 'pointer' : 'grab' }
    }
    const onUp = () => {
      if (dragging) {
        if (!moved) {
          const n = dragging
          if (n.rank === 'species' && n.key) onOpenRef.current?.(n.key)
          else if (n.rank !== 'root' && n.key) onFocusRef.current?.(n)
          else if (n.rank === 'root') onFocusRef.current?.(null) // Biota -> reset
        }
        dragging.fx = (dragging.id === data.center) ? dragging.fx : null
        dragging.fy = (dragging.id === data.center) ? dragging.fy : null
        sim.alphaTarget(0)
      }
      dragging = null; panning = false
    }
    const onWheel = (e) => {
      e.preventDefault(); const sp = screenPos(e); const w = toWorld(sp.x, sp.y)
      const f = e.deltaY < 0 ? 1.1 : 1 / 1.1; const nk = Math.max(0.3, Math.min(5, view.k * f))
      view.x = sp.x - w.x * nk; view.y = sp.y - w.y * nk; view.k = nk; draw()
    }
    canvas.addEventListener('mousedown', onDown)
    window.addEventListener('mousemove', onMove)
    window.addEventListener('mouseup', onUp)
    canvas.addEventListener('wheel', onWheel, { passive: false })
    canvas.style.cursor = 'grab'
    const onResize = () => { setSize(); sim.alpha(0.2).restart() }
    window.addEventListener('resize', onResize)

    return () => {
      sim.stop()
      canvas.removeEventListener('mousedown', onDown)
      window.removeEventListener('mousemove', onMove)
      window.removeEventListener('mouseup', onUp)
      canvas.removeEventListener('wheel', onWheel)
      window.removeEventListener('resize', onResize)
    }
  }, [data, dark])

  return (
    <div ref={wrapRef} style={{ position: 'absolute', inset: 0 }}>
      <canvas ref={canvasRef} style={{ display: 'block', touchAction: 'none' }} />
    </div>
  )
}