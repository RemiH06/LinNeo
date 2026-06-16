import { useEffect, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import { forceSimulation, forceLink, forceManyBody, forceCenter, forceCollide } from 'd3-force'
import { useTheme } from '../theme/ThemeContext'

const RANK_COLOR = {
  kingdom: '#8060FF', phylum: '#4D8FFF', class: '#00C8FF', order: '#00E8D8',
  family: '#00F090', genus: '#F0C000', species: '#F04050',
}
const RANK_COLOR_LIGHT = {
  kingdom: '#3A2A6A', phylum: '#2A3A7A', class: '#1A4A6A', order: '#0A5C58',
  family: '#1A5C30', genus: '#7A5A0A', species: '#7A1A2A',
}
const rankColor = (rank, dark) => (dark ? RANK_COLOR : RANK_COLOR_LIGHT)[(rank || 'species').toLowerCase()] || '#888'

/*
  Grafo de navegacion para un nodo taxonomico (no-especie).
  Dibuja: linaje (ancestros) -> nodo actual -> hijos directos.
  Clic: especie -> /species/key ; taxon -> /taxon/rank/key.
  Colorea por reino (hue) + rango (luminosidad).
*/
export default function TaxonGraph({ lineage = [], current, children = [], kingdom, height = 300 }) {
  const canvasRef = useRef(null)
  const wrapRef = useRef(null)
  const navigate = useNavigate()
  const navRef = useRef(navigate); navRef.current = navigate
  const { dark } = useTheme()

  useEffect(() => {
    const canvas = canvasRef.current
    const wrap = wrapRef.current
    const ctx = canvas.getContext('2d')
    const edgeColor = dark ? '#2A3A5A' : '#B8BDD0'
    const textColor = dark ? '#DCE0F0' : '#0E1018'

    const H = height
    let W = wrap.clientWidth
    const dpr = window.devicePixelRatio || 1
    const setSize = () => {
      W = wrap.clientWidth
      canvas.width = W * dpr; canvas.height = H * dpr
      canvas.style.width = W + 'px'; canvas.style.height = H + 'px'
    }
    setSize()
    const view = { x: 0, y: 0, k: 1 }

    // construir nodos
    const nodes = []
    const links = []
    const seen = new Set()
    const add = (rank, name, extra = {}) => {
      const id = `${rank}:${name}`
      if (seen.has(id)) return id
      seen.add(id)
      nodes.push({ id, rank, name, ...extra })
      return id
    }
    let prev = null
    for (const n of lineage) {
      const isCurrent = n.rank === current.rank && n.key === current.key
      const id = add(n.rank, n.name, { key: n.key, taxonRank: n.rank, current: isCurrent })
      if (prev) links.push({ source: prev, target: id })
      prev = id
    }
    const curId = add(current.rank, current.name, { key: current.key, taxonRank: current.rank, current: true })
    if (prev && prev !== curId && !links.find((l) => l.target === curId)) {
      links.push({ source: prev, target: curId })
    }
    for (const c of children) {
      const cid = add(c.rank, c.name, { key: c.key, taxonRank: c.rank, child: true })
      links.push({ source: curId, target: cid })
    }

    nodes.forEach((n) => { n.x = W / 2 + (Math.random() - .5) * 80; n.y = H / 2 + (Math.random() - .5) * 80 })

    const sim = forceSimulation(nodes)
      .force('link', forceLink(links).id((d) => d.id).distance(46).strength(0.6))
      .force('charge', forceManyBody().strength(-170))
      .force('center', forceCenter(W / 2, H / 2))
      .force('collide', forceCollide().radius((d) => (d.current ? 15 : 10)))

    const radiusOf = (n) => (n.current ? 9 : n.child ? 5 : 7)

    function draw() {
      ctx.save()
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
      ctx.clearRect(0, 0, W, H)
      ctx.translate(view.x, view.y); ctx.scale(view.k, view.k)
      ctx.lineWidth = 1 / view.k
      for (const l of links) {
        ctx.beginPath(); ctx.moveTo(l.source.x, l.source.y); ctx.lineTo(l.target.x, l.target.y)
        ctx.strokeStyle = edgeColor; ctx.globalAlpha = 0.6; ctx.stroke()
      }
      ctx.globalAlpha = 1
      for (const n of nodes) {
        const r = radiusOf(n)
        ctx.beginPath(); ctx.arc(n.x, n.y, r, 0, Math.PI * 2)
        ctx.fillStyle = rankColor(n.taxonRank, dark)
        ctx.globalAlpha = n.child ? 0.8 : 1; ctx.fill()
        if (n.current) {
          ctx.lineWidth = 2.5 / view.k; ctx.strokeStyle = dark ? '#FFF' : '#000'; ctx.globalAlpha = 1; ctx.stroke()
        }
        ctx.globalAlpha = 1
        const showLabel = !n.child || n.current
        if (showLabel) {
          ctx.font = `${n.current ? 'bold ' : ''}${10 / view.k}px Iosevka, monospace`
          ctx.fillStyle = textColor; ctx.textAlign = 'left'
          ctx.fillText(n.name, n.x + r + 3, n.y + 3)
        }
      }
      ctx.restore()
    }
    sim.on('tick', draw)

    const toWorld = (sx, sy) => ({ x: (sx - view.x) / view.k, y: (sy - view.y) / view.k })
    const screenPos = (e) => { const r = canvas.getBoundingClientRect(); return { x: e.clientX - r.left, y: e.clientY - r.top } }
    const nodeAt = (wx, wy) => {
      for (let i = nodes.length - 1; i >= 0; i--) {
        const n = nodes[i]; const r = radiusOf(n) + 4
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
        if (!moved && dragging.key && !dragging.current) {
          const r = dragging.taxonRank || 'species'
          navRef.current(r === 'species' ? `/species/${dragging.key}` : `/taxon/${r}/${dragging.key}`)
        }
        dragging.fx = null; dragging.fy = null; sim.alphaTarget(0)
      }
      dragging = null; panning = false
    }
    const onWheel = (e) => {
      e.preventDefault(); const sp = screenPos(e); const w = toWorld(sp.x, sp.y)
      const f = e.deltaY < 0 ? 1.1 : 1 / 1.1; const nk = Math.max(0.3, Math.min(4, view.k * f))
      view.x = sp.x - w.x * nk; view.y = sp.y - w.y * nk; view.k = nk; draw()
    }
    canvas.addEventListener('mousedown', onDown)
    window.addEventListener('mousemove', onMove)
    window.addEventListener('mouseup', onUp)
    canvas.addEventListener('wheel', onWheel, { passive: false })
    canvas.style.cursor = 'grab'
    const onResize = () => { setSize(); sim.force('center', forceCenter(W / 2, H / 2)); sim.alpha(0.2).restart() }
    window.addEventListener('resize', onResize)

    return () => {
      sim.stop()
      canvas.removeEventListener('mousedown', onDown)
      window.removeEventListener('mousemove', onMove)
      window.removeEventListener('mouseup', onUp)
      canvas.removeEventListener('wheel', onWheel)
      window.removeEventListener('resize', onResize)
    }
  }, [lineage, current, children, kingdom, dark, height])

  return (
    <div ref={wrapRef} style={{ position: 'relative', width: '100%' }}>
      <canvas ref={canvasRef} style={{ display: 'block', width: '100%', height, touchAction: 'none' }} />
      <p className="bw-muted" style={{ fontSize: 9, marginTop: 4 }}>Arrastra para mover, rueda para zoom, clic en un nodo para navegar.</p>
    </div>
  )
}