import { useEffect, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import { forceSimulation, forceLink, forceManyBody, forceCenter, forceCollide } from 'd3-force'
import { useTheme } from '../theme/ThemeContext'

const RANK_COLOR = {
  Kingdom: '#8060FF', Phylum: '#4D8FFF', Class: '#00C8FF', Order: '#00E8D8',
  Family: '#00F090', Genus: '#F0C000', Species: '#F04050',
}
const RANK_COLOR_LIGHT = {
  Kingdom: '#3A2A6A', Phylum: '#2A3A7A', Class: '#1A4A6A', Order: '#0A5C58',
  Family: '#1A5C30', Genus: '#7A5A0A', Species: '#7A1A2A',
}

function buildGraph(lineage, relatives, currentName, currentKey) {
  const nodes = []
  const links = []
  const idOf = (rank, name) => `${rank}:${name}`
  const seen = new Set()
  const add = (rank, name, extra = {}) => {
    const id = idOf(rank, name)
    if (seen.has(id)) return id
    seen.add(id)
    nodes.push({ id, rank, name, ...extra })
    return id
  }
  let prev = null
  for (const n of lineage) {
    const cap = n.rank ? n.rank.charAt(0).toUpperCase() + n.rank.slice(1).toLowerCase() : 'Species'
    const id = add(cap, n.name, { current: n.name === currentName, key: n.key, taxonRank: n.rank })
    if (prev) links.push({ source: prev, target: id })
    prev = id
  }
  const currentId = add('Species', currentName, { current: true, key: currentKey, taxonRank: 'species' })
  if (prev && prev !== currentId && !links.find((l) => l.target === currentId)) {
    links.push({ source: prev, target: currentId })
  }
  if (relatives) {
    const famId = relatives.family?.name ? idOf('Family', relatives.family.name) : null
    const genId = relatives.genus?.name ? idOf('Genus', relatives.genus.name) : null
    if (famId) {
      // asegurar que el nodo familia tenga su key
      const fam = nodes.find((x) => x.id === famId)
      if (fam) { fam.key = relatives.family.key; fam.taxonRank = 'family' }
      for (const g of (relatives.sibling_genera || [])) {
        const gid = add('Genus', g.name, { sibling: true, key: g.key, taxonRank: 'genus' })
        links.push({ source: famId, target: gid })
      }
    }
    if (genId) {
      const gen = nodes.find((x) => x.id === genId)
      if (gen) { gen.key = relatives.genus.key; gen.taxonRank = 'genus' }
      for (const sp of (relatives.sibling_species || [])) {
        const sid = add('Species', sp.name, { sibling: true, key: sp.key, taxonRank: 'species' })
        links.push({ source: genId, target: sid })
      }
    }
  }
  return { nodes, links }
}

export default function TaxonomyGraph({ lineage = [], relatives, currentName, currentKey, kingdom }) {
  const canvasRef = useRef(null)
  const wrapRef = useRef(null)
  const navigate = useNavigate()
  const { dark } = useTheme()
  // hover en ref (no en estado) para no recrear la simulacion
  const hoverRef = useRef(null)
  const navRef = useRef(navigate)
  navRef.current = navigate

  useEffect(() => {
    const canvas = canvasRef.current
    const wrap = wrapRef.current
    const ctx = canvas.getContext('2d')
    const edgeColor = dark ? '#2A3A5A' : '#B8BDD0'
    const textColor = dark ? '#DCE0F0' : '#0E1018'

    const H = 360
    let W = wrap.clientWidth
    // soporte de pantallas retina
    const dpr = window.devicePixelRatio || 1
    const setSize = () => {
      W = wrap.clientWidth
      canvas.width = W * dpr
      canvas.height = H * dpr
      canvas.style.width = W + 'px'
      canvas.style.height = H + 'px'
    }
    setSize()

    // transform de vista (paneo + zoom)
    const view = { x: 0, y: 0, k: 1 }

    const { nodes, links } = buildGraph(lineage, relatives, currentName, currentKey)
    nodes.forEach((n) => { n.x = W / 2 + (Math.random() - .5) * 80; n.y = H / 2 + (Math.random() - .5) * 80 })

    const sim = forceSimulation(nodes)
      .force('link', forceLink(links).id((d) => d.id).distance(48).strength(0.7))
      .force('charge', forceManyBody().strength(-160))
      .force('center', forceCenter(W / 2, H / 2))
      .force('collide', forceCollide().radius((d) => (d.current ? 16 : 11)))

    const radiusOf = (n) => (n.current ? 9 : n.sibling ? 5 : 7)

    function draw() {
      ctx.save()
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
      ctx.clearRect(0, 0, W, H)
      // aplicar paneo+zoom
      ctx.translate(view.x, view.y)
      ctx.scale(view.k, view.k)

      ctx.lineWidth = 1 / view.k
      for (const l of links) {
        ctx.beginPath()
        ctx.moveTo(l.source.x, l.source.y)
        ctx.lineTo(l.target.x, l.target.y)
        ctx.strokeStyle = edgeColor
        ctx.globalAlpha = 0.6
        ctx.stroke()
      }
      ctx.globalAlpha = 1
      for (const n of nodes) {
        const r = radiusOf(n)
        ctx.beginPath()
        ctx.arc(n.x, n.y, r, 0, Math.PI * 2)
        ctx.fillStyle = (dark ? RANK_COLOR : RANK_COLOR_LIGHT)[n.rank] || '#888'
        ctx.globalAlpha = n.sibling ? 0.75 : 1
        ctx.fill()
        if (n.current) {
          ctx.lineWidth = 2.5 / view.k
          ctx.strokeStyle = dark ? '#FFFFFF' : '#000000'
          ctx.globalAlpha = 1
          ctx.stroke()
        }
        ctx.globalAlpha = 1
        const showLabel = !n.sibling || hoverRef.current === n.id || n.current
        if (showLabel) {
          ctx.font = `${n.current ? 'bold ' : ''}${10 / view.k}px Iosevka, monospace`
          ctx.fillStyle = textColor
          ctx.textAlign = 'left'
          ctx.fillText(n.name, n.x + r + 3, n.y + 3)
        }
      }
      ctx.restore()
    }

    sim.on('tick', draw)

    // ── Coordenadas: pantalla -> mundo (deshaciendo el transform) ──
    const toWorld = (sx, sy) => ({
      x: (sx - view.x) / view.k,
      y: (sy - view.y) / view.k,
    })
    const screenPos = (e) => {
      const rect = canvas.getBoundingClientRect()
      return { x: e.clientX - rect.left, y: e.clientY - rect.top }
    }
    const nodeAt = (wx, wy) => {
      for (let i = nodes.length - 1; i >= 0; i--) {
        const n = nodes[i]
        const r = radiusOf(n) + 4
        if ((n.x - wx) ** 2 + (n.y - wy) ** 2 <= r * r) return n
      }
      return null
    }

    let draggingNode = null
    let panning = false
    let panStart = null
    let moved = false

    const onDown = (e) => {
      const sp = screenPos(e)
      const w = toWorld(sp.x, sp.y)
      const n = nodeAt(w.x, w.y)
      moved = false
      if (n) {
        draggingNode = n
        sim.alphaTarget(0.2).restart()
        n.fx = n.x; n.fy = n.y
      } else {
        panning = true
        panStart = { x: sp.x - view.x, y: sp.y - view.y }
      }
    }
    const onMove = (e) => {
      const sp = screenPos(e)
      if (draggingNode) {
        const w = toWorld(sp.x, sp.y)
        draggingNode.fx = w.x; draggingNode.fy = w.y
        moved = true
      } else if (panning) {
        view.x = sp.x - panStart.x
        view.y = sp.y - panStart.y
        moved = true
        draw()
      } else {
        const w = toWorld(sp.x, sp.y)
        const n = nodeAt(w.x, w.y)
        const newHover = n ? n.id : null
        if (newHover !== hoverRef.current) {
          hoverRef.current = newHover
          canvas.style.cursor = n ? 'pointer' : 'grab'
          draw()  // redibuja solo, sin reiniciar la simulacion
        }
      }
    }
    const onUp = (e) => {
      if (draggingNode) {
        // si no hubo arrastre real, tratarlo como clic -> navegar
        if (!moved && draggingNode.key && !draggingNode.current) {
          const r = draggingNode.taxonRank || 'species'
          if (r === 'species') navRef.current(`/species/${draggingNode.key}`)
          else navRef.current(`/taxon/${r}/${draggingNode.key}`)
        }
        draggingNode.fx = null; draggingNode.fy = null
        sim.alphaTarget(0)
      }
      draggingNode = null
      panning = false
    }
    const onWheel = (e) => {
      e.preventDefault()
      const sp = screenPos(e)
      const w = toWorld(sp.x, sp.y)
      const factor = e.deltaY < 0 ? 1.1 : 1 / 1.1
      const newK = Math.max(0.3, Math.min(4, view.k * factor))
      // zoom centrado en el cursor
      view.x = sp.x - w.x * newK
      view.y = sp.y - w.y * newK
      view.k = newK
      draw()
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
  }, [lineage, relatives, currentName, currentKey, dark, kingdom])

  const RANKS = ['kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species']
  const RANK_ES = { kingdom: 'Reino', phylum: 'Filo', class: 'Clase', order: 'Orden', family: 'Familia', genus: 'Genero', species: 'Especie' }
  const RANK_CAP = { kingdom: 'Kingdom', phylum: 'Phylum', class: 'Class', order: 'Order', family: 'Family', genus: 'Genus', species: 'Species' }

  return (
    <div ref={wrapRef} style={{ position: 'relative', width: '100%' }}>
      <canvas ref={canvasRef} style={{ display: 'block', width: '100%', height: 360, touchAction: 'none' }} />
      <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginTop: 8, fontSize: 9 }}>
        {RANKS.map((rank) => (
          <span key={rank} style={{ display: 'flex', alignItems: 'center', gap: 4, color: 'var(--text2)' }}>
            <span style={{ width: 8, height: 8, borderRadius: '50%', background: (dark ? RANK_COLOR : RANK_COLOR_LIGHT)[RANK_CAP[rank]], display: 'inline-block' }} />
            {RANK_ES[rank]}
          </span>
        ))}
      </div>
      <p className="muted" style={{ fontSize: 9, marginTop: 4 }}>Arrastra el fondo para mover, rueda para zoom, arrastra un nodo para moverlo, clic en una especie para abrir su ficha.</p>
    </div>
  )
}