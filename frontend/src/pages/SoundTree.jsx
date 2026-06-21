import { useEffect, useState, useMemo, useCallback } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import { api } from '../api/client'
import { useTheme } from '../theme/ThemeContext'
import { kingdomStyleVars, kingdomAccent } from '../theme/kingdomColor'
import { useSetKingdom } from '../theme/KingdomContext'
import { useKeyboardShortcuts } from '../hooks/useKeyboardShortcuts'
import { useElapsedTimer, formatElapsed } from '../hooks/useElapsedTimer'
import LoadingSpinner from '../components/LoadingSpinner'
import LinNeoLogo from '../components/LinNeoLogo'
import SoundPlayerBox from '../components/SoundPlayerBox'
import { layoutTree } from '../theme/soundTreeLayout'
import '../theme/bookworm.css'

const MAX_TABS = 12
const MAX_CHILDREN = 3 // tope por nodo en TODOS los niveles, para evitar scroll horizontal
const LEAF_R = 26
const NODE_R = 7

const RANK_ES = {
  domain: 'Dominio', kingdom: 'Reino', phylum: 'Filo', class: 'Clase', order: 'Orden',
  family: 'Familia', genus: 'Genero', species: 'Especie',
}

function rndSample(arr, n) {
  const copy = [...arr]
  for (let i = copy.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1))
    ;[copy[i], copy[j]] = [copy[j], copy[i]]
  }
  return copy.slice(0, n)
}

export default function SoundTree() {
  const { rank, key } = useParams()
  const navigate = useNavigate()
  const { dark, toggle: toggleTheme } = useTheme()
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const timer = useElapsedTimer()

  // que 3 hijos se muestran de cada nodo con mas de 3 hijos reales:
  // { [nodeId]: [childId, childId, childId] }
  const [visibleChildren, setVisibleChildren] = useState({})

  const [tabs, setTabs] = useState([])

  useSetKingdom(data?.kingdom)

  useEffect(() => {
    let alive = true
    setLoading(true); setError(null); setData(null)
    setTabs([]); setVisibleChildren({})
    timer.start()
    api.taxonSounds(rank, key, 250)
      .then((d) => { if (alive) { setData(d); setLoading(false); timer.stop() } })
      .catch((e) => { if (alive) { setError(e.message); setLoading(false); timer.stop() } })
    return () => { alive = false }
  }, [rank, key])

  // pool real de hijos de cada nodo intermedio: si el backend lo manda
  // (children_pool), se usa; si un nodo no esta ahi (raro, ej. el propio
  // root sin entrada), se cae al edge-based childrenOf de respaldo.
  const fallbackChildrenOf = useMemo(() => {
    if (!data?.edges) return {}
    const m = {}
    for (const e of data.edges) (m[e.source] ||= []).push(e.target)
    return m
  }, [data])

  const poolFor = useCallback((nodeId) => {
    return data?.children_pool?.[nodeId] || (fallbackChildrenOf[nodeId] || []).map((cid) => {
      // respaldo: si no hay pool, usa lo que ya esta en el arbol (sin extras)
      return data.nodes.find((n) => n.id === cid)
    }).filter(Boolean)
  }, [data, fallbackChildrenOf])

  // por cada nodo del arbol (incluida la raiz), decide que 3 hijos se ven.
  // Se inicializa una vez al cargar `data`; rerolls posteriores solo tocan
  // el nodo afectado via setVisibleChildren.
  useEffect(() => {
    if (!data?.nodes) return
    const initial = {}
    const allIds = [data.root_id, ...data.nodes.map((n) => n.id)]
    for (const nodeId of allIds) {
      const pool = poolFor(nodeId)
      if (pool.length === 0) continue
      // preferir los hijos que YA estan en el arbol (llevan a sonido real)
      // antes que rellenar con extras del pool, para que la primera vista
      // priorice ramas con contenido.
      const inTree = (fallbackChildrenOf[nodeId] || [])
      const rest = pool.map((c) => c.id).filter((cid) => !inTree.includes(cid))
      const chosen = [...inTree, ...rndSample(rest, Math.max(0, MAX_CHILDREN - inTree.length))].slice(0, MAX_CHILDREN)
      initial[nodeId] = chosen
    }
    setVisibleChildren(initial)
  }, [data]) // eslint-disable-line react-hooks/exhaustive-deps

  function rerollNode(nodeId) {
    const pool = poolFor(nodeId)
    if (pool.length <= MAX_CHILDREN) return // no hay de donde elegir distinto
    const chosen = rndSample(pool.map((c) => c.id), MAX_CHILDREN)
    setVisibleChildren((prev) => {
      const next = { ...prev, [nodeId]: chosen }
      // los nodos nuevos que entran al arbol (no estaban antes entre los
      // hijos visibles de algun padre) necesitan su PROPIA seleccion de
      // hijos inicializada de forma recursiva, si no quedarian ramas
      // truncadas sin nada debajo.
      function ensureInitialized(id) {
        if (next[id] !== undefined) return
        const p = poolFor(id)
        if (p.length === 0) return
        const ids = p.map((c) => c.id)
        const sel = rndSample(ids, MAX_CHILDREN)
        next[id] = sel
        for (const cid of sel) ensureInitialized(cid)
      }
      for (const childId of chosen) ensureInitialized(childId)
      return next
    })
  }

  // construir el subconjunto visible de nodes/edges a partir de
  // visibleChildren, empezando desde la raiz (BFS).
  const { visibleNodes, visibleEdges, nodesById, poolSizeById } = useMemo(() => {
    if (!data?.nodes) return { visibleNodes: [], visibleEdges: [], nodesById: {}, poolSizeById: {} }
    const byId = Object.fromEntries(data.nodes.map((n) => [n.id, n]))
    const poolById = {}
    for (const n of data.nodes) poolById[n.id] = poolFor(n.id).length
    poolById[data.root_id] = poolFor(data.root_id).length

    const nodes = []
    const edges = []
    const seen = new Set()
    const queue = [data.root_id]
    while (queue.length) {
      const id = queue.shift()
      if (seen.has(id)) continue
      seen.add(id)
      const nodeData = id === data.root_id
        ? { id: data.root_id, name: data.name, rank: data.rank, key: data.key, kingdom: data.kingdom }
        : byId[id]
      if (nodeData) nodes.push(nodeData)
      const chosenChildren = visibleChildren[id] || []
      for (const cid of chosenChildren) {
        edges.push({ source: id, target: cid })
        if (!seen.has(cid)) queue.push(cid)
        // si el hijo elegido no estaba en data.nodes (vino del pool, p.ej.
        // una especie sin sonido que solo aparecio al hacer reroll), buscarlo
        // en el pool del padre para tener su info completa.
        if (!byId[cid]) {
          const fromPool = poolFor(id).find((c) => c.id === cid)
          if (fromPool) byId[cid] = fromPool
        }
      }
    }
    return { visibleNodes: nodes, visibleEdges: edges, nodesById: byId, poolSizeById: poolById }
  }, [data, visibleChildren, poolFor])

  const { positions, width, height, childrenOf } = useMemo(() => {
    if (!data || visibleNodes.length === 0) return { positions: {}, width: 0, height: 0, childrenOf: {} }
    return layoutTree(visibleNodes, visibleEdges, data.root_id)
  }, [data, visibleNodes, visibleEdges])

  useKeyboardShortcuts({ navigate, toggleTheme })

  function openLeaf(node) {
    const tabId = node.id
    setTabs((prev) => {
      if (prev.some((t) => t.id === tabId)) return prev
      if (prev.length >= MAX_TABS) return prev
      return [...prev, {
        id: tabId,
        name: node.name,
        image: node.image || null,
        kingdom: node.kingdom,
        soundUrl: node.sounds?.[0],
        speciesKey: node.key,
      }]
    })
  }
  function closeTab(id) {
    setTabs((prev) => prev.filter((t) => t.id !== id))
  }
  function onNodeClick(node) {
    if (node.rank === 'species') {
      if (node.sounds?.length > 0) openLeaf(node)
      return
    }
    navigate(`/taxon/${node.rank}/${node.key}/sounds`)
  }

  const atTabLimit = tabs.length >= MAX_TABS

  return (
    <div className="bookworm-scope" style={kingdomStyleVars(data?.kingdom, dark)}>
      <div className="bw-page bw-page-wide">
        <div className="bw-topbar">
          <LinNeoLogo />
          <button className="bw-btn" onClick={() => navigate(-1)}>{'\u2039'} volver</button>
        </div>

        {loading && (
          <div style={{ marginTop: 20 }}>
            <LoadingSpinner inline={false} timeText={timer.elapsedMs != null ? formatElapsed(timer.elapsedMs) : null} />
          </div>
        )}
        {error && <p style={{ marginTop: 20, color: 'var(--bw-danger)' }}>{error}</p>}

        {data && (
          <>
            <div className="bw-header">
              <div className="bw-header-title">
                <div className="bw-rank">Sonidos · {RANK_ES[data.rank] || data.rank}</div>
                <h1>{data.name}</h1>
                <p className="bw-muted">
                  Mostrando {data.shown_with_sound || 0} de {data.total_with_sound || 0} especie(s) con sonido.
                  {atTabLimit && ' Cierra una pestana del reproductor para abrir otra.'}
                </p>
              </div>
            </div>

            {(!data.nodes || data.nodes.length === 0) ? (
              <p className="bw-muted" style={{ marginTop: 20 }}>Ninguna especie de este grupo tiene sonidos.</p>
            ) : (
              <svg className="st-svg" width={width + 60} height={height + 60} viewBox={`-30 -10 ${width + 60} ${height + 60}`}>
                <defs>
                  <clipPath id="st-leaf-clip" clipPathUnits="objectBoundingBox">
                    <circle cx="0.5" cy="0.5" r="0.5" />
                  </clipPath>
                </defs>
                {/* lineas: padre -> hijo. Se retrocede el radio de cada nodo
                    a lo largo del angulo REAL entre los dos puntos (no solo
                    en X), porque el padre casi nunca comparte la misma Y que
                    el hijo -- la linea real va en diagonal la mayoria de las
                    veces, asi que un offset de un solo eje deja la linea
                    entrando mal al circulo (corta, larga o desalineada). */}
                {visibleEdges.map((e, i) => {
                  const p1 = positions[e.source]
                  const p2 = positions[e.target]
                  if (!p1 || !p2) return null
                  const r1 = isLeaf(e.source, childrenOf) ? LEAF_R : NODE_R
                  const r2 = isLeaf(e.target, childrenOf) ? LEAF_R : NODE_R
                  const dx = p2.x - p1.x
                  const dy = p2.y - p1.y
                  const dist = Math.sqrt(dx * dx + dy * dy) || 1
                  const ux = dx / dist
                  const uy = dy / dist
                  return (
                    <line key={i}
                      x1={p1.x + ux * r1} y1={p1.y + uy * r1}
                      x2={p2.x - ux * r2} y2={p2.y - uy * r2}
                      stroke="var(--bw-border)" strokeWidth="1.5"
                    />
                  )
                })}
                {/* nodos */}
                {visibleNodes.map((node) => {
                  const pos = positions[node.id]
                  if (!pos) return null
                  const leaf = isLeaf(node.id, childrenOf)
                  const poolSize = poolSizeById[node.id] || 0
                  const canReroll = poolSize > MAX_CHILDREN

                  if (!leaf) {
                    return (
                      <g key={node.id} transform={`translate(${pos.x}, ${pos.y})`}>
                        <g className="st-node-mid" onClick={() => onNodeClick(node)}>
                          <circle r={NODE_R} fill="var(--bw-accent)" />
                          <text y={-12} textAnchor="middle" className="st-node-label">{node.name}</text>
                        </g>
                        {canReroll && (
                          <g className="st-reroll" transform={`translate(0, ${NODE_R + 14})`}
                            onClick={(ev) => { ev.stopPropagation(); rerollNode(node.id) }}>
                            <circle r="9" />
                            <text textAnchor="middle" dominantBaseline="central" fontSize="11">{'\u21BB'}</text>
                          </g>
                        )}
                      </g>
                    )
                  }

                  const { accent } = kingdomAccent(node.kingdom, dark)
                  const hasSound = node.sounds?.length > 0
                  return (
                    <g key={node.id} transform={`translate(${pos.x}, ${pos.y})`}>
                      <g className={`st-leaf ${hasSound ? '' : 'st-leaf-nosound'}`} onClick={() => onNodeClick(node)}>
                        <circle r={LEAF_R} fill="var(--bw-bg2)" stroke={accent} strokeWidth="2.5" />
                        {node.image ? (
                          <image href={node.image} x={-LEAF_R} y={-LEAF_R} width={LEAF_R * 2} height={LEAF_R * 2}
                            clipPath="url(#st-leaf-clip)" preserveAspectRatio="xMidYMid slice" />
                        ) : (
                          <text textAnchor="middle" dominantBaseline="central" fontSize="20">{'\u266A'}</text>
                        )}
                        <text y={LEAF_R + 16} textAnchor="middle" className="st-leaf-label">{node.name}</text>
                      </g>
                    </g>
                  )
                })}
              </svg>
            )}
          </>
        )}
      </div>

      <SoundPlayerBox
        tabs={tabs}
        onCloseTab={closeTab}
      />
    </div>
  )
}

function isLeaf(id, childrenOf) {
  return !childrenOf[id] || childrenOf[id].length === 0
}