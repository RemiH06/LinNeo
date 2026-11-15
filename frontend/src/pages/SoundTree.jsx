import { useCallback, useEffect, useMemo, useState } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { api } from '../api/client'
import { useTheme } from '../theme/ThemeContext'
import { useSetKingdom } from '../theme/KingdomContext'
import { useKeyboardShortcuts } from '../hooks/useKeyboardShortcuts'
import { useElapsedTimer, formatElapsed } from '../hooks/useElapsedTimer'
import LoadingSpinner from '../components/LoadingSpinner'
import SiteHeader from '../components/SiteHeader'
import Brace from '../components/Brace'
import SoundDock from '../components/SoundDock'
import '../theme/sounds.css'

/*
  Sonidos de un taxón como clave dicotómica: cada grupo abre una llave con
  hasta MAX_CHILDREN hijos (prefiriendo los que llevan a especies con sonido)
  y "Otras" baraja la selección. Las especies con sonido se escuchan en el
  atril de abajo, que admite varias a la vez.
*/
const MAX_TABS = 12
const MAX_CHILDREN = 3
const RANK_ES = {
  biota: 'Biota', domain: 'Dominio', kingdom: 'Reino', phylum: 'Filo', class: 'Clase', order: 'Orden',
  family: 'Familia', genus: 'Género', species: 'Especie',
}
const fmt = (n) => (n ?? 0).toLocaleString('es-MX')

function sample(arr, n) {
  const a = [...arr]
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]]
  }
  return a.slice(0, n)
}

export default function SoundTree() {
  const { key } = useParams()
  const navigate = useNavigate()
  const { toggle: toggleTheme } = useTheme()
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [visible, setVisible] = useState({})   // { nodeId: [childId, …] }
  const [tabs, setTabs] = useState([])
  const timer = useElapsedTimer()
  useSetKingdom(data?.kingdom)
  useKeyboardShortcuts({ navigate, toggleTheme })

  useEffect(() => {
    let alive = true
    setLoading(true); setError(null); setData(null); setVisible({})
    timer.start()
    api.taxonSounds(key, 250)
      .then((d) => { if (alive) setData(d) })
      .catch((e) => { if (alive) setError(e.message) })
      .finally(() => { if (alive) { setLoading(false); timer.stop() } })
    return () => { alive = false }
  }, [key])

  // Hijos del árbol (los que llevan a sonido) y el resto del pool para barajar.
  const treeKids = useMemo(() => {
    const m = {}
    for (const e of data?.edges || []) (m[e.source] ||= []).push(e.target)
    return m
  }, [data])
  const byId = useMemo(() => {
    const m = Object.fromEntries((data?.nodes || []).map((n) => [n.id, n]))
    for (const pool of Object.values(data?.children_pool || {})) for (const c of pool) m[c.id] ||= c
    return m
  }, [data])
  const poolFor = useCallback((id) => (data?.children_pool?.[id] || (treeKids[id] || []).map((c) => byId[c]).filter(Boolean)), [data, treeKids, byId])

  useEffect(() => {
    if (!data?.nodes) return
    const init = {}
    for (const id of [data.root_id, ...data.nodes.map((n) => n.id)]) {
      const inTree = treeKids[id] || []
      const rest = poolFor(id).map((c) => c.id).filter((c) => !inTree.includes(c))
      if (inTree.length || rest.length) init[id] = [...inTree, ...sample(rest, Math.max(0, MAX_CHILDREN - inTree.length))].slice(0, MAX_CHILDREN)
    }
    setVisible(init)
  }, [data]) // eslint-disable-line react-hooks/exhaustive-deps

  function reroll(id) {
    const chosen = sample(poolFor(id).map((c) => c.id), MAX_CHILDREN)
    setVisible((prev) => {
      const next = { ...prev, [id]: chosen }
      const ensure = (cid) => {   // una rama nueva necesita su propia selección hacia abajo
        if (next[cid] !== undefined) return
        const p = poolFor(cid).map((c) => c.id)
        if (!p.length) return
        next[cid] = sample(p, MAX_CHILDREN)
        next[cid].forEach(ensure)
      }
      chosen.forEach(ensure)
      return next
    })
  }

  function listen(node) {
    setTabs((prev) => (prev.some((t) => t.id === node.id) || prev.length >= MAX_TABS ? prev : [...prev, {
      id: node.id, name: node.name, image: node.image || null, kingdom: node.kingdom,
      soundUrl: node.sounds[0], speciesKey: node.key, commonName: node.common_names?.[0],
    }]))
  }

  // Función, no componente: uno definido aquí dentro se remontaría en cada render y el foco se perdería.
  function branch(id, depth) {
    const node = id === data.root_id ? { id, name: data.name, rank: data.rank, key: data.key } : byId[id]
    if (!node) return null
    const kids = visible[id] || []
    if (node.rank === 'species') {
      const has = node.sounds?.length > 0
      const open = tabs.some((t) => t.id === id)
      return (
        <li key={id} className="st-leaf" data-rank="species">
          {has
            ? <button type="button" className="ctl st-play" aria-pressed={open} disabled={!open && tabs.length >= MAX_TABS} onClick={() => listen(node)}>{open ? 'En el atril' : 'Escuchar'}</button>
            : <span className="st-mute">sin sonido</span>}
          <Link className="taxon-name" to={`/species/${node.key}`}>{node.name}</Link>
          {node.common_names?.[0] && <span className="entry-note">{node.common_names[0]}</span>}
        </li>
      )
    }
    const canShuffle = poolFor(id).length > MAX_CHILDREN
    return (
      <li key={id} className="st-node" data-rank={node.rank}>
        <div className="st-head">
          <span className="st-rank">{RANK_ES[node.rank] || node.rank}</span>{' '}
          {depth === 0
            ? <span className="taxon-name">{node.name}</span>
            : <Link className="taxon-name" to={`/taxon/${node.rank}/${node.key}/sounds`} title="Ver los sonidos de este grupo">{node.name}</Link>}
          {canShuffle && <button type="button" className="ctl st-shuffle" onClick={() => reroll(id)}>Otras (de {fmt(poolFor(id).length)})</button>}
        </div>
        {kids.length > 0 && (
          <Brace><ul className="st-kids">{kids.map((c) => branch(c, depth + 1))}</ul></Brace>
        )}
      </li>
    )
  }

  return (
    <div className={`folio${tabs.length ? ' with-dock' : ''}`}>
      <SiteHeader />
      {loading && <LoadingSpinner inline={false} timeText={formatElapsed(timer.elapsedMs)} />}
      {error && <p className="taxon-error" role="alert">No se pudieron cargar los sonidos: {error}</p>}
      {data && (
        <>
          <header className="taxon-head" data-rank={data.rank}>
            <p className="taxon-rank">Sonidos · {RANK_ES[data.rank] || data.rank}</p>
            <h1 className="taxon-name"><Link to={`/taxon/${data.rank}/${data.key}`}>{data.name}</Link></h1>
            <p className="taxon-summary">
              {fmt(data.total_with_sound)} {data.total_with_sound === 1 ? 'especie tiene' : 'especies tienen'} grabación
              {data.shown_with_sound < data.total_with_sound ? `; aquí se recorren ${fmt(data.shown_with_sound)}` : ''}.
              {' '}Cada grupo muestra {MAX_CHILDREN}; «Otras» cambia la selección.
            </p>
          </header>
          {!data.nodes?.length
            ? <p className="taxon-count">Ninguna especie de este grupo tiene sonidos.</p>
            : <ul className="st-tree">{branch(data.root_id, 0)}</ul>}
          {tabs.length >= MAX_TABS && <p className="taxon-count">El atril tiene {MAX_TABS}; cierra una grabación para abrir otra.</p>}
        </>
      )}
      <SoundDock tabs={tabs} onClose={(id) => setTabs((p) => p.filter((t) => t.id !== id))} />
    </div>
  )
}
