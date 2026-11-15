import { useEffect, useState, useRef } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { api } from '../api/client'
import { thumb } from '../api/media'
import ShuiGraph from '../components/ShuiGraph'
import { useTheme } from '../theme/ThemeContext'
import { useSetKingdom } from '../theme/KingdomContext'
import { kingdomInk } from '../theme/kingdomColor'
import SiteHeader from '../components/SiteHeader'
import SearchBox, { hrefOf } from '../components/SearchBox'
import { useKeyboardShortcuts } from '../hooks/useKeyboardShortcuts'
import { useElapsedTimer, formatElapsed } from '../hooks/useElapsedTimer'
import LoadingSpinner from '../components/LoadingSpinner'
import '../theme/home.css'

// Inicio: buscador, grafo de Biota → dominios → reinos (o del taxón en foco),
// filtro por reino y especies al azar.

const RANKS = ['biota', 'domain', 'kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species']
const RANK_ES = {
  biota: 'Biota', domain: 'Dominio', kingdom: 'Reino', phylum: 'Filo', class: 'Clase',
  order: 'Orden', family: 'Familia', genus: 'Género', species: 'Especie',
}
const DEFAULT_KINGDOMS = ['Animalia', 'Plantae', 'Fungi']
const LIST_MAX = 40  // hijos listados bajo el grafo; el resto está en la lámina

// { dominio: [reinos] } a partir de los nodos y aristas reales del grafo inicial.
function deriveDomainGroups(full) {
  if (!full) return []
  const byId = Object.fromEntries(full.nodes.map((n) => [n.id, n]))
  return full.nodes.filter((n) => n.rank === 'domain').map((d) => ({
    id: d.id, name: d.name,
    kingdoms: full.links.filter((l) => l.source === d.id).map((l) => byId[l.target]).filter((n) => n?.rank === 'kingdom'),
  }))
}

// Imagen de la lámina: miniatura primero, luego el original (una miniatura más ancha
// que el original falla) y al final el letrero, nunca el ícono roto.
function Plate({ src }) {
  const tries = src ? [...new Set([thumb(src), src])] : []
  const [i, setI] = useState(0)
  useEffect(() => setI(0), [src])
  return (
    <span className="plate">
      {i < tries.length
        ? <img src={tries[i]} alt="" loading="lazy" onError={() => setI((n) => n + 1)} />
        : <span className="plate-empty">Sin lámina</span>}
    </span>
  )
}

export default function Shui() {
  const navigate = useNavigate()
  const { dark, toggle: toggleTheme } = useTheme()

  const [graphData, setGraphData] = useState(null)
  const [fullGraph, setFullGraph] = useState(null)  // Biota → dominios → reinos, sin filtrar
  const [focusKingdom, setFocusKingdom] = useState(null)
  const [focusNode, setFocusNode] = useState(null)
  const [kingdoms, setKingdoms] = useState([])
  const [activeKingdoms, setActiveKingdoms] = useState(new Set())
  const [domainGroups, setDomainGroups] = useState([])

  const searchRef = useRef(null)

  const [examples, setExamples] = useState([])
  const [exLoading, setExLoading] = useState(true)
  const [exError, setExError] = useState(false)

  useSetKingdom(focusKingdom)
  const initialTimer = useElapsedTimer()
  const [initialLoading, setInitialLoading] = useState(true)
  const focusTimer = useElapsedTimer()
  const [focusLoading, setFocusLoading] = useState(false)

  useEffect(() => {
    initialTimer.start()
    let pending = 2
    const settle = () => { pending -= 1; if (pending === 0) { setInitialLoading(false); initialTimer.stop() } }
    api.graph().then((g) => { setFullGraph(g); setDomainGroups(deriveDomainGroups(g)) }).finally(settle)
    api.kingdoms().then((ks) => {
      setKingdoms(ks)
      const names = ks.map((k) => k.name)
      setActiveKingdoms(new Set(DEFAULT_KINGDOMS.filter((n) => names.includes(n))))
    }).finally(settle)
    // El grafo visible y los ejemplos los arma el efecto de abajo cuando ya están el grafo y los reinos.
  }, [])

  // Vista Biota: el grafo visible respeta los reinos marcados. Depende de fullGraph para no
  // perder la primera vuelta si /kingdoms responde antes que /graph.
  useEffect(() => {
    if (focusNode || !fullGraph || initialLoading) return
    const full = fullGraph
    const nodes = full.nodes.filter((n) => n.rank === 'biota' || n.rank === 'domain' || activeKingdoms.has(n.kingdom))
    const ids = new Set(nodes.map((n) => n.id))
    setGraphData({ ...full, nodes, links: full.links.filter((l) => ids.has(l.source) && ids.has(l.target)) })
    loadKingdomExamples()
  }, [activeKingdoms, focusNode, fullGraph, initialLoading])

  function loadKingdomExamples() {
    setExLoading(true); setExError(false)
    api.randomPool([...activeKingdoms], 8)
      .then((rows) => setExamples(rows || []))
      .catch(() => setExError(true))
      .finally(() => setExLoading(false))
  }
  function reloadExamples() {
    if (!focusNode) return loadKingdomExamples()
    setExLoading(true); setExError(false)
    api.randomDescendants(focusNode.key, 8)
      .then((rows) => setExamples(rows || []))
      .catch(() => setExError(true))
      .finally(() => setExLoading(false))
  }

  // Cada foco (o reinicio) incrementa el contador; una respuesta tardía solo se
  // aplica si su operación sigue siendo la vigente.
  const focusOpRef = useRef(0)
  function focusOn(node) {
    const opId = ++focusOpRef.current
    if (!node) {
      setFocusKingdom(null); setFocusNode(null)
      setFocusLoading(false); focusTimer.stop()
      return   // el efecto de activeKingdoms/focusNode rehace el grafo y los ejemplos
    }
    setFocusLoading(true)
    focusTimer.start()
    api.graphFocus(node.key).then((g) => {
      if (focusOpRef.current !== opId) return
      // Al bajar nunca se cambia de reino: si el backend no lo sabe, se conserva el anterior.
      const nextKingdom = g.kingdom || focusKingdom
      setGraphData({ ...g, kingdom: nextKingdom })
      setFocusKingdom(nextKingdom)
      setFocusNode(node)
      setFocusLoading(false); focusTimer.stop()
      setExLoading(true); setExError(false)
      api.randomDescendants(node.key, 8)
        .then((rows) => { if (focusOpRef.current === opId) setExamples(rows || []) })
        .catch(() => { if (focusOpRef.current === opId) setExError(true) })
        .finally(() => { if (focusOpRef.current === opId) setExLoading(false) })
    }).catch(() => {
      if (focusOpRef.current === opId) { setFocusLoading(false); focusTimer.stop() }
    })
  }

  function toggleKingdom(name) {
    setActiveKingdoms((prev) => {
      const next = new Set(prev)
      next.has(name) ? next.delete(name) : next.add(name)
      return next
    })
  }
  // Marcar un dominio: si todos sus reinos están, los quita; si no, agrega los que falten.
  function toggleDomain(group) {
    const names = group.kingdoms.map((k) => k.name)
    setActiveKingdoms((prev) => {
      const allOn = names.every((n) => prev.has(n))
      const next = new Set(prev)
      names.forEach((n) => (allOn ? next.delete(n) : next.add(n)))
      return next
    })
  }
  function domainState(group) {
    const on = group.kingdoms.filter((k) => activeKingdoms.has(k.name)).length
    return on === 0 ? 'none' : on === group.kingdoms.length ? 'all' : 'partial'
  }

  function resetAll() {
    const names = kingdoms.map((k) => k.name)
    setActiveKingdoms(new Set(DEFAULT_KINGDOMS.filter((n) => names.includes(n))))
    focusOn(null)
  }

  useKeyboardShortcuts({
    navigate, toggleTheme,
    shui: {
      searchRef, resetAll, reloadExamples,
      navigateToList: () => focusNode && navigate(hrefOf(focusNode)),
      selectAllKingdoms: () => setActiveKingdoms(new Set(kingdoms.map((k) => k.name))),
      deselectAllKingdoms: () => setActiveKingdoms(new Set()),
    },
  })

  // Primer nivel del grafo, para la lista navegable de abajo.
  const level1 = (() => {
    if (!graphData) return []
    const byId = Object.fromEntries(graphData.nodes.map((n) => [n.id, n]))
    return graphData.links.filter((l) => l.source === graphData.center).map((l) => byId[l.target]).filter(Boolean)
      .sort((a, b) => RANKS.indexOf(a.rank) - RANKS.indexOf(b.rank) || a.name.localeCompare(b.name))
  })()
  const centerName = focusNode?.name || 'Biota'

  return (
    <div className="folio home">
      <SiteHeader back={false}>
        <SearchBox inputRef={searchRef} />
        <button type="button" className="ctl" onClick={resetAll}>Reiniciar</button>
      </SiteHeader>

      <div className="home-body">
        <aside className="home-kingdoms" aria-labelledby="reinos-t">
          <h2 id="reinos-t" className="folio-h2">Reinos</h2>
          {domainGroups.map((g) => {
            const state = domainState(g)
            return (
              <fieldset key={g.id} className="domain" disabled={!!focusNode}>
                <legend>
                  <label className="check">
                    <input type="checkbox" checked={state === 'all'}
                      ref={(el) => { if (el) el.indeterminate = state === 'partial' }}
                      onChange={() => toggleDomain(g)} disabled={!!focusNode} />
                    <span className="domain-name">{g.name}</span>
                  </label>
                </legend>
                {g.kingdoms.map((k) => (
                  <label key={k.key} className="check kingdom">
                    <input type="checkbox" checked={activeKingdoms.has(k.name)} onChange={() => toggleKingdom(k.name)} />
                    <span className="dot" style={{ background: kingdomInk(k.name, dark) }} aria-hidden="true" />
                    {k.name}
                  </label>
                ))}
              </fieldset>
            )
          })}
          {focusNode && <p className="home-note">Reinicia el grafo para volver a filtrar por reino.</p>}
        </aside>

        <section className="home-main" aria-labelledby="centro">
          <div className="home-head" data-rank={focusNode?.rank}>
            <p className="taxon-rank">{focusNode ? RANK_ES[focusNode.rank] : 'Árbol de la vida'}</p>
            <h1 id="centro" className="taxon-name">{centerName}</h1>
          </div>

          <div className="home-graph">
            {graphData && <ShuiGraph data={graphData} onFocus={focusOn} onOpenSpecies={(k) => navigate(`/species/${k}`)} />}
            {(initialLoading || focusLoading) && (
              <div className="graph-loading">
                <LoadingSpinner inline={false} timeText={formatElapsed((focusLoading ? focusTimer : initialTimer).elapsedMs)} />
              </div>
            )}
          </div>
          <p className="graph-hint">
            <span className="hint-mouse">Haz clic en un nombre para centrar el grafo en él; Ctrl y la rueda acercan.</span>
            <span className="hint-touch">Toca un nombre para centrar el grafo en él.</span>
          </p>

          {level1.length > 0 && (
            <nav className="home-children" aria-label={`Cuelgan de ${centerName}`}>
              <span className="tool-label">Cuelgan de {centerName}:</span>
              <ul>
                {level1.slice(0, LIST_MAX).map((n) => (
                  <li key={n.id} data-rank={n.rank}>
                    <button type="button" className="ctl taxon-name"
                      onClick={() => (n.rank === 'species' ? navigate(`/species/${n.key}`) : focusOn(n))}>{n.name}</button>
                  </li>
                ))}
              </ul>
              {level1.length > LIST_MAX && focusNode && (
                <Link to={hrefOf(focusNode)}>y {level1.length - LIST_MAX} más en la lámina</Link>
              )}
            </nav>
          )}
          <p className="home-links">
            {focusNode && <Link to={hrefOf(focusNode)}>Abrir la lámina de {focusNode.name}</Link>}
            <Link to="/map">Explorar por país en el mapa</Link>
          </p>
        </section>
      </div>

      <section className="home-examples" aria-labelledby="azar">
        <div className="examples-head">
          <h2 id="azar" className="folio-h2">Especies al azar{focusNode ? ` de ${focusNode.name}` : ''}</h2>
          <button type="button" className="ctl" onClick={reloadExamples} disabled={exLoading}>Otras</button>
        </div>
        {exError && <p className="home-note">No se pudieron cargar ejemplos.</p>}
        {!exError && !exLoading && examples.length === 0 && <p className="home-note">Sin especies con descripción aquí.</p>}
        <ul className="examples" aria-busy={exLoading}>
          {(exLoading ? Array.from({ length: 8 }, () => null) : examples).map((ex, i) => (
            <li key={ex?.key ?? `ph${i}`} className={ex ? '' : 'placeholder'}>
              {ex && (
                <Link to={`/species/${ex.key}`} data-kingdom={ex.kingdom}>
                  <Plate src={ex.image} />
                  <span className="taxon-name binomial">{ex.name}</span>
                  {ex.common_names?.[0] && <span className="example-common">{ex.common_names[0]}</span>}
                </Link>
              )}
            </li>
          ))}
        </ul>
      </section>
    </div>
  )
}
