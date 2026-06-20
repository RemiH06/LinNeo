import { useEffect, useState, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import { api } from '../api/client'
import ShuiGraph from '../components/ShuiGraph'
import ShuiBackground from '../backgrounds/ShuiBackground'
import { useTheme } from '../theme/ThemeContext'
import { useSetKingdom } from '../theme/KingdomContext'
import { KINGDOM_HUE } from '../theme/kingdomColor'
import ThemeToggle from '../components/ThemeToggle'
import LinNeoLogo from '../components/LinNeoLogo'
import { useKeyboardShortcuts } from '../hooks/useKeyboardShortcuts'
import { useElapsedTimer, formatElapsed } from '../hooks/useElapsedTimer'
import LoadingSpinner from '../components/LoadingSpinner'
import '../theme/shui.css'

function glowFor(kingdom, dark) {
  const h = KINGDOM_HUE[kingdom]
  const gray = (kingdom === 'incertae sedis' || h == null)
  const hue = gray ? 210 : h
  const sat = gray ? 12 : (dark ? 80 : 65)
  return `hsl(${hue}, ${sat}%, ${dark ? 55 : 48}%)`
}
function swatchFor(kingdom, dark) {
  const h = KINGDOM_HUE[kingdom]
  const gray = (kingdom === 'incertae sedis' || h == null)
  return `hsl(${gray ? 210 : h}, ${gray ? 12 : 80}%, ${dark ? 55 : 45}%)`
}

const RANK_ES_SHORT = {
  domain: 'Dominio', kingdom: 'Reino', phylum: 'Filo', class: 'Clase',
  order: 'Orden', family: 'Familia', genus: 'Genero', species: 'Especie',
}

// Resalta la subcadena de `text` que coincide con `query` (case-insensitive).
function highlightMatch(text, query) {
  if (!query) return text
  const idx = text.toLowerCase().indexOf(query.toLowerCase())
  if (idx === -1) return text
  return (
    <>
      {text.slice(0, idx)}
      <mark className="sh-match">{text.slice(idx, idx + query.length)}</mark>
      {text.slice(idx + query.length)}
    </>
  )
}

// Reinos activos por defecto (el resto arranca desmarcado)
const DEFAULT_KINGDOMS = ['Animalia', 'Plantae', 'Fungi']

// Deriva { domainName: [{key,name}, ...] } a partir de los nodos/links reales del
// grafo inicial (Biota->Domain->Kingdom), en vez de hardcodear el agrupamiento.
function deriveDomainGroups(full) {
  if (!full) return []
  const byId = Object.fromEntries(full.nodes.map((n) => [n.id, n]))
  const domains = full.nodes.filter((n) => n.rank === 'domain')
  return domains.map((d) => {
    const kingdomIds = full.links.filter((l) => l.source === d.id).map((l) => l.target)
    const kids = kingdomIds.map((id) => byId[id]).filter((n) => n && n.rank === 'kingdom')
    return { id: d.id, name: d.name, kingdoms: kids }
  })
}

export default function Shui() {
  const navigate = useNavigate()
  const { dark, toggle: toggleTheme } = useTheme()

  const [graphData, setGraphData] = useState(null)
  const [focusKingdom, setFocusKingdom] = useState(null)
  const [focusNode, setFocusNode] = useState(null)

  // reinos y cuales estan activos (checkboxes izquierda)
  const [kingdoms, setKingdoms] = useState([])
  const [activeKingdoms, setActiveKingdoms] = useState(new Set())
  const [domainGroups, setDomainGroups] = useState([])

  // buscador
  const [q, setQ] = useState('')
  const [results, setResults] = useState([])
  const [searchLoading, setSearchLoading] = useState(false)
  const [searchError, setSearchError] = useState(false)
  const [activeIdx, setActiveIdx] = useState(-1)
  const searchTimer = useRef(null)
  const searchRef = useRef(null)
  const resultsWrapRef = useRef(null)

  // barra inferior (cajon deslizable)
  const [examples, setExamples] = useState([])
  const [exLoading, setExLoading] = useState(true)
  const [exError, setExError] = useState(false)
  const [drawerOpen, setDrawerOpen] = useState(false)
  const drawerCloseTimer = useRef(null)

  function openDrawer() {
    clearTimeout(drawerCloseTimer.current)
    setDrawerOpen(true)
  }
  function scheduleCloseDrawer() {
    clearTimeout(drawerCloseTimer.current)
    drawerCloseTimer.current = setTimeout(() => setDrawerOpen(false), 250)
  }

  useSetKingdom(focusKingdom)
  const initialTimer = useElapsedTimer()
  const [initialLoading, setInitialLoading] = useState(true)
  const focusTimer = useElapsedTimer()
  const [focusLoading, setFocusLoading] = useState(false)

  useEffect(() => {
    initialTimer.start()
    let pending = 2
    const settle = () => { pending -= 1; if (pending === 0) { setInitialLoading(false); initialTimer.stop() } }
    api.graph().then((g) => { setGraphData(g); setFullGraph(g); setDomainGroups(deriveDomainGroups(g)); settle() }).catch(settle)
    api.kingdoms().then((ks) => {
      setKingdoms(ks)
      const names = ks.map((k) => k.name)
      setActiveKingdoms(new Set(DEFAULT_KINGDOMS.filter((n) => names.includes(n))))
      settle()
    }).catch(settle)
    // nota: la barra de ejemplos NO se carga aqui -- el useEffect reactivo a
    // activeKingdoms (abajo) ya la dispara en cuanto setActiveKingdoms
    // resuelve con los reinos default, evitando una doble carga/parpadeo.
  }, [])

  // guardamos el grafo completo para filtrar por reino sin re-pedir
  const fullGraphRef = useRef(null)
  const setFullGraph = (g) => { fullGraphRef.current = g }

  // recalcular grafo visible al cambiar checkboxes (solo en vista Biota)
  useEffect(() => {
    if (focusNode || !fullGraphRef.current) return
    const full = fullGraphRef.current
    const keepKingdomNames = activeKingdoms
    // nodos: biota + TODOS los dominios (siempre visibles) + reinos activos
    const nodes = full.nodes.filter((n) => {
      if (n.rank === 'root' || n.rank === 'domain') return true
      return keepKingdomNames.has(n.kingdom)
    })
    const ids = new Set(nodes.map((n) => n.id))
    const links = full.links.filter((l) => ids.has(l.source) && ids.has(l.target))
    setGraphData({ ...full, nodes, links })
    // la barra de ejemplos tambien debe respetar el pot de reinos activos
    loadKingdomExamples()
  }, [activeKingdoms, focusNode])

  // Pot de ejemplos: n tiradas, cada una elige un reino al azar DENTRO de los
  // reinos activos (no 1 garantizado por reino). Vacio = usa todos los reinos.
  function loadKingdomExamples() {
    setExLoading(true); setExError(false)
    api.randomPool([...activeKingdoms], 8).then((rows) => {
      setExamples(rows || []); setExLoading(false)
    }).catch(() => { setExError(true); setExLoading(false) })
  }

  // contador de "operacion vigente": cada llamada a focusOn (sea foco o
  // reset) lo incrementa; una promesa que resuelve tarde solo aplica sus
  // cambios si su id sigue siendo el vigente. Evita que un click en un nodo
  // "reviva" el foco despues de que el usuario ya presiono Reiniciar
  // mientras la respuesta de graphFocus seguia en vuelo.
  const focusOpRef = useRef(0)

  function focusOn(node) {
    const opId = ++focusOpRef.current
    if (!node) {
      setGraphData(fullGraphRef.current)
      setFocusKingdom(null); setFocusNode(null)
      setFocusLoading(false); focusTimer.stop()
      loadKingdomExamples()
      return
    }
    if (focusLoading) return // evita carreras si se hace click varias veces mientras carga
    setFocusLoading(true)
    focusTimer.start()
    api.graphFocus(node.rank, node.key).then((g) => {
      if (focusOpRef.current !== opId) return // una operacion mas reciente (otro click o un reset) ya tomo el control
      // Fallback: si el backend no pudo determinar el reino del nuevo nodo
      // (rangos como Phylum/Class/Order/Family no tienen su propia propiedad
      // 'kingdom' poblada en Neo4j -- solo Species/Genus la tienen, y Kingdom
      // usa su propio nombre), se mantiene el reino del foco anterior en vez
      // de perderlo. Al navegar SIEMPRE bajamos dentro del mismo reino o nos
      // quedamos igual, nunca subimos a otro, asi que el valor previo sigue
      // siendo correcto.
      const nextKingdom = g.kingdom || focusKingdom
      setGraphData({ ...g, kingdom: nextKingdom })
      setFocusKingdom(nextKingdom)
      setFocusNode(node)
      setFocusLoading(false); focusTimer.stop()
      setExLoading(true); setExError(false)
      api.randomDescendants(node.rank, node.key, 9)
        .then((rows) => { if (focusOpRef.current === opId) { setExamples(rows || []); setExLoading(false) } })
        .catch(() => { if (focusOpRef.current === opId) { setExError(true); setExLoading(false) } })
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

  // Click en el checkbox de un Domain: si todos sus reinos estan activos, los
  // desactiva todos; si no, activa los que falten (selecciona el grupo completo).
  function toggleDomain(group) {
    const names = group.kingdoms.map((k) => k.name)
    setActiveKingdoms((prev) => {
      const allOn = names.every((n) => prev.has(n))
      const next = new Set(prev)
      names.forEach((n) => (allOn ? next.delete(n) : next.add(n)))
      return next
    })
  }
  // Estado del checkbox padre: 'all' | 'none' | 'partial' (indeterminate)
  function domainCheckState(group) {
    const names = group.kingdoms.map((k) => k.name)
    if (names.length === 0) return 'none'
    const onCount = names.filter((n) => activeKingdoms.has(n)).length
    if (onCount === 0) return 'none'
    if (onCount === names.length) return 'all'
    return 'partial'
  }

  function onSearch(value) {
    setQ(value)
    setActiveIdx(-1)
    clearTimeout(searchTimer.current)
    if (!value.trim()) { setResults([]); setSearchError(false); return }
    searchTimer.current = setTimeout(() => {
      setSearchLoading(true); setSearchError(false)
      api.searchClades(value.trim(), 8)
        .then((data) => {
          // aplanar { groups: { rank: [{kingdom, items}] } } a una lista plana
          // de sugerencias, manteniendo rank/kingdom para mostrar contexto.
          const flat = []
          const groups = data?.groups || {}
          for (const rank of Object.keys(groups)) {
            for (const g of groups[rank]) {
              for (const item of g.items) flat.push(item)
            }
          }
          setResults(flat)
          setSearchLoading(false)
        })
        .catch(() => { setResults([]); setSearchError(true); setSearchLoading(false) })
    }, 250)
  }

  function resultHref(r) {
    return r.rank === 'species' ? `/species/${r.key}` : `/taxon/${r.rank}/${r.key}`
  }
  function openResult(r) {
    navigate(resultHref(r))
    setResults([]); setQ(''); setActiveIdx(-1)
  }

  function onSearchKeyDown(e) {
    if (e.key === 'ArrowDown') {
      e.preventDefault()
      setActiveIdx((i) => Math.min(i + 1, results.length - 1))
      return
    }
    if (e.key === 'ArrowUp') {
      e.preventDefault()
      setActiveIdx((i) => Math.max(i - 1, -1))
      return
    }
    if (e.key === 'Escape') {
      setResults([]); setActiveIdx(-1)
      return
    }
    if (e.key === 'Enter') {
      e.preventDefault()
      const query = q.trim()
      if (!query) return
      // si hay una sugerencia resaltada con las flechas, esa manda
      if (activeIdx >= 0 && results[activeIdx]) {
        openResult(results[activeIdx])
        return
      }
      // si alguna sugerencia coincide EXACTO (case-insensitive) con lo escrito,
      // se abre directo; si no, se va a la vista de resultados de busqueda.
      const exact = results.find((r) => r.name?.toLowerCase() === query.toLowerCase())
      if (exact) { openResult(exact); return }
      navigate(`/search?q=${encodeURIComponent(query)}`)
      setResults([]); setActiveIdx(-1)
    }
  }

  function resetAll() {
    setQ(''); setResults([])
    const names = kingdoms.map((k) => k.name)
    setActiveKingdoms(new Set(DEFAULT_KINGDOMS.filter((n) => names.includes(n))))
    focusOn(null)
  }

  function selectAllKingdoms() {
    setActiveKingdoms(new Set(kingdoms.map((k) => k.name)))
  }
  function deselectAllKingdoms() {
    setActiveKingdoms(new Set())
  }
  function navigateToList() {
    if (focusNode) navigate(`/taxon/${focusNode.rank}/${focusNode.key}`)
  }

  useKeyboardShortcuts({
    navigate,
    toggleTheme,
    shui: {
      searchRef,
      resetAll,
      reloadExamples: () => focusNode
        ? api.randomDescendants(focusNode.rank, focusNode.key, 9).then(setExamples)
        : loadKingdomExamples(),
      openDrawer,
      navigateToList,
      selectAllKingdoms,
      deselectAllKingdoms,
    },
  })

  // cerrar el dropdown de sugerencias al hacer clic fuera
  useEffect(() => {
    function onClickOutside(e) {
      if (resultsWrapRef.current && !resultsWrapRef.current.contains(e.target)) {
        setResults([]); setActiveIdx(-1)
      }
    }
    document.addEventListener('mousedown', onClickOutside)
    return () => document.removeEventListener('mousedown', onClickOutside)
  }, [])

  return (
    <div className="shui-scope">
      <ShuiBackground />
      {/* luz del reino al hacer focus */}
      <div className="sh-kingdom-glow" style={{
        background: focusKingdom
          ? `radial-gradient(circle at 50% 100%, ${glowFor(focusKingdom, dark)}55 0%, transparent 65%)`
          : 'transparent',
      }} />

      <div className="sh-page">
        <div className="sh-top">
          <div className="sh-topbar">
            <LinNeoLogo className="sh-title" />
            <div className="sh-searchbar" ref={resultsWrapRef}>
              <input ref={searchRef} className="sh-input" value={q}
                onChange={(e) => onSearch(e.target.value)}
                onKeyDown={onSearchKeyDown}
                placeholder="Buscar por nombre comun o cientifico..." />
              {(q.trim().length > 0) && (
                <div className="sh-results">
                  {searchLoading && <div className="sh-result sh-result-status">Buscando...</div>}
                  {!searchLoading && searchError && <div className="sh-result sh-result-status">Error al buscar.</div>}
                  {!searchLoading && !searchError && results.length === 0 && (
                    <div className="sh-result sh-result-status">Sin resultados. Enter para ver clados que contengan "{q.trim()}".</div>
                  )}
                  {!searchLoading && !searchError && results.map((r, i) => (
                    <a key={`${r.rank}:${r.key}`} className={`sh-result ${i === activeIdx ? 'active' : ''}`}
                      href={resultHref(r)}
                      onClick={(e) => { e.preventDefault(); openResult(r) }}
                      onMouseEnter={() => setActiveIdx(i)}>
                      <span className="sh-swatch" style={{ background: swatchFor(r.kingdom, dark) }} />
                      <span className="sci">{highlightMatch(r.name, q.trim())}</span>
                      <span className="sh-result-meta">
                        {RANK_ES_SHORT[r.rank] || r.rank}{r.kingdom ? ` · ${r.kingdom}` : ''}
                      </span>
                      {r.common_names?.length > 0 && <span className="common"> - {r.common_names.slice(0, 3).join(', ')}</span>}
                    </a>
                  ))}
                </div>
              )}
            </div>
            <button className="sh-kbtn" onClick={resetAll}>Reiniciar</button>
            <ThemeToggle />
          </div>
        </div>

        {/* Cuerpo: barra izquierda + grafo + barra derecha */}
        <div className="sh-body">
          {/* Izquierda: reinos */}
          <aside className="sh-side">
            <h3>Reinos</h3>
            {domainGroups.map((g) => {
              const state = domainCheckState(g)
              return (
                <div key={g.id} className="sh-domain-group">
                  <label className="sh-check sh-check-domain">
                    <input
                      type="checkbox"
                      checked={state === 'all'}
                      ref={(el) => { if (el) el.indeterminate = state === 'partial' }}
                      onChange={() => toggleDomain(g)}
                      disabled={!!focusNode}
                    />
                    <span className="sh-domain-name">{g.name}</span>
                  </label>
                  <div className="sh-kingdom-sublist">
                    {g.kingdoms.map((k) => (
                      <label key={k.key} className="sh-check sh-check-sub">
                        <input type="checkbox" checked={activeKingdoms.has(k.name)} onChange={() => toggleKingdom(k.name)} disabled={!!focusNode} />
                        <span className="sh-swatch" style={{ background: swatchFor(k.name, dark) }} />
                        {k.name}
                      </label>
                    ))}
                  </div>
                </div>
              )
            })}
            {focusNode && <p style={{ fontSize: 10, color: 'var(--sh-text2)', marginTop: 10 }}>Reinicia el grafo para filtrar por reino.</p>}
          </aside>

          {/* Centro: grafo */}
          <div className="sh-center-col">
            <div className="sh-graph-wrap">
              {(initialLoading || focusLoading) && (
                <div className="ln-map-overlay">
                  <LoadingSpinner
                    inline={false}
                    timeText={
                      focusLoading
                        ? (focusTimer.elapsedMs != null ? formatElapsed(focusTimer.elapsedMs) : null)
                        : (initialTimer.elapsedMs != null ? formatElapsed(initialTimer.elapsedMs) : null)
                    }
                  />
                </div>
              )}
              {graphData && <ShuiGraph data={graphData} onFocus={focusOn} onOpenSpecies={(k) => navigate(`/species/${k}`)} />}
            </div>
          </div>

          {/* Derecha: geografia + vista */}
          <aside className="sh-side right">
            <h3>Geografia</h3>
            <button
              className="sh-kbtn"
              style={{ width: '100%' }}
              onClick={() => navigate('/map')}
            >
              Ver en mapa
            </button>

            <h3 style={{ marginTop: 22 }}>Vista</h3>
            <button
              className="sh-kbtn"
              style={{ width: '100%' }}
              disabled={!focusNode}
              title={focusNode ? '' : 'Selecciona un nodo del grafo para explorarlo como lista'}
              onClick={() => focusNode && navigate(`/taxon/${focusNode.rank}/${focusNode.key}`)}
            >
              Explorar como lista
            </button>
          </aside>
        </div>

        {/* Zona sensible al borde inferior: abre el cajon al pasar el mouse */}
        <div className="sh-kbar-trigger" onMouseEnter={openDrawer} onMouseLeave={scheduleCloseDrawer} />

        {/* Cajon deslizable de ejemplos */}
        <div
          className={`sh-kbar-drawer ${drawerOpen ? 'open' : ''}`}
          onMouseEnter={openDrawer}
          onMouseLeave={scheduleCloseDrawer}
        >
          <div className="sh-kbar-drawer-inner">
            <div className="sh-kbar">
              {exLoading && Array.from({ length: 9 }).map((_, i) => (
                <div key={`ph${i}`} className="sh-kbox" style={{ opacity: .5 }}><span className="label">cargando...</span></div>
              ))}
              {!exLoading && exError && (
                <div className="sh-kbox" style={{ minWidth: '100%' }}><span className="label">No se pudieron cargar ejemplos.</span></div>
              )}
              {!exLoading && !exError && examples.length === 0 && (
                <div className="sh-kbox" style={{ minWidth: '100%' }}><span className="label">Sin ejemplos</span></div>
              )}
              {!exLoading && !exError && examples.map((ex, i) => (
                <a key={ex.species_key || i} className="sh-kbox"
                  style={{ '--kbox-glow': glowFor(ex.kingdom, dark) }}
                  href={`/species/${ex.species_key}`}
                  onClick={(e) => { e.preventDefault(); navigate(`/species/${ex.species_key}`) }}
                  title={ex.name}>
                  {ex.image && <img src={ex.image} alt={ex.name} loading="lazy" />}
                  <span className="label">{ex.name}</span>
                </a>
              ))}
            </div>
            <div className="sh-kbar-actions">
              <button className="sh-kbtn" onClick={() => focusNode ? api.randomDescendants(focusNode.rank, focusNode.key, 9).then(setExamples) : loadKingdomExamples()}>Re-tirar ejemplos</button>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}