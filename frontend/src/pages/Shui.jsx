import { useEffect, useState, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import { api } from '../api/client'
import ShuiGraph from '../components/ShuiGraph'
import ShuiBackground from '../backgrounds/ShuiBackground'
import { useTheme } from '../theme/ThemeContext'
import { useSetKingdom } from '../theme/KingdomContext'
import { KINGDOM_HUE } from '../theme/kingdomColor'
import ThemeToggle from '../components/ThemeToggle'
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
  const { dark } = useTheme()

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
  const searchTimer = useRef(null)

  // filtros geograficos (barra derecha)
  const [continents, setContinents] = useState([])
  const [countries, setCountries] = useState([])
  const [continent, setContinent] = useState('')
  const [country, setCountry] = useState('')

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

  useEffect(() => {
    api.graph().then((g) => { setGraphData(g); setFullGraph(g); setDomainGroups(deriveDomainGroups(g)) }).catch(() => {})
    api.continents().then(setContinents).catch(() => {})
    api.kingdoms().then((ks) => {
      setKingdoms(ks)
      const names = ks.map((k) => k.name)
      setActiveKingdoms(new Set(DEFAULT_KINGDOMS.filter((n) => names.includes(n))))
    }).catch(() => {})
    loadKingdomExamples()
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
  }, [activeKingdoms, focusNode])

  function loadKingdomExamples() {
    setExLoading(true); setExError(false)
    api.randomKingdoms().then((rows) => {
      const ex = rows.map((r) => r.examples?.[0]).filter(Boolean)
      setExamples(ex); setExLoading(false)
    }).catch(() => { setExError(true); setExLoading(false) })
  }

  function focusOn(node) {
    if (!node) {
      setGraphData(fullGraphRef.current)
      setFocusKingdom(null); setFocusNode(null)
      loadKingdomExamples()
      return
    }
    api.graphFocus(node.rank, node.key).then((g) => {
      setGraphData(g)
      setFocusKingdom(g.kingdom || null)
      setFocusNode(node)
      setExLoading(true); setExError(false)
      api.randomDescendants(node.rank, node.key, 9)
        .then((rows) => { setExamples(rows || []); setExLoading(false) })
        .catch(() => { setExError(true); setExLoading(false) })
    }).catch(() => {})
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
    clearTimeout(searchTimer.current)
    if (!value.trim()) { setResults([]); return }
    searchTimer.current = setTimeout(() => {
      api.search(value.trim(), 15).then((rows) => setResults(rows || [])).catch(() => setResults([]))
    }, 250)
  }

  function onContinent(value) {
    setContinent(value); setCountry(''); setCountries([])
    if (value) api.countries(value).then(setCountries).catch(() => setCountries([]))
  }
  function applyCountryFilter() {
    if (!country) return
    api.filter({ country }).then((res) => setResults(res.results || [])).catch(() => setResults([]))
  }

  function resetAll() {
    setQ(''); setResults([]); setContinent(''); setCountry(''); setCountries([])
    const names = kingdoms.map((k) => k.name)
    setActiveKingdoms(new Set(DEFAULT_KINGDOMS.filter((n) => names.includes(n))))
    focusOn(null)
  }

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
            <div className="sh-title">LinNeo<span>_</span></div>
            <div className="sh-searchbar">
              <input className="sh-input" value={q} onChange={(e) => onSearch(e.target.value)}
                placeholder="Buscar por nombre comun o cientifico..." />
            </div>
            <button className="sh-kbtn" onClick={resetAll}>Reiniciar</button>
            <ThemeToggle />
          </div>
          {results.length > 0 && (
            <div className="sh-results">
              {results.map((r) => (
                <div key={r.species_key} className="sh-result" onClick={() => navigate(`/species/${r.species_key}`)}>
                  <span className="sci">{r.canonical_name || r.scientific_name}</span>
                  {r.common_names?.length > 0 && <span className="common"> - {r.common_names.slice(0, 3).join(', ')}</span>}
                </div>
              ))}
            </div>
          )}
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
              {graphData && <ShuiGraph data={graphData} onFocus={focusOn} onOpenSpecies={(k) => navigate(`/species/${k}`)} />}
            </div>
          </div>

          {/* Derecha: geografia */}
          <aside className="sh-side right">
            <h3>Geografia</h3>
            <label className="sh-fieldlabel">Continente</label>
            <select className="sh-select" style={{ width: '100%' }} value={continent} onChange={(e) => onContinent(e.target.value)}>
              <option value="">Todos</option>
              {continents.map((c) => <option key={c} value={c}>{c}</option>)}
            </select>
            <label className="sh-fieldlabel">Pais</label>
            <select className="sh-select" style={{ width: '100%' }} value={country} onChange={(e) => setCountry(e.target.value)} disabled={!continent}>
              <option value="">Todos</option>
              {countries.map((c) => <option key={c.key} value={c.key}>{c.name}</option>)}
            </select>
            <button className="sh-kbtn" style={{ width: '100%', marginTop: 10 }} onClick={applyCountryFilter} disabled={!country}>Filtrar especies</button>

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
                <div key={ex.species_key || i} className="sh-kbox"
                  style={{ '--kbox-glow': glowFor(ex.kingdom, dark) }}
                  onClick={() => navigate(`/species/${ex.species_key}`)} title={ex.name}>
                  {ex.image && <img src={ex.image} alt={ex.name} loading="lazy" />}
                  <span className="label">{ex.name}</span>
                </div>
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