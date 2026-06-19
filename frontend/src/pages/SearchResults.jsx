import { useEffect, useState, useRef } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'
import { api } from '../api/client'
import { useTheme } from '../theme/ThemeContext'
import { kingdomStyleVars } from '../theme/kingdomColor'
import { useSetKingdom } from '../theme/KingdomContext'
import { useKeyboardShortcuts } from '../hooks/useKeyboardShortcuts'
import { ContentFlags } from './TaxonNode'
import '../theme/bookworm.css'

const RANK_ES = {
  domain: 'Dominio', kingdom: 'Reino', phylum: 'Filo', class: 'Clase', order: 'Orden',
  family: 'Familia', genus: 'Genero', species: 'Especie',
}
// orden de aparicion de los grupos de rango en la pagina
const RANK_ORDER = ['domain', 'kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species']

function cladeHref(item) {
  return item.rank === 'species' ? `/species/${item.key}` : `/taxon/${item.rank}/${item.key}`
}

export default function SearchResults() {
  const [params] = useSearchParams()
  const q = params.get('q') || ''
  const navigate = useNavigate()
  const { dark, toggle: toggleTheme } = useTheme()

  // resultados por rango: cada rango llega de forma independiente (8 llamadas
  // paralelas, no se espera al mas lento para pintar los que ya respondieron).
  const [groupsByRank, setGroupsByRank] = useState({}) // { rank: [{kingdom, items}] }
  const [rankStatus, setRankStatus] = useState({}) // { rank: 'loading'|'done'|'error' }
  const [searchInput, setSearchInput] = useState(q)

  // filtros de contenido, aplicados sobre lo que ya cargo (no esperan a que
  // termine toda la busqueda)
  const [onlyImages, setOnlyImages] = useState(false)
  const [onlyDescriptions, setOnlyDescriptions] = useState(false)
  const [onlyCommonNames, setOnlyCommonNames] = useState(false)
  const [onlySounds, setOnlySounds] = useState(false)
  // 'contiene' vs 'empieza con' -- cambia el modo de busqueda en el backend,
  // asi que alternar este filtro relanza las 8 llamadas (no es un filtro en
  // memoria como los de arriba).
  const [searchMode, setSearchMode] = useState('contains')

  // cronometro: arranca al disparar las 8 llamadas, se detiene cuando la
  // ultima (la mas lenta) responde.
  const [elapsedMs, setElapsedMs] = useState(null)
  const startRef = useRef(null)
  const tickRef = useRef(null)

  useSetKingdom(null) // vista neutral, no pertenece a un reino especifico
  useKeyboardShortcuts({ navigate, toggleTheme })

  useEffect(() => {
    setSearchInput(q)
    setOnlyImages(false); setOnlyDescriptions(false); setOnlySounds(false); setOnlyCommonNames(false)
    if (!q.trim()) {
      setGroupsByRank({}); setRankStatus({}); setElapsedMs(null)
      return
    }
    let alive = true
    const term = q.trim()
    const initialStatus = {}
    RANK_ORDER.forEach((r) => { initialStatus[r] = 'loading' })
    setGroupsByRank({}); setRankStatus(initialStatus); setElapsedMs(null)

    startRef.current = performance.now()
    clearInterval(tickRef.current)
    tickRef.current = setInterval(() => {
      if (alive) setElapsedMs(performance.now() - startRef.current)
    }, 47) // refresco visual del cronometro, no afecta el tiempo real medido

    let pending = RANK_ORDER.length
    function settle() {
      pending -= 1
      if (pending === 0 && alive) {
        clearInterval(tickRef.current)
        setElapsedMs(performance.now() - startRef.current)
      }
    }

    // 8 llamadas paralelas e independientes: cada una pinta su rango en
    // cuanto responde, sin esperar a las demas.
    RANK_ORDER.forEach((rank) => {
      api.searchCladesByRank(rank, term, 100, searchMode)
        .then((data) => {
          if (!alive) return
          setGroupsByRank((prev) => ({ ...prev, [rank]: data?.groups || [] }))
          setRankStatus((prev) => ({ ...prev, [rank]: 'done' }))
          settle()
        })
        .catch(() => {
          if (!alive) return
          setRankStatus((prev) => ({ ...prev, [rank]: 'error' }))
          settle()
        })
    })

    return () => { alive = false; clearInterval(tickRef.current) }
  }, [q, searchMode])

  function submitSearch(e) {
    e.preventDefault()
    if (searchInput.trim()) navigate(`/search?q=${encodeURIComponent(searchInput.trim())}`)
  }

  function hasImage(item) { return item.flags?.images > 0 }
  function hasDescription(item) { return item.flags?.descriptions > 0 }
  function hasSound(item) { return item.flags?.sounds > 0 }
  function hasCommonName(item) { return item.common_names?.length > 0 }
  function passesFilters(item) {
    if (onlyImages && !hasImage(item)) return false
    if (onlyDescriptions && !hasDescription(item)) return false
    if (onlySounds && !hasSound(item)) return false
    if (onlyCommonNames && !hasCommonName(item)) return false
    return true
  }

  const anyLoading = Object.values(rankStatus).some((s) => s === 'loading')
  const ranksWithData = RANK_ORDER.filter((r) => groupsByRank[r]?.length)

  // total visible respetando filtros (recalculado en cada render, suficientemente
  // ligero ya que opera sobre lo que ya esta en memoria, no dispara red)
  const totalVisible = ranksWithData.reduce((sum, rank) =>
    sum + groupsByRank[rank].reduce((s, g) => s + g.items.filter(passesFilters).length, 0), 0)
  const totalRaw = ranksWithData.reduce((sum, rank) =>
    sum + groupsByRank[rank].reduce((s, g) => s + g.items.length, 0), 0)
  const anyFilterActive = onlyImages || onlyDescriptions || onlySounds || onlyCommonNames

  function formatElapsed(ms) {
    if (ms == null) return null
    const s = Math.floor(ms / 1000)
    const rest = Math.round(ms % 1000)
    return `${s}.${String(rest).padStart(3, '0')}s`
  }

  return (
    <div className="bookworm-scope" style={kingdomStyleVars(null, dark)}>
      <div className="bw-page">
        <button className="bw-btn" onClick={() => navigate(-1)}>{'\u2039'} volver</button>

        <div className="bw-header sr-header">
          <div className="bw-header-title">
            <div className="bw-rank">Busqueda</div>
            <h1>"{q}"</h1>
            <p className="bw-muted">
              {anyFilterActive ? `${totalVisible} de ${totalRaw}` : totalRaw} resultado(s)
              {anyLoading && ' (buscando...)'}
              {elapsedMs != null && <span className="sr-timer"> · {formatElapsed(elapsedMs)}</span>}
            </p>
          </div>

          {/* Filtros, al lado del contador -- se aplican sobre lo ya cargado,
              sin esperar a que termine toda la busqueda */}
          <div className="bw-filters sr-filters">
            <div className="sr-mode-toggle">
              <button
                className={`sr-mode-btn ${searchMode === 'contains' ? 'active' : ''}`}
                onClick={() => setSearchMode('contains')}
                title="El nombre contiene la cadena en cualquier posicion"
              >Contiene</button>
              <button
                className={`sr-mode-btn ${searchMode === 'starts' ? 'active' : ''}`}
                onClick={() => setSearchMode('starts')}
                title="El nombre empieza con la cadena (en especies, el epiteto)"
              >Empieza con</button>
            </div>
            <label className="bw-filter-chk">
              <input type="checkbox" checked={onlyImages} onChange={(e) => setOnlyImages(e.target.checked)} />
              Con imagen
            </label>
            <label className="bw-filter-chk">
              <input type="checkbox" checked={onlyDescriptions} onChange={(e) => setOnlyDescriptions(e.target.checked)} />
              Con descripcion
            </label>
            <label className="bw-filter-chk">
              <input type="checkbox" checked={onlySounds} onChange={(e) => setOnlySounds(e.target.checked)} />
              Con sonido
            </label>
            <label className="bw-filter-chk">
              <input type="checkbox" checked={onlyCommonNames} onChange={(e) => setOnlyCommonNames(e.target.checked)} />
              Con nombre comun
            </label>
            {anyFilterActive && (
              <button className="bw-btn" onClick={() => { setOnlyImages(false); setOnlyDescriptions(false); setOnlySounds(false); setOnlyCommonNames(false) }}>
                Limpiar filtros
              </button>
            )}
          </div>
        </div>

        {/* Barra de busqueda, igual que en Shui */}
        <form className="sr-searchbar" onSubmit={submitSearch}>
          <input
            className="sr-input"
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
            placeholder="Buscar por nombre comun o cientifico..."
            autoFocus
          />
          <button className="bw-btn" type="submit">Buscar</button>
        </form>

        {!anyLoading && totalRaw === 0 && (
          <p className="bw-muted" style={{ marginTop: 20 }}>Ningun clado o especie contiene "{q}".</p>
        )}

        {RANK_ORDER.map((rank) => {
          const status = rankStatus[rank]
          if (!status) return null
          const groups = groupsByRank[rank] || []
          const visibleGroups = groups
            .map((g) => ({ ...g, items: g.items.filter(passesFilters) }))
            .filter((g) => g.items.length > 0)

          if (status === 'loading') {
            return (
              <div key={rank} className="bw-list-section sr-rank-loading" style={{ marginTop: 24 }}>
                <h2>{RANK_ES[rank]}s</h2>
                <p className="bw-muted">Buscando...</p>
              </div>
            )
          }
          if (status === 'error') {
            return (
              <div key={rank} className="bw-list-section" style={{ marginTop: 24 }}>
                <h2>{RANK_ES[rank]}s</h2>
                <p style={{ color: 'var(--bw-danger)' }}>Error al buscar en este rango.</p>
              </div>
            )
          }
          if (visibleGroups.length === 0) return null

          return (
            <div key={rank} className="bw-list-section" style={{ marginTop: 24 }}>
              <h2>{RANK_ES[rank]}s</h2>
              {visibleGroups.map((g, gi) => (
                <div key={gi} style={{ marginBottom: 14 }}>
                  {g.kingdom && rank !== 'kingdom' && (
                    <div className="bw-muted" style={{ marginBottom: 6, fontSize: 11, textTransform: 'uppercase', letterSpacing: '.06em' }}>
                      {g.kingdom}
                    </div>
                  )}
                  <div className="bw-children sr-grid">
                    {g.items.map((item, i) => (
                      <a key={i} className="bw-child sr-card"
                        href={cladeHref(item)}
                        onClick={(e) => { e.preventDefault(); navigate(cladeHref(item)) }}>
                        <div className="bw-child-info">
                          <div className="bw-rank">{RANK_ES[item.rank] || item.rank}</div>
                          <div style={{ fontStyle: item.rank === 'species' ? 'italic' : 'normal' }}>{item.name}</div>
                          <ContentFlags
                            flags={item.flags}
                            isSpecies={item.rank === 'species'}
                            commonNames={item.common_names}
                          />
                        </div>
                      </a>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          )
        })}
      </div>
    </div>
  )
}