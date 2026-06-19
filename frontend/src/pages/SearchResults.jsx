import { useEffect, useState } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'
import { api } from '../api/client'
import { useTheme } from '../theme/ThemeContext'
import { kingdomStyleVars } from '../theme/kingdomColor'
import { useSetKingdom } from '../theme/KingdomContext'
import { useKeyboardShortcuts } from '../hooks/useKeyboardShortcuts'
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
  const [groups, setGroups] = useState(null) // { rank: [{kingdom, items}] }
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [searchInput, setSearchInput] = useState(q)

  useSetKingdom(null) // vista neutral, no pertenece a un reino especifico

  useEffect(() => {
    let alive = true
    setLoading(true); setError(null); setGroups(null)
    setSearchInput(q)
    if (!q.trim()) { setLoading(false); setGroups({}); return }
    api.searchClades(q.trim(), 100)
      .then((data) => { if (alive) { setGroups(data?.groups || {}); setLoading(false) } })
      .catch((e) => { if (alive) { setError(e.message); setLoading(false) } })
    return () => { alive = false }
  }, [q])

  useKeyboardShortcuts({ navigate, toggleTheme })

  function submitSearch(e) {
    e.preventDefault()
    if (searchInput.trim()) navigate(`/search?q=${encodeURIComponent(searchInput.trim())}`)
  }

  const totalResults = groups
    ? Object.values(groups).reduce((sum, g) => sum + g.reduce((s, x) => s + x.items.length, 0), 0)
    : 0

  return (
    <div className="bookworm-scope" style={kingdomStyleVars(null, dark)}>
      <div className="bw-page">
        <button className="bw-btn" onClick={() => navigate(-1)}>{'\u2039'} volver</button>

        <div className="bw-header">
          <div className="bw-header-title">
            <div className="bw-rank">Busqueda</div>
            <h1>"{q}"</h1>
            {!loading && !error && <p className="bw-muted">{totalResults} resultado(s) en total.</p>}
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

        {loading && <p className="bw-muted" style={{ marginTop: 20 }}>Buscando...</p>}
        {error && <p style={{ marginTop: 20, color: 'var(--bw-danger)' }}>{error}</p>}

        {!loading && !error && totalResults === 0 && (
          <p className="bw-muted" style={{ marginTop: 20 }}>Ningun clado o especie contiene "{q}".</p>
        )}

        {!loading && !error && RANK_ORDER.filter((r) => groups[r]?.length).map((rank) => (
          <div key={rank} className="bw-list-section" style={{ marginTop: 24 }}>
            <h2>{RANK_ES[rank]}s</h2>
            {groups[rank].map((g, gi) => (
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
                        {item.common_names?.length > 0 && (
                          <div className="bw-muted" style={{ fontSize: 11, marginTop: 2 }}>
                            {item.common_names.slice(0, 2).join(', ')}
                          </div>
                        )}
                      </div>
                    </a>
                  ))}
                </div>
              </div>
            ))}
          </div>
        ))}
      </div>
    </div>
  )
}