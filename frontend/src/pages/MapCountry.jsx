import { useEffect, useState } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import { api } from '../api/client'
import { useTheme } from '../theme/ThemeContext'
import { kingdomStyleVars } from '../theme/kingdomColor'
import { useSetKingdom } from '../theme/KingdomContext'
import { useKeyboardShortcuts } from '../hooks/useKeyboardShortcuts'
import { isoToName } from '../components/DistributionMap'
import { ContentFlags } from './TaxonNode'
import '../theme/bookworm.css'

const ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('')

function alphaKeyOf(item) {
  const words = (item.name || '').trim().split(/\s+/)
  const word = words[1] || words[0] || ''
  return word.charAt(0).toUpperCase()
}

export default function MapCountry() {
  const { code } = useParams()
  const navigate = useNavigate()
  const { dark, toggle: toggleTheme } = useTheme()
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  const [onlyImages, setOnlyImages] = useState(false)
  const [onlyDescriptions, setOnlyDescriptions] = useState(false)
  const [onlyCommonNames, setOnlyCommonNames] = useState(false)
  const [onlySounds, setOnlySounds] = useState(false)
  const [activeLetter, setActiveLetter] = useState(null)
  const [activeKingdom, setActiveKingdom] = useState(null)

  useSetKingdom(activeKingdom)

  useEffect(() => {
    let alive = true
    setLoading(true); setError(null); setData(null)
    setOnlyImages(false); setOnlyDescriptions(false); setOnlySounds(false); setOnlyCommonNames(false)
    setActiveLetter(null); setActiveKingdom(null)
    api.mapCountry(code)
      .then((d) => { if (alive) { setData(d); setLoading(false) } })
      .catch((e) => { if (alive) { setError(e.message); setLoading(false) } })
    return () => { alive = false }
  }, [code])

  function openSpecies(s) { navigate(`/species/${s.key}`) }

  const children = data?.children || []
  function hasImage(c) { return c.flags?.images > 0 }
  function hasDescription(c) { return c.flags?.descriptions > 0 }
  function hasSound(c) { return c.flags?.sounds > 0 }
  function hasCommonName(c) { return c.common_names?.length > 0 }
  const availableLetters = new Set(children.map(alphaKeyOf).filter(Boolean))

  const filteredChildren = children.filter((c) => {
    if (onlyImages && !hasImage(c)) return false
    if (onlyDescriptions && !hasDescription(c)) return false
    if (onlySounds && !hasSound(c)) return false
    if (onlyCommonNames && !hasCommonName(c)) return false
    if (activeLetter && alphaKeyOf(c) !== activeLetter) return false
    if (activeKingdom && c.kingdom !== activeKingdom) return false
    return true
  })

  useKeyboardShortcuts({ navigate, toggleTheme, taxon: { filteredChildren, openChild: openSpecies } })

  function clearAllFilters() {
    setOnlyImages(false); setOnlyDescriptions(false); setOnlySounds(false); setOnlyCommonNames(false)
    setActiveLetter(null); setActiveKingdom(null)
  }
  const anyFilterActive = onlyImages || onlyDescriptions || onlySounds || onlyCommonNames || activeLetter || activeKingdom

  return (
    <div className="bookworm-scope" style={kingdomStyleVars(activeKingdom, dark)}>
      <div className="bw-page">
        <button className="bw-btn" onClick={() => navigate(-1)}>{'\u2039'} volver</button>

        {loading && <p className="bw-muted" style={{ marginTop: 20 }}>Cargando...</p>}
        {error && <p style={{ marginTop: 20, color: 'var(--bw-danger)' }}>{error}</p>}

        {data && (
          <>
            <div className="bw-header">
              <div className="bw-header-title">
                <div className="bw-rank">Pais</div>
                <h1>{isoToName(data.key) || data.name}</h1>
                <p className="bw-muted">
                  {data.species_count?.toLocaleString() || 0} especie(s) registradas
                  {data.continent ? ` · ${data.continent}` : ''}
                </p>
                {data.by_kingdom?.length > 0 && (
                  <div className="bw-stats">
                    {data.by_kingdom.map((k) => (
                      <span key={k.kingdom} className="bw-stat">
                        <span className="bw-stat-val">{k.n}</span>
                        <span className="bw-stat-lbl">{k.kingdom}</span>
                      </span>
                    ))}
                  </div>
                )}
              </div>
            </div>

            {data.by_kingdom?.length > 0 && (
              <div className="bw-filters" style={{ marginTop: 4 }}>
                {data.by_kingdom.map((k) => (
                  <button
                    key={k.kingdom}
                    className={`bw-btn bw-filter-tag ${activeKingdom === k.kingdom ? 'active' : ''}`}
                    onClick={() => setActiveKingdom((prev) => (prev === k.kingdom ? null : k.kingdom))}
                  >{k.kingdom}</button>
                ))}
              </div>
            )}

            <div className="bw-list-section" style={{ marginTop: 20 }}>
              <h2>Especies</h2>

              <div className="bw-filters">
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
                  <button className="bw-btn" onClick={clearAllFilters} style={{ marginLeft: 'auto' }}>Limpiar filtros</button>
                )}
              </div>

              <div className="bw-alpha">
                <button className={`bw-alpha-letter ${!activeLetter ? 'active' : ''}`} onClick={() => setActiveLetter(null)}>Todas</button>
                {ALPHABET.map((letter) => (
                  <button
                    key={letter}
                    className={`bw-alpha-letter ${activeLetter === letter ? 'active' : ''}`}
                    disabled={!availableLetters.has(letter)}
                    onClick={() => setActiveLetter((prev) => (prev === letter ? null : letter))}
                  >{letter}</button>
                ))}
              </div>

              <div className="bw-children">
                {filteredChildren.map((c, i) => (
                  <a key={c.key || i} className="bw-child" href={`/species/${c.key}`}
                    onClick={(e) => { e.preventDefault(); openSpecies(c) }}>
                    <div className="bw-child-info">
                      <div className="bw-rank">{c.kingdom || 'Especie'}</div>
                      <div style={{ fontStyle: 'italic' }}>{c.name}</div>
                      <ContentFlags flags={c.flags} isSpecies commonNames={c.common_names} conservation={c.conservation} />
                    </div>
                    {c.image && (
                      <div className="bw-child-thumb">
                        <img src={c.image} alt={c.name} loading="lazy" />
                      </div>
                    )}
                  </a>
                ))}
              </div>
              {children.length === 0 && <p className="bw-muted">Sin especies registradas en este pais.</p>}
              {children.length > 0 && filteredChildren.length === 0 && (
                <p className="bw-muted">Ningun resultado con los filtros activos.</p>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  )
}