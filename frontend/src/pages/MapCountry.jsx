import { useEffect, useState } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { api } from '../api/client'
import { useTheme } from '../theme/ThemeContext'
import { useSetKingdom } from '../theme/KingdomContext'
import { kingdomInk } from '../theme/kingdomColor'
import { useKeyboardShortcuts } from '../hooks/useKeyboardShortcuts'
import { useElapsedTimer, formatElapsed } from '../hooks/useElapsedTimer'
import { isoToName, CONTINENT_ES } from '../geo/countries'
import SiteHeader from '../components/SiteHeader'
import MarginMap from '../components/MarginMap'
import Brace from '../components/Brace'
import TaxonEntry from '../components/TaxonEntry'
import LoadingSpinner from '../components/LoadingSpinner'
import '../theme/taxon.css'

// Lámina de un país: sus especies agrupadas por reino bajo la llave, filtro por
// reino en el servidor (la página trae 500 en orden alfabético) y al margen el
// país en el mapa con su desglose por reino.

const fmt = (n) => (n ?? 0).toLocaleString('es-MX')
const ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('')
const CONTENT = [['images', 'imagen'], ['descriptions', 'texto'], ['sounds', 'sonido'], ['common_names', 'nombre común']]
const hasContent = (s, k) => (k === 'common_names' ? s.common_names?.length > 0 : s.flags?.[k] > 0)
const initialOf = (s) => ((s.name || '').split(/\s+/)[1] || s.name || '').charAt(0).toUpperCase()

export default function MapCountry() {
  const { code } = useParams()
  const iso = code.toUpperCase()
  const navigate = useNavigate()
  const { dark, toggle: toggleTheme } = useTheme()
  const [data, setData] = useState(null)
  const [species, setSpecies] = useState([])
  const [kingdom, setKingdom] = useState(null)
  const [loading, setLoading] = useState(true)
  const [loadingMore, setLoadingMore] = useState(false)
  const [error, setError] = useState(null)
  const [only, setOnly] = useState({})
  const [letter, setLetter] = useState(null)
  const timer = useElapsedTimer()
  useSetKingdom(kingdom)

  useEffect(() => { setKingdom(null) }, [iso])
  useEffect(() => {
    let alive = true
    setLoading(true); setError(null); setOnly({}); setLetter(null)
    timer.start()
    api.mapCountry(iso, { kingdom })
      .then((d) => { if (alive) { setData(d); setSpecies(d.children) } })
      .catch((e) => { if (alive) setError(e.message) })
      .finally(() => { if (alive) { setLoading(false); timer.stop() } })
    return () => { alive = false }
  }, [iso, kingdom])

  const loadMore = () => {
    setLoadingMore(true)
    api.mapCountry(iso, { kingdom, offset: species.length })
      .then((d) => setSpecies((p) => [...p, ...d.children]))
      .catch((e) => setError(e.message))
      .finally(() => setLoadingMore(false))
  }

  const filtered = species.filter((s) => CONTENT.every(([k]) => !only[k] || hasContent(s, k)) && (!letter || initialOf(s) === letter))
  useKeyboardShortcuts({ navigate, toggleTheme, taxon: { filteredChildren: filtered, openChild: (s) => navigate(`/species/${s.key}`) } })

  if (!data) {
    return (
      <div className="folio">
        <SiteHeader />
        {loading && <LoadingSpinner inline={false} timeText={formatElapsed(timer.elapsedMs)} />}
        {error && <p className="taxon-error" role="alert">No se pudo cargar el país: {error}</p>}
      </div>
    )
  }

  const name = isoToName(iso) || data.name
  const total = data.children_total ?? data.species_count
  const partial = species.length < total
  const anyFilter = Object.values(only).some(Boolean) || letter
  const letters = new Set(species.map(initialOf))
  const groups = (kingdom ? [{ kingdom, n: total }] : data.by_kingdom)
    .map((g) => ({ ...g, items: filtered.filter((s) => s.kingdom === g.kingdom) }))
    .filter((g) => g.items.length > 0)

  return (
    <div className="folio">
      <SiteHeader />

      <header className="taxon-head">
        <p className="taxon-rank">País{data.continent ? ` · ${CONTINENT_ES[data.continent] || data.continent}` : ''}</p>
        <h1>{name}</h1>
        <nav aria-label="Geografía">
          <ol className="lineage">
            <li><Link to="/map">El mundo</Link></li>
            {data.continent && <li><Link to={`/map/${encodeURIComponent(data.continent)}`}>{CONTINENT_ES[data.continent] || data.continent}</Link></li>}
            <li aria-current="page"><span className="lineage-here">{name}</span> · <span className="taxon-id">{iso}</span></li>
          </ol>
        </nav>
        <p className="taxon-summary">{fmt(data.species_count)} especies registradas en {name}.</p>
      </header>

      <div className="taxon-body">
        <section className="taxon-main" aria-labelledby="especies">
          <h2 id="especies" className="folio-h2">Especies{kingdom ? ` de ${kingdom}` : ''}</h2>

          <div className="taxon-tools">
            <div role="group" aria-label="Reino" className="tool-group">
              <button type="button" className="ctl" aria-pressed={!kingdom} onClick={() => setKingdom(null)}>Todos los reinos</button>
              {data.by_kingdom.map((k) => (
                <button key={k.kingdom} type="button" className="ctl" aria-pressed={kingdom === k.kingdom}
                  onClick={() => setKingdom((p) => (p === k.kingdom ? null : k.kingdom))}>
                  <span className="dot" style={{ background: kingdomInk(k.kingdom, dark) }} aria-hidden="true" /> {k.kingdom}
                </button>
              ))}
            </div>
            <div role="group" aria-labelledby="solo-con" className="tool-group">
              <span id="solo-con" className="tool-label">Solo con</span>
              {CONTENT.map(([k, label]) => (
                <label key={k}>
                  <input type="checkbox" checked={!!only[k]} onChange={(e) => setOnly((o) => ({ ...o, [k]: e.target.checked }))} />
                  {label}
                </label>
              ))}
              {anyFilter && <button type="button" className="ctl" onClick={() => { setOnly({}); setLetter(null) }}>Quitar filtros</button>}
            </div>
          </div>

          {species.length > 30 && (
            <nav className="alpha" aria-label="Filtrar por inicial del epíteto">
              {ALPHABET.map((l) => (
                <button key={l} type="button" aria-pressed={letter === l} disabled={!letters.has(l)}
                  onClick={() => setLetter((p) => (p === l ? null : l))}>{l}</button>
              ))}
            </nav>
          )}

          {(anyFilter || partial) && (
            <p className="taxon-count">
              {anyFilter ? `${fmt(filtered.length)} con estos filtros, de ` : 'Van '}
              {fmt(species.length)} cargadas{partial && ` de ${fmt(total)}`}.
            </p>
          )}
          {loading && <p className="taxon-count">Cargando…</p>}

          {groups.map((g) => (
            <div className="rank-group" key={g.kingdom} data-kingdom={g.kingdom}>
              <div className="rank-label">
                <h3>{g.kingdom}</h3>
                <span className="n">{fmt(g.n)}</span>
              </div>
              <Brace><ul className="entries">{g.items.map((s) => <TaxonEntry key={s.key} child={s} />)}</ul></Brace>
            </div>
          ))}
          {!loading && species.length === 0 && <p className="taxon-count">Sin especies registradas.</p>}
          {species.length > 0 && groups.length === 0 && <p className="taxon-count">Ninguna con estos filtros.</p>}
          {partial && !loading && (
            <button type="button" className="ctl load-more" onClick={loadMore} disabled={loadingMore}>
              {loadingMore ? 'Cargando…' : `Cargar ${fmt(Math.min(500, total - species.length))} más`}
            </button>
          )}
        </section>

        <aside className="taxon-margin" aria-label="Al margen">
          <h2 className="folio-h2">En el mapa</h2>
          <MarginMap countries={[iso]} />
          <h2 className="folio-h2 margin-gap">Por reino</h2>
          <dl className="margin-stats">
            {data.by_kingdom.map((k) => (
              <div key={k.kingdom}>
                <dt><span className="dot" style={{ background: kingdomInk(k.kingdom, dark) }} aria-hidden="true" /> {k.kingdom}</dt>
                <dd>{fmt(k.n)} <span>{data.species_count ? `${Math.round((100 * k.n) / data.species_count)} %` : ''}</span></dd>
              </div>
            ))}
          </dl>
        </aside>
      </div>
    </div>
  )
}
