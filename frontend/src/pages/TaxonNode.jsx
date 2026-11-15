import { useEffect, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { api, IUCN } from '../api/client'
import { useTheme } from '../theme/ThemeContext'
import { useSetKingdom } from '../theme/KingdomContext'
import { isoToName, ISO_CONTINENT } from '../components/DistributionMap'
import SiteHeader from '../components/SiteHeader'
import Brace from '../components/Brace'
import MarginMap from '../components/MarginMap'
import LoadingSpinner from '../components/LoadingSpinner'
import { useKeyboardShortcuts } from '../hooks/useKeyboardShortcuts'
import { useElapsedTimer, formatElapsed } from '../hooks/useElapsedTimer'
import '../theme/taxon.css'

// Lámina de un taxón superior: cabeza con linaje, hijos agrupados por rango bajo
// una llave, y al margen el contenido del subárbol y su distribución.

const RANKS = ['biota', 'domain', 'kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species']
const RANK_ES = {
  biota: ['Biota', 'Biota'], domain: ['Dominio', 'Dominios'], kingdom: ['Reino', 'Reinos'],
  phylum: ['Filo', 'Filos'], class: ['Clase', 'Clases'], order: ['Orden', 'Órdenes'],
  family: ['Familia', 'Familias'], genus: ['Género', 'Géneros'], species: ['Especie', 'Especies'],
}
const rankName = (rank, n = 1) => RANK_ES[rank]?.[n === 1 ? 0 : 1] ?? rank
const fmt = (n) => (n ?? 0).toLocaleString('es-MX')
const pct = (n, total) => (total ? `${Math.round((100 * n) / total)} %` : '')
const listEs = (items) => new Intl.ListFormat('es', { type: 'conjunction' }).format(items)
const hrefOf = (t) => (t.rank === 'species' ? `/species/${t.key}` : `/taxon/${t.rank}/${t.key}`)
const CONTINENT_ES = {
  Africa: 'África', Antarctica: 'Antártida', Asia: 'Asia', Europe: 'Europa',
  'North America': 'América del Norte', Oceania: 'Oceanía', 'South America': 'América del Sur',
}
const THREATENED = new Set(['VU', 'EN', 'CR', 'EW', 'EX'])
const ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('')
const CONTENT = [
  ['images', 'imagen'], ['descriptions', 'texto'], ['sounds', 'sonido'], ['common_names', 'nombre común'],
]

// Inicial por la que se filtra: en especies, la del epíteto (la del género es la misma para todas).
function initialOf(child) {
  const words = (child.name || '').trim().split(/\s+/)
  const word = child.rank === 'species' ? (words[1] || words[0] || '') : (words[0] || '')
  return word.charAt(0).toUpperCase()
}

function hasContent(child, kind) {
  if (kind === 'common_names' && child.rank === 'species') return child.common_names?.length > 0
  return child.flags?.[kind] > 0
}

function Entry({ child }) {
  const sp = child.rank === 'species'
  return (
    <li data-rank={child.rank}>
      <Link className="taxon-name" to={hrefOf(child)}>{child.name}</Link>
      {sp && child.common_names?.[0] && <span className="entry-note">{child.common_names[0]}</span>}
      {sp && THREATENED.has(child.conservation) && (
        <abbr className="iucn" title={IUCN[child.conservation]?.label}>{child.conservation}</abbr>
      )}
      {!sp && <span className="entry-note">{child.flags?.species ? `${fmt(child.flags.species)} esp.` : 'sin especies'}</span>}
    </li>
  )
}

export default function TaxonNode({ nodeKey }) {
  const [data, setData] = useState(null)
  const [children, setChildren] = useState([])
  const [error, setError] = useState(null)
  const [loading, setLoading] = useState(true)
  const [loadingMore, setLoadingMore] = useState(false)
  const timer = useElapsedTimer()
  const navigate = useNavigate()
  const { toggle: toggleTheme } = useTheme()
  useSetKingdom(data?.kingdom)

  const [only, setOnly] = useState({})               // { images: true, ... }
  const [letter, setLetter] = useState(null)
  const [country, setCountry] = useState(null)
  const [continent, setContinent] = useState(null)

  useEffect(() => {
    let alive = true
    setLoading(true); setError(null); setData(null); setChildren([])
    setOnly({}); setLetter(null); setCountry(null); setContinent(null)
    timer.start()
    api.taxon(nodeKey)
      .then((d) => { if (alive) { setData(d); setChildren(d.children) } })
      .catch((e) => { if (alive) setError(e.message) })
      .finally(() => { if (alive) { setLoading(false); timer.stop() } })
    return () => { alive = false }
  }, [nodeKey])

  const loadMore = () => {
    setLoadingMore(true)
    api.taxon(nodeKey, children.length)
      .then((d) => setChildren((prev) => [...prev, ...d.children]))
      .catch((e) => setError(e.message))
      .finally(() => setLoadingMore(false))
  }

  // El filtro geográfico solo aplica a especies: los hijos superiores no traen sus países.
  const geoFilterable = children.some((c) => Array.isArray(c.countries))
  const filtered = children.filter((c) => {
    if (CONTENT.some(([k]) => only[k] && !hasContent(c, k))) return false
    if (letter && initialOf(c) !== letter) return false
    if (country && !c.countries?.includes(country)) return false
    if (!country && continent && !c.countries?.some((iso) => ISO_CONTINENT[iso] === continent)) return false
    return true
  })
  const anyFilter = Object.values(only).some(Boolean) || letter || country || continent
  const clearFilters = () => { setOnly({}); setLetter(null); setCountry(null); setContinent(null) }

  useKeyboardShortcuts({ navigate, toggleTheme, taxon: { filteredChildren: filtered, openChild: (c) => navigate(hrefOf(c)) } })

  if (loading || error || !data) {
    return (
      <div className="folio">
        <SiteHeader />
        {loading && <LoadingSpinner inline={false} timeText={timer.elapsedMs != null ? formatElapsed(timer.elapsedMs) : null} />}
        {error && <p className="taxon-error" role="alert">No se pudo cargar el taxón: {error}</p>}
      </div>
    )
  }

  const ancestors = data.lineage.slice(0, -1)
  const flags = data.flags || {}
  const total = data.species_count
  const nextRank = RANKS[RANKS.indexOf(data.rank) + 1]
  const onlySpecies = data.child_ranks.length === 1 && data.child_ranks[0].rank === 'species'
  const letters = new Set(children.map(initialOf))
  const continents = [...new Set(data.countries.map((iso) => ISO_CONTINENT[iso]).filter(Boolean))].sort()
  const groups = data.child_ranks
    .map((g) => ({ ...g, items: filtered.filter((c) => c.rank === g.rank), loaded: children.filter((c) => c.rank === g.rank).length }))
    .filter((g) => g.items.length > 0)
  const partial = children.length < data.children_total

  return (
    <div className="folio">
      <SiteHeader />

      <header className="taxon-head" data-rank={data.rank}>
        <p className="taxon-rank">{rankName(data.rank)}</p>
        <h1 className="taxon-name">{data.name}</h1>
        {ancestors.length > 0 && (
          <nav aria-label="Linaje">
            <ol className="lineage">
              {ancestors.map((a) => <li key={a.key}><Link to={`/taxon/${a.rank}/${a.key}`}>{a.name}</Link></li>)}
              <li aria-current="page">
                <span className="lineage-here">{data.name}</span>
                {data.key >= 0 && <> · <span className="taxon-id" title="taxonID de GBIF">{data.key}</span></>}
              </li>
            </ol>
          </nav>
        )}
        <p className="taxon-summary">
          {fmt(total)} {total === 1 ? 'especie descendiente' : 'especies descendientes'}
          {data.countries.length > 0 && ` en ${fmt(data.countries.length)} ${data.countries.length === 1 ? 'país' : 'países'}`}.
          {!onlySpecies && data.child_ranks.length > 0 && (
            <> Cuelgan directo {listEs(data.child_ranks.map((g) => `${fmt(g.n)} ${rankName(g.rank, g.n).toLowerCase()}`))}.</>
          )}
        </p>
        <p className="taxon-views">
          {flags.images > 0 && <Link to={`/taxon/${data.rank}/${data.key}/gallery`}>Galería de {fmt(flags.images)} especies con imagen</Link>}
          {flags.sounds > 0 && <Link to={`/taxon/${data.rank}/${data.key}/sounds`}>Sonidos de {fmt(flags.sounds)} especies</Link>}
          <Link to={`/taxon/${data.rank}/${data.key}/infographic`}>Infografía</Link>
        </p>
      </header>

      <div className="taxon-body">
        <section className="taxon-main" aria-labelledby="hijos">
          <h2 id="hijos" className="folio-h2" data-rank={data.rank}>Cuelgan de <span className="taxon-name">{data.name}</span></h2>

          {children.length > 0 && (
            <div className="taxon-tools">
              <div role="group" aria-labelledby="solo-con" className="tool-group">
                <span id="solo-con" className="tool-label">Solo con</span>
                {CONTENT.map(([k, label]) => (
                  <label key={k}>
                    <input type="checkbox" checked={!!only[k]} onChange={(e) => setOnly((o) => ({ ...o, [k]: e.target.checked }))} />
                    {label}
                  </label>
                ))}
              </div>
              {(country || continent) && (
                <button type="button" className="ctl" onClick={() => { setCountry(null); setContinent(null) }}>
                  Quitar {country ? isoToName(country) || country : CONTINENT_ES[continent] || continent}
                </button>
              )}
              {anyFilter && <button type="button" className="ctl" onClick={clearFilters}>Quitar filtros</button>}
            </div>
          )}

          {children.length > 30 && (
            <nav className="alpha" aria-label="Filtrar por inicial">
              {ALPHABET.map((l) => (
                <button key={l} type="button" aria-pressed={letter === l} disabled={!letters.has(l)}
                  onClick={() => setLetter((p) => (p === l ? null : l))}>{l}</button>
              ))}
            </nav>
          )}

          {(anyFilter || partial) && children.length > 0 && (
            <p className="taxon-count">
              {anyFilter ? `${fmt(filtered.length)} con estos filtros, de ` : 'Van '}
              {fmt(children.length)} cargados{partial && ` de ${fmt(data.children_total)}`}.
            </p>
          )}

          {groups.map((g) => {
            const gap = RANKS.indexOf(g.rank) > RANKS.indexOf(nextRank)
            return (
              <div className="rank-group" key={g.rank}>
                <div className="rank-label">
                  <h3>{rankName(g.rank, g.n)}</h3>
                  <span className="n">{fmt(g.n)}</span>
                  {gap && <p className="rank-gap">Sin {rankName(nextRank).toLowerCase()} en el backbone de GBIF</p>}
                </div>
                <Brace>
                  <ul className="entries">{g.items.map((c) => <Entry key={c.key} child={c} />)}</ul>
                </Brace>
              </div>
            )
          })}

          {children.length === 0 && <p className="taxon-count">Sin taxones directos registrados.</p>}
          {children.length > 0 && groups.length === 0 && <p className="taxon-count">Ninguno con estos filtros.</p>}
          {partial && (
            <button type="button" className="ctl load-more" onClick={loadMore} disabled={loadingMore}>
              {loadingMore ? 'Cargando…' : `Cargar ${fmt(Math.min(500, data.children_total - children.length))} más`}
            </button>
          )}
        </section>

        <aside className="taxon-margin" aria-label="Al margen">
          {total > 0 && (
            <>
              <h2 className="folio-h2">Contenido</h2>
              <dl className="margin-stats">
                {[['images', 'Con imagen'], ['descriptions', 'Con texto'], ['sounds', 'Con sonido'], ['common_names', 'Con nombre común']]
                  .map(([k, label]) => (
                    <div key={k}><dt>{label}</dt><dd>{fmt(flags[k])} <span>{pct(flags[k] || 0, total)}</span></dd></div>
                  ))}
              </dl>
            </>
          )}
          <h2 className="folio-h2">Distribución</h2>
          {data.countries.length > 0 ? (
            <>
              <p className="margin-note">
                {fmt(data.countries.length)} {data.countries.length === 1 ? 'país' : 'países'}.
                {geoFilterable ? ' Toca uno para filtrar las especies.' : ''}
              </p>
              <MarginMap countries={data.countries} activeCountry={country} activeContinent={continent}
                onSelect={geoFilterable ? (iso) => { setContinent(null); setCountry((p) => (p === iso ? null : iso)) } : null} />
              {geoFilterable && continents.length > 1 && (
                <div className="continents" role="group" aria-label="Filtrar por continente">
                  {continents.map((c) => (
                    <button key={c} type="button" className="ctl" aria-pressed={continent === c}
                      onClick={() => { setCountry(null); setContinent((p) => (p === c ? null : c)) }}>{CONTINENT_ES[c] || c}</button>
                  ))}
                </div>
              )}
            </>
          ) : <p className="margin-note">Sin datos de distribución para este grupo.</p>}
        </aside>
      </div>
    </div>
  )
}
