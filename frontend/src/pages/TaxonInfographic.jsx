import { useEffect, useState } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { api, IUCN } from '../api/client'
import { thumb } from '../api/media'
import { useTheme } from '../theme/ThemeContext'
import { useSetKingdom } from '../theme/KingdomContext'
import { kingdomInk } from '../theme/kingdomColor'
import { useKeyboardShortcuts } from '../hooks/useKeyboardShortcuts'
import { useElapsedTimer, formatElapsed } from '../hooks/useElapsedTimer'
import LoadingSpinner from '../components/LoadingSpinner'
import SiteHeader from '../components/SiteHeader'
import MiniWorldMap from '../components/MiniWorldMap'
import '../theme/infographic.css'

/*
  Infografía de un taxón como lámina suelta (y póster imprimible): cifras del
  subárbol, mapa, Lista Roja en su orden, desglose por reino si hay más de uno y
  tres especies destacadas que cambian en cada visita.
*/
const RANK_ES = {
  biota: 'Biota', domain: 'Dominio', kingdom: 'Reino', phylum: 'Filo', class: 'Clase', order: 'Orden',
  family: 'Familia', genus: 'Género', species: 'Especie',
}
const IUCN_ORDER = ['LC', 'NT', 'VU', 'EN', 'CR', 'RE', 'EW', 'EX', 'DD', 'NE']
const THREATENED = new Set(['VU', 'EN', 'CR', 'EW', 'EX'])
const fmt = (n) => (n ?? 0).toLocaleString('es-MX')
const pct = (n, t) => (t ? Math.round((100 * n) / t) : 0)

function Bars({ rows, label, color }) {
  const max = Math.max(1, ...rows.map((r) => r.n))
  return (
    <ul className="ig-bars" aria-label={label}>
      {rows.map((r) => (
        <li key={r.key} title={`${r.name}: ${fmt(r.n)}`} className={r.threat ? 'threat' : ''}>
          <span className="ig-bar-name">{r.name}{r.threat && <span className="ig-tag"> · amenazada</span>}</span>
          <span className="ig-bar-track" aria-hidden="true">
            <span className="ig-bar" style={{ width: `${(100 * r.n) / max}%`, background: color?.(r) }} />
          </span>
          <span className="ig-bar-n">{fmt(r.n)}</span>
        </li>
      ))}
    </ul>
  )
}

export default function TaxonInfographic() {
  const { key } = useParams()
  const navigate = useNavigate()
  const { dark, toggle: toggleTheme } = useTheme()
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const timer = useElapsedTimer()
  useSetKingdom(data?.kingdom)
  useKeyboardShortcuts({ navigate, toggleTheme })

  useEffect(() => {
    let alive = true
    setLoading(true); setError(null); setData(null)
    timer.start()
    api.taxonInfographic(key)
      .then((d) => { if (alive) setData(d) })
      .catch((e) => { if (alive) setError(e.message) })
      .finally(() => { if (alive) { setLoading(false); timer.stop() } })
    return () => { alive = false }
  }, [key])

  if (!data) {
    return (
      <div className="folio">
        <SiteHeader />
        {loading && <LoadingSpinner inline={false} timeText={formatElapsed(timer.elapsedMs)} />}
        {error && <p className="taxon-error" role="alert">No se pudo cargar la infografía: {error}</p>}
      </div>
    )
  }

  const total = data.species_count
  const cons = [...data.conservation].sort((a, b) => IUCN_ORDER.indexOf(a.code) - IUCN_ORDER.indexOf(b.code))
  const evaluated = cons.reduce((s, c) => s + c.count, 0)
  const threatened = cons.filter((c) => THREATENED.has(c.code)).reduce((s, c) => s + c.count, 0)
  const stats = [
    [total, total === 1 ? 'especie' : 'especies'], [data.countries.length, data.countries.length === 1 ? 'país' : 'países'],
    [data.images_count, 'con imagen'], [data.sounds_count, 'con sonido'], [data.descriptions_count, 'con texto'],
  ]

  return (
    <div className="folio ig">
      <SiteHeader />
      <article className="ig-sheet" data-rank={data.rank}>
        <header className="ig-head">
          <p className="taxon-rank">Infografía · {RANK_ES[data.rank] || data.rank}{data.kingdom && data.rank !== 'kingdom' ? ` · ${data.kingdom}` : ''}</p>
          <h1 className="taxon-name"><Link to={`/taxon/${data.rank}/${data.key}`}>{data.name}</Link></h1>
          <button type="button" className="ctl ig-print" onClick={() => window.print()}>Imprimir</button>
        </header>

        <dl className="ig-stats">
          {stats.map(([n, label]) => (
            <div key={label}><dt>{label}</dt><dd>{fmt(n)}</dd></div>
          ))}
        </dl>

        <div className="ig-grid">
          <section aria-labelledby="ig-mapa">
            <h2 id="ig-mapa" className="folio-h2">Dónde vive</h2>
            {data.countries.length > 0
              ? <MiniWorldMap countries={data.countries} height="auto" />
              : <p className="margin-note">Sin datos de distribución.</p>}
          </section>

          <section aria-labelledby="ig-uicn">
            <h2 id="ig-uicn" className="folio-h2">Lista Roja de la UICN</h2>
            {cons.length > 0 ? (
              <>
                <p className="margin-note">
                  {fmt(evaluated)} de {fmt(total)} especies evaluadas ({pct(evaluated, total)} %)
                  {threatened > 0 && <>; <strong className="ig-threat">{fmt(threatened)} amenazadas</strong></>}.
                </p>
                <Bars label="Especies por categoría de la Lista Roja"
                  rows={cons.map((c) => ({ key: c.code, n: c.count, threat: THREATENED.has(c.code), name: `${IUCN[c.code]?.label || c.code} (${c.code})` }))} />
              </>
            ) : <p className="margin-note">Ninguna especie evaluada.</p>}

            {data.by_kingdom.length > 1 && (
              <>
                <h2 className="folio-h2 margin-gap">Por reino</h2>
                <Bars label="Especies por reino" color={(r) => kingdomInk(r.key, dark)}
                  rows={data.by_kingdom.map((k) => ({ key: k.kingdom, n: k.count, name: k.kingdom }))} />
              </>
            )}
          </section>
        </div>

        {data.featured.length > 0 && (
          <section className="ig-featured" aria-labelledby="ig-dest">
            <h2 id="ig-dest" className="folio-h2">Tres especies al azar</h2>
            <ul>
              {data.featured.map((f) => (
                <li key={f.key}>
                  <Link to={`/species/${f.key}`}>
                    <span className="plate">{f.image ? <img src={thumb(f.image, 330)} alt="" loading="lazy" /> : <span className="plate-empty">Sin lámina</span>}</span>
                    <span className="binomial">{f.name}</span>
                    {f.common_names?.[0] && <span className="example-common">{f.common_names[0]}</span>}
                  </Link>
                </li>
              ))}
            </ul>
          </section>
        )}
        <p className="ig-colophon">LinNeo · datos del backbone de GBIF, Wikipedia, Wikimedia Commons y xeno-canto{data.key >= 0 ? ` · taxonID ${data.key}` : ''}</p>
      </article>
    </div>
  )
}
