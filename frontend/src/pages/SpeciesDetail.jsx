import { useEffect, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { api, IUCN, ESTABLISHMENT } from '../api/client'
import { thumb } from '../api/media'
import { isoToName } from '../components/DistributionMap'
import SiteHeader from '../components/SiteHeader'
import MarginMap from '../components/MarginMap'
import ImageLightbox from '../components/ImageLightbox'
import LoadingSpinner from '../components/LoadingSpinner'
import { useTheme } from '../theme/ThemeContext'
import { useSetKingdom } from '../theme/KingdomContext'
import { useKeyboardShortcuts } from '../hooks/useKeyboardShortcuts'
import { useElapsedTimer, formatElapsed } from '../hooks/useElapsedTimer'
import '../theme/species.css'

// Ficha de especie como página del folio: binomio con autoría, lámina, textos por
// tipo con su fuente, y al margen datos, distribución, sonidos, sinónimos y parentesco.

const TYPE_LABEL = {
  summary: 'Resumen', description: 'Descripción', diagnosis: 'Diagnosis', biology: 'Biología y ecología',
  habitat: 'Hábitat', etymology: 'Etimología', discussion: 'Discusión', habit: 'Hábito',
  distribution: 'Distribución', conservation: 'Conservación', reference: 'Referencias', type_specimen: 'Espécimen tipo',
}
const TYPE_ORDER = ['summary', 'description', 'diagnosis', 'biology', 'habitat', 'habit', 'distribution',
  'conservation', 'etymology', 'discussion', 'type_specimen', 'reference']
const LANG_ES = { es: 'español', en: 'inglés', de: 'alemán', fr: 'francés', pt: 'portugués', it: 'italiano', la: 'latín', nl: 'neerlandés', sv: 'sueco', ja: 'japonés', zh: 'chino', ru: 'ruso' }
const LANG_RANK = { es: 0, en: 1 }
const SOURCE_ES = { wikipedia: 'Wikipedia', fishbase: 'FishBase', powo: 'Plants of the World Online', wikimedia_commons: 'Wikimedia Commons', xeno_canto: 'xeno-canto' }
const SHOWN = 4         // textos visibles por sección antes de "ver más"
const SIBLINGS = 12     // parientes listados antes de remitir a la lámina

const byLang = (a, b) => (LANG_RANK[a.lang] ?? 2) - (LANG_RANK[b.lang] ?? 2)
const plain = (t) => (t || '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim()   // algunos textos del backbone traen HTML
const sourceName = (s) => SOURCE_ES[s] || s
const licenseName = (url) => {
  const m = url?.match(/licenses\/([a-z-]+)\/([\d.]+)/i)
  return m ? `CC ${m[1].toUpperCase()} ${m[2]}` : url?.includes('publicdomain') ? 'Dominio público' : null
}

function TextSection({ type, items }) {
  const [all, setAll] = useState(false)
  const sorted = [...items].sort(byLang)
  const shown = all ? sorted : sorted.slice(0, SHOWN)
  const isRef = type === 'reference'
  return (
    <section className={`sp-section${isRef ? ' refs' : ''}`} aria-labelledby={`sec-${type}`}>
      <h2 id={`sec-${type}`} className="folio-h2">{TYPE_LABEL[type] || type.replace(/_/g, ' ')}</h2>
      {isRef ? (
        <ol className="sp-refs">{shown.map((d, i) => <li key={i}>{plain(d.text)}</li>)}</ol>
      ) : shown.map((d, i) => (
        <div className="sp-text" key={i} lang={d.lang || undefined}>
          <p>{plain(d.text)}</p>
          <p className="sp-source">
            {d.lang && d.lang !== 'es' && <>En {LANG_ES[d.lang] || d.lang} · </>}
            {d.url ? <a href={d.url} target="_blank" rel="noreferrer">{sourceName(d.source)}</a> : <cite>{sourceName(d.source)}</cite>}
          </p>
        </div>
      ))}
      {sorted.length > SHOWN && (
        <button type="button" className="ctl" onClick={() => setAll((v) => !v)}>
          {all ? 'Mostrar menos' : `Ver ${sorted.length - SHOWN} más`}
        </button>
      )}
    </section>
  )
}

// Lámina principal: miniatura, luego el original, luego nada (nunca el ícono roto).
function Figure({ image, count, name, onOpen }) {
  const tries = [...new Set([thumb(image.url, 960), image.url])]
  const [i, setI] = useState(0)
  if (i >= tries.length) return null
  const lic = licenseName(image.license)
  return (
    <figure className="sp-figure">
      <button type="button" className="sp-figure-open" onClick={onOpen} aria-label={`Ampliar la lámina de ${name}`}>
        <img src={tries[i]} alt={`${name}${image.title ? `: ${image.title}` : ''}`} onError={() => setI((n) => n + 1)} />
      </button>
      <figcaption>
        {count > 1 ? `Lámina 1 de ${count}` : 'Lámina'}
        {' · '}{image.creator ? `${image.creator}, ` : ''}
        {image.source_url ? <a href={image.source_url} target="_blank" rel="noreferrer">{sourceName(image.source)}</a> : sourceName(image.source)}
        {lic && ` · ${lic}`}
        {' · '}<button type="button" className="ctl" onClick={onOpen}>{count > 1 ? 'Ver todas' : 'Ampliar'}</button>
      </figcaption>
    </figure>
  )
}

export default function SpeciesDetail({ speciesKey }) {
  const navigate = useNavigate()
  const [data, setData] = useState(null)
  const [error, setError] = useState(null)
  const [loading, setLoading] = useState(true)
  const [lightbox, setLightbox] = useState(null)   // índice abierto o null
  const { toggle: toggleTheme } = useTheme()
  useSetKingdom(data?.kingdom)
  const timer = useElapsedTimer()
  useKeyboardShortcuts({ navigate, toggleTheme })

  useEffect(() => {
    let alive = true
    setLoading(true); setError(null); setData(null); setLightbox(null)
    timer.start()
    api.species(speciesKey)
      .then((d) => { if (alive) setData(d) })
      .catch((e) => { if (alive) setError(e.message) })
      .finally(() => { if (alive) { setLoading(false); timer.stop() } })
    return () => { alive = false }
  }, [speciesKey])

  if (loading || error || !data) {
    return (
      <div className="folio">
        <SiteHeader />
        {loading && <LoadingSpinner inline={false} timeText={formatElapsed(timer.elapsedMs)} />}
        {error && <p className="taxon-error" role="alert">No se pudo cargar la especie: {error}</p>}
      </div>
    )
  }

  const images = data.media.filter((m) => m.type === 'image')
  const sounds = data.media.filter((m) => m.type === 'sound')
  const ancestors = data.lineage.filter((a) => a.key !== data.key)
  const author = data.authorship || (data.scientific_name?.startsWith(data.name) ? data.scientific_name.slice(data.name.length).trim() : '')

  // Nombres comunes: español primero; los demás con su idioma.
  const names = (data.common_names || []).map((n, i) => ({ n, lang: data.common_names_languages?.[i] }))
  const namesEs = names.filter((x) => x.lang === 'es').map((x) => x.n)
  const namesOther = names.filter((x) => x.lang !== 'es').sort(byLang)

  const byType = {}
  for (const d of data.descriptions) (byType[d.type || 'description'] ||= []).push(d)
  const types = [...TYPE_ORDER.filter((t) => byType[t]), ...Object.keys(byType).filter((t) => !TYPE_ORDER.includes(t))]

  const dist = data.distribution || []
  const countries = [...new Set(dist.map((d) => d.country))]
  const iucn = data.conservation_overall_code
  const rel = data.relatives || {}

  return (
    <div className="folio">
      <SiteHeader />

      <header className="taxon-head" data-rank="species">
        <p className="taxon-rank">Especie{data.status && data.status !== 'accepted' ? ` · ${data.status}` : ''}</p>
        <h1 className="sp-title"><span className="taxon-name">{data.name}</span>{author && <span className="sp-author"> {author}</span>}</h1>
        <nav aria-label="Linaje">
          <ol className="lineage">
            {ancestors.map((a) => <li key={a.key}><Link to={`/taxon/${a.rank}/${a.key}`}>{a.name}</Link></li>)}
            <li aria-current="page"><span className="lineage-here taxon-name">{data.name}</span> · <span className="taxon-id" title="taxonID de GBIF">{data.key}</span></li>
          </ol>
        </nav>
        {namesEs.length > 0 && <p className="sp-names">{namesEs.join(', ')}</p>}
        {namesOther.length > 0 && (
          <details className="sp-names-other">
            <summary>Nombres en otros idiomas ({namesOther.length})</summary>
            <ul>{namesOther.map((x, i) => <li key={i} lang={x.lang}>{x.n} <span>{LANG_ES[x.lang] || x.lang}</span></li>)}</ul>
          </details>
        )}
      </header>

      <div className="species-body">
        <main className="sp-main">
          {images.length > 0 && <Figure image={images[0]} count={images.length} name={data.name} onOpen={() => setLightbox(0)} />}
          {images.length > 1 && (
            <ul className="sp-thumbs" aria-label="Más láminas">
              {images.slice(1, 9).map((m, i) => (
                <li key={m.url}>
                  <button type="button" onClick={() => setLightbox(i + 1)} aria-label={`Ampliar lámina ${i + 2} de ${images.length}`}>
                    <img src={thumb(m.url, 250)} alt="" loading="lazy" onError={(e) => { e.currentTarget.closest('li').hidden = true }} />
                  </button>
                </li>
              ))}
            </ul>
          )}
          {types.length > 0
            ? types.map((t) => <TextSection key={t} type={t} items={byType[t]} />)
            : <p className="margin-note">No hay textos registrados para esta especie.</p>}
        </main>

        <aside className="taxon-margin" aria-label="Al margen">
          <h2 className="folio-h2">Datos</h2>
          <dl className="margin-stats">
            {iucn && (
              <div><dt>Lista Roja UICN</dt>
                <dd><abbr title={data.conservation_overall || ''} className={`iucn-tag${['VU', 'EN', 'CR', 'EW', 'EX'].includes(iucn) ? ' threat' : ''}`}>{IUCN[iucn]?.label || iucn} ({iucn})</abbr></dd></div>
            )}
            {data.habit && <div><dt>Hábito</dt><dd>{data.habit}</dd></div>}
            <div><dt>Imágenes</dt><dd>{images.length}</dd></div>
            <div><dt>Sonidos</dt><dd>{sounds.length}</dd></div>
            <div><dt>Textos</dt><dd>{data.descriptions.filter((d) => d.type !== 'reference').length}</dd></div>
          </dl>

          {sounds.length > 0 && (
            <>
              <h2 className="folio-h2">Sonidos</h2>
              <ul className="sp-sounds">
                {sounds.slice(0, 3).map((m, i) => (
                  <li key={m.url}>
                    <audio controls preload="none" src={m.url} aria-label={`Grabación ${i + 1} de ${data.name}`} />
                    <p className="margin-note">
                      {m.source_url ? <a href={m.source_url} target="_blank" rel="noreferrer">{sourceName(m.source)}</a> : sourceName(m.source)}
                      {licenseName(m.license) && ` · ${licenseName(m.license)}`}
                    </p>
                  </li>
                ))}
              </ul>
              {sounds.length > 3 && rel.genus && (
                <p className="margin-note"><Link to={`/taxon/${rel.genus.rank}/${rel.genus.key}/sounds`}>Más sonidos del género</Link></p>
              )}
            </>
          )}

          <h2 className="folio-h2">Distribución</h2>
          {countries.length > 0 ? (
            <>
              <MarginMap countries={countries} />
              <ul className="sp-countries">
                {dist.map((d, i) => {
                  const est = d.establishment_means && ESTABLISHMENT[d.establishment_means]?.label
                  return (
                    <li key={`${d.country}${i}`}>
                      {isoToName(d.country) || d.country}
                      {est && <span> · {est.toLowerCase()}</span>}
                      {d.conservation_code && d.conservation_code !== 'NE' && <span> · {IUCN[d.conservation_code]?.label.toLowerCase() || d.conservation_code}</span>}
                    </li>
                  )
                })}
              </ul>
            </>
          ) : <p className="margin-note">Sin datos de distribución.</p>}

          {data.synonyms?.length > 0 && (
            <>
              <h2 className="folio-h2">Sinónimos</h2>
              <ul className="sp-synonyms">{data.synonyms.slice(0, 10).map((s) => <li key={s} className="binomial">{s}</li>)}</ul>
              {data.synonyms.length > 10 && (
                <details className="sp-names-other sp-syn-more">
                  <summary>Otros {data.synonyms.length - 10}</summary>
                  <ul className="sp-synonyms">{data.synonyms.slice(10).map((s) => <li key={s} className="binomial">{s}</li>)}</ul>
                </details>
              )}
            </>
          )}

          {(rel.sibling_species?.length > 0 || rel.sibling_genera?.length > 0) && (
            <>
              <h2 className="folio-h2">Parentesco</h2>
              {rel.sibling_species?.length > 0 && rel.genus && (
                <div className="sp-kin">
                  <p className="margin-note">Otras especies de <Link to={`/taxon/${rel.genus.rank}/${rel.genus.key}`} className="taxon-name binomial">{rel.genus.name}</Link></p>
                  <ul>{rel.sibling_species.slice(0, SIBLINGS).map((s) => <li key={s.key}><Link className="binomial" to={`/species/${s.key}`}>{s.name}</Link></li>)}</ul>
                  {rel.sibling_species.length > SIBLINGS && <Link to={`/taxon/${rel.genus.rank}/${rel.genus.key}`}>y {rel.sibling_species.length - SIBLINGS} más</Link>}
                </div>
              )}
              {rel.sibling_genera?.length > 0 && rel.family && (
                <div className="sp-kin">
                  <p className="margin-note">Otros géneros de <Link to={`/taxon/${rel.family.rank}/${rel.family.key}`}>{rel.family.name}</Link></p>
                  <ul>{rel.sibling_genera.slice(0, SIBLINGS).map((g) => <li key={g.key}><Link className="binomial" to={`/taxon/${g.rank}/${g.key}`}>{g.name}</Link></li>)}</ul>
                  {rel.sibling_genera.length > SIBLINGS && <Link to={`/taxon/${rel.family.rank}/${rel.family.key}`}>y {rel.sibling_genera.length - SIBLINGS} más</Link>}
                </div>
              )}
            </>
          )}
        </aside>
      </div>

      {lightbox != null && (
        <ImageLightbox items={images} index={lightbox} onClose={() => setLightbox(null)} speciesName={data.name} />
      )}
    </div>
  )
}
