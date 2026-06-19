import { useEffect, useState } from 'react'
import { api, IUCN } from '../api/client'
import { Card, Badge, Metric, Callout } from '../components/ui'
import TaxonomyGraph from '../components/TaxonomyGraph'
import DistributionMap, { isoToName } from '../components/DistributionMap'
import { useTheme } from '../theme/ThemeContext'
import { kingdomStyleVars } from '../theme/kingdomColor'
import { useSetKingdom } from '../theme/KingdomContext'
import ImageLightbox from '../components/ImageLightbox'
import { useKeyboardShortcuts } from '../hooks/useKeyboardShortcuts'
import { useNavigate } from 'react-router-dom'
import '../theme/lightbox.css'

const SOURCE_VARIANT = {
  wikipedia: 'arctic', powo: 'pine', fishbase: 'teal',
  amphibiaweb: 'violet', eol: 'indigo',
}
const RANK_ORDER = ['kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species']

// Etiquetas en espanol y orden de los tipos de descripcion del backbone
const TYPE_LABEL = {
  description: 'Descripcion', diagnosis: 'Diagnosis', biology: 'Biologia y ecologia',
  habitat: 'Habitat', etymology: 'Etimologia', discussion: 'Discusion', habit: 'Habito',
  reference: 'Referencias', type_specimen: 'Especimen tipo',
}
// orden de aparicion de las secciones
const TYPE_ORDER = ['etymology', 'description', 'diagnosis', 'biology', 'habitat', 'habit', 'discussion', 'reference', 'type_specimen']
const LANG_LABEL = { en: 'EN', es: 'ES', de: 'DE', fr: 'FR', pt: 'PT', it: 'IT' }

function ConservationBadge({ code, status }) {
  if (!code) return null
  const info = IUCN[code] || { label: status || code, color: 'var(--text2)' }
  return (
    <span style={{
      display: 'inline-flex', alignItems: 'center', gap: 5,
      fontFamily: 'var(--mono)', fontSize: 10, fontWeight: 700,
      padding: '3px 9px', borderRadius: 'var(--radius)',
      border: `1px solid ${info.color}`, color: info.color,
      letterSpacing: '.05em',
    }}>
      <span style={{ width: 8, height: 8, borderRadius: '50%', background: info.color }} />
      {code} - {info.label}
    </span>
  )
}

export default function SpeciesDetail({ speciesKey, onOpenMedia }) {
  const navigate = useNavigate()
  const [data, setData] = useState(null)
  const [error, setError] = useState(null)
  const [loading, setLoading] = useState(true)
  const { dark, toggle: toggleTheme } = useTheme()
  useSetKingdom(data?.kingdom)
  const [lightbox, setLightbox] = useState(null) // { items, index } | null
  useKeyboardShortcuts({ navigate, toggleTheme })

  useEffect(() => {
    let alive = true
    setLoading(true); setError(null); setData(null)
    api.species(speciesKey)
      .then((d) => { if (alive) { setData(d); setLoading(false) } })
      .catch((e) => { if (alive) { setError(e.message); setLoading(false) } })
    return () => { alive = false }
  }, [speciesKey])

  if (loading) return <Callout title="Cargando">Consultando el grafo...</Callout>
  if (error) return <Callout title="Error" variant="danger">{error}</Callout>
  if (!data) return null

  const images = (data.media || []).filter((m) => m.type === 'image')
  const sounds = (data.media || []).filter((m) => m.type === 'sound')
  const commonNames = data.common_names || []
  const distribution = data.distribution || []
  const allDescriptions = data.descriptions || []
  // separar: las de Wikipedia/otras fuentes (sin type) van como "resumen";
  // las del backbone se agrupan por type
  const prose = allDescriptions.filter((d) => !d.type)
  const byType = {}
  for (const d of allDescriptions) {
    if (!d.type) continue
    ;(byType[d.type] ||= []).push(d)
  }
  const typedSections = TYPE_ORDER.filter((t) => byType[t]?.length)
  const lineage = (data.lineage || []).slice()
    .sort((a, b) => RANK_ORDER.indexOf(a.rank) - RANK_ORDER.indexOf(b.rank))
  const title = data.scientific_name || data.canonical_name

  // conservacion por pais (solo los que tienen codigo), para el detalle
  const consByCountry = distribution.filter((d) => d.conservation_code)

  return (
    <div className="triptych" style={kingdomStyleVars(data.kingdom, dark)}>
      {/* ══ IZQUIERDA — ficha tecnica + grafo ══ */}
      <aside className="col-left">
        <div className="panel">
          <div style={{ fontSize: 18, fontWeight: 700, letterSpacing: '-.5px', fontStyle: 'italic', lineHeight: 1.3 }}>
            {title}
          </div>
          {data.canonical_name && data.canonical_name !== title && (
            <div className="muted" style={{ marginTop: 2 }}>{data.canonical_name}</div>
          )}

          <div style={{ display: 'flex', gap: 5, flexWrap: 'wrap', margin: '10px 0' }}>
            {data.kingdom && <Badge variant="accent">{data.kingdom}</Badge>}
            {data.habit && <Badge variant="teal">{data.habit}</Badge>}
          </div>

          {/* Estado de conservacion general (peor caso) */}
          {data.conservation_overall_code && (
            <div style={{ margin: '10px 0' }}>
              <h3 style={{ margin: '8px 0 5px' }}>Conservacion</h3>
              <ConservationBadge code={data.conservation_overall_code} status={data.conservation_overall} />
            </div>
          )}

          {commonNames.length > 0 && (
            <div style={{ marginBottom: 10 }}>
              <h3 style={{ margin: '8px 0 4px' }}>Nombres comunes</h3>
              <div style={{ display: 'flex', gap: 5, flexWrap: 'wrap' }}>
                {commonNames.slice(0, 12).map((n, i) => <Badge key={i}>{n}</Badge>)}
                {commonNames.length > 12 && <Badge>+{commonNames.length - 12}</Badge>}
              </div>
            </div>
          )}

          <div className="metrics-col">
            <Metric value={data.descriptions?.length || 0} label="DESCRIPCIONES" />
            <Metric value={images.length} label="IMAGENES" variant="teal" />
            <Metric value={sounds.length} label="SONIDOS" variant="violet" />
            <Metric value={distribution.length} label="PAISES" variant="arctic" />
          </div>
        </div>

        <div className="panel">
          <h3>Taxonomia y parentesco</h3>
          <TaxonomyGraph
            lineage={lineage}
            relatives={data.relatives}
            currentName={data.canonical_name || title}
            currentKey={data.species_key}
            kingdom={data.kingdom}
          />
        </div>
      </aside>

      {/* ══ CENTRO — descripcion + mapa ══ */}
      <main className="col-center">
        {(prose.length > 0 || typedSections.length > 0) ? (
          <>
            {/* Resumen (Wikipedia y otras fuentes de prosa) */}
            {prose.length > 0 && (
              <>
                <h2>Resumen</h2>
                {prose.map((d, i) => (
                  <Card key={i}>
                    <div style={{ marginBottom: 8 }}>
                      <Badge variant={SOURCE_VARIANT[d.source] || ''}>{d.source}</Badge>
                    </div>
                    <p style={{ marginBottom: 0 }}>{d.text}</p>
                    {d.url && (
                      <div style={{ marginTop: 8 }}>
                        <a href={d.url} target="_blank" rel="noreferrer">{'\u2197'} fuente original</a>
                      </div>
                    )}
                  </Card>
                ))}
              </>
            )}

            {/* Secciones del backbone agrupadas por tipo */}
            {typedSections.map((type) => (
              <div key={type}>
                <h2>{TYPE_LABEL[type] || type}</h2>
                {byType[type].slice(0, type === 'reference' ? 8 : 6).map((d, i) => (
                  <Card key={i}>
                    <div style={{ marginBottom: 6, display: 'flex', gap: 5, flexWrap: 'wrap', alignItems: 'center' }}>
                      {d.lang && <Badge>{LANG_LABEL[d.lang] || d.lang.toUpperCase()}</Badge>}
                      {d.source && <span className="muted" style={{ fontSize: 10 }}>{d.source}</span>}
                    </div>
                    <p style={{ marginBottom: 0, fontSize: type === 'reference' ? 12 : undefined }}>{d.text}</p>
                  </Card>
                ))}
                {byType[type].length > (type === 'reference' ? 8 : 6) && (
                  <p className="muted" style={{ fontSize: 11 }}>
                    +{byType[type].length - (type === 'reference' ? 8 : 6)} mas
                  </p>
                )}
              </div>
            ))}
          </>
        ) : (
          <Callout title="Sin descripcion">No hay descripcion registrada para esta especie.</Callout>
        )}

        {distribution.length > 0 && (
          <>
            <h2>Distribucion</h2>
            <DistributionMap distribution={distribution} continents={data.continents || []} />
            <div style={{ display: 'flex', gap: 5, flexWrap: 'wrap', marginTop: 8 }}>
              {(data.continents || []).map((c, i) => <Badge key={i} variant="accent">{c}</Badge>)}
            </div>

            {/* Conservacion por pais, si hay */}
            {consByCountry.length > 0 && (
              <div style={{ marginTop: 12 }}>
                <h3>Conservacion por pais</h3>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                  {consByCountry.map((d, i) => (
                    <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 11 }}>
                      <span style={{ minWidth: 130 }}>{isoToName(d.country)}</span>
                      <ConservationBadge code={d.conservation_code} status={d.conservation_status} />
                    </div>
                  ))}
                </div>
              </div>
            )}
            <button className="btn primary" style={{ marginTop: 8 }} onClick={() => onOpenMedia?.({ type: 'map', distribution })}>
              Ver mapa completo
            </button>
          </>
        )}
      </main>

      {/* ══ DERECHA — audio + imagenes ══ */}
      <aside className="col-right">
        {sounds.length > 0 && (
          <div className="panel">
            <h3>Sonido</h3>
            {sounds.slice(0, 3).map((m, i) => (
              <div key={i} style={{ marginBottom: 10 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
                  <Badge variant="violet">xeno-canto</Badge>
                  {m.source_url && <a href={m.source_url} target="_blank" rel="noreferrer" style={{ fontSize: 10 }}>fuente</a>}
                </div>
                <audio controls preload="none" src={m.url} />
              </div>
            ))}
            {sounds.length > 3 && (
              <button className="btn" onClick={() => onOpenMedia?.({ type: 'sound', items: sounds, index: 0 })}>
                Ver los {sounds.length} sonidos
              </button>
            )}
          </div>
        )}

        {images.length > 0 && (
          <div className="panel">
            <h3>Imagenes</h3>
            <div className="media-grid-side">
              {images.map((m, i) => (
                <div key={i} className="media-thumb-lg" onClick={() => setLightbox({ items: images, index: i })} title="Abrir galeria">
                  <img src={m.url} alt={data.canonical_name} loading="lazy" />
                </div>
              ))}
            </div>
          </div>
        )}
      </aside>

      {lightbox && (
        <ImageLightbox
          items={lightbox.items}
          index={lightbox.index}
          onClose={() => setLightbox(null)}
          speciesName={data.canonical_name || data.scientific_name}
        />
      )}
    </div>
  )
}