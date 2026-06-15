import { useEffect, useState } from 'react'
import { api } from '../api/client'
import { Card, Badge, Metric, Callout } from '../components/ui'
import RelativesTree from '../components/RelativesTree'
import DistributionMap from '../components/DistributionMap'

const SOURCE_VARIANT = {
  wikipedia: 'arctic', powo: 'pine', fishbase: 'teal',
  amphibiaweb: 'violet', eol: 'indigo',
}
const RANK_ORDER = ['Kingdom', 'Phylum', 'Class', 'Order', 'Family', 'Genus', 'Species']

export default function SpeciesDetail({ speciesKey, onOpenMedia }) {
  const [data, setData] = useState(null)
  const [error, setError] = useState(null)
  const [loading, setLoading] = useState(true)

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
  const lineage = (data.lineage || []).slice()
    .sort((a, b) => RANK_ORDER.indexOf(a.rank) - RANK_ORDER.indexOf(b.rank))
  const title = data.scientific_name || data.canonical_name

  return (
    <div className="triptych">
      {/* ══ IZQUIERDA (20%) — ficha tecnica + arbol taxonomico ══ */}
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
            <Metric value={data.countries?.length || 0} label="PAISES" variant="arctic" />
          </div>
        </div>

        {/* Linaje completo */}
        {lineage.length > 0 && (
          <div className="panel">
            <h3>Linaje</h3>
            <div style={{ fontSize: 11 }}>
              {lineage.map((node, i) => (
                <div key={i} style={{ paddingLeft: i * 10, color: i === lineage.length - 1 ? 'var(--accent)' : 'var(--text2)', padding: '2px 0' }}>
                  <span className="pipeline-step" style={{ marginRight: 6 }}>{node.rank.slice(0, 3)}</span>
                  {node.name}
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Arbol de parentesco */}
        {data.relatives && (
          <div className="panel">
            <h3>Parentesco</h3>
            <RelativesTree relatives={data.relatives} currentName={data.canonical_name || title} />
          </div>
        )}
      </aside>

      {/* ══ CENTRO (45%) — descripcion + imagenes ══ */}
      <main className="col-center">
        {data.descriptions?.length > 0 ? (
          <>
            <h2>Descripcion</h2>
            {data.descriptions.map((d, i) => (
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
        ) : (
          <Callout title="Sin descripcion">No hay descripcion registrada para esta especie.</Callout>
        )}

        {images.length > 0 && (
          <>
            <h2>Imagenes</h2>
            <div className="media-grid-lg">
              {images.map((m, i) => (
                <div key={i} className="media-thumb-lg" onClick={() => onOpenMedia?.({ type: 'image', items: images, index: i })} title="Abrir galeria">
                  <img src={m.url} alt={data.canonical_name} loading="lazy" />
                </div>
              ))}
            </div>
          </>
        )}
      </main>

      {/* ══ DERECHA (35%) — audio + mapa ══ */}
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

        <div className="panel">
          <h3>Distribucion</h3>
          {data.countries?.length > 0 ? (
            <>
              <DistributionMap countries={data.countries} />
              <div style={{ display: 'flex', gap: 5, flexWrap: 'wrap', marginTop: 8 }}>
                {(data.continents || []).map((c, i) => <Badge key={i} variant="accent">{c}</Badge>)}
              </div>
              <button className="btn primary" style={{ marginTop: 8 }} onClick={() => onOpenMedia?.({ type: 'map', countries: data.countries })}>
                Ver mapa completo
              </button>
            </>
          ) : (
            <p className="muted">Sin datos de distribucion.</p>
          )}
        </div>
      </aside>
    </div>
  )
}