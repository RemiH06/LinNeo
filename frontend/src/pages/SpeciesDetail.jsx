import { useEffect, useState } from 'react'
import { api } from '../api/client'
import { Card, Badge, Metric, Callout } from '../components/ui'

const SOURCE_VARIANT = {
  wikipedia: 'arctic', powo: 'pine', fishbase: 'teal',
  amphibiaweb: 'violet', eol: 'indigo',
}

// Orden taxonomico esperado para ordenar el linaje
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
  const lineage = (data.lineage || [])
    .slice()
    .sort((a, b) => RANK_ORDER.indexOf(a.rank) - RANK_ORDER.indexOf(b.rank))

  return (
    <div>
      {/* ── Encabezado ── */}
      <div style={{ marginBottom: 8 }}>
        <div style={{ fontSize: 26, fontWeight: 700, letterSpacing: '-1px', fontStyle: 'italic' }}>
          {data.scientific_name || data.canonical_name}
        </div>
        {data.canonical_name && data.canonical_name !== data.scientific_name && (
          <div className="muted" style={{ marginTop: 2 }}>{data.canonical_name}</div>
        )}
      </div>

      <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', margin: '12px 0' }}>
        {data.kingdom && <Badge variant="accent">{data.kingdom}</Badge>}
        {data.habit && <Badge variant="teal">{data.habit}</Badge>}
        {commonNames.slice(0, 8).map((n, i) => <Badge key={i}>{n}</Badge>)}
        {commonNames.length > 8 && <Badge>+{commonNames.length - 8}</Badge>}
      </div>

      {/* ── Metricas ── */}
      <div className="metrics-row">
        <Metric value={data.descriptions?.length || 0} label="DESCRIPCIONES" />
        <Metric value={images.length} label="IMAGENES" variant="teal" />
        <Metric value={sounds.length} label="SONIDOS" variant="violet" />
        <Metric value={data.countries?.length || 0} label="PAISES" variant="arctic" />
      </div>

      {/* ── Taxonomia ── */}
      {lineage.length > 0 && (
        <>
          <h2>Taxonomia</h2>
          <div className="pipeline-row">
            {lineage.map((node, i) => (
              <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <div style={{ display: 'flex', flexDirection: 'column' }}>
                  <span className="pipeline-step">{node.rank}</span>
                  <span style={{ fontSize: 12 }}>{node.name}</span>
                </div>
                {i < lineage.length - 1 && <span style={{ color: 'var(--text2)' }}>{'\u203A'}</span>}
              </div>
            ))}
          </div>
        </>
      )}

      {/* ── Imagenes ── */}
      {images.length > 0 && (
        <>
          <h2>Imagenes</h2>
          <div className="media-grid">
            {images.map((m, i) => (
              <div key={i} className="media-thumb" onClick={() => onOpenMedia?.({ type: 'image', items: images, index: i })} title="Abrir galeria">
                <img src={m.url} alt={data.canonical_name} loading="lazy" />
              </div>
            ))}
          </div>
        </>
      )}

      {/* ── Sonidos ── */}
      {sounds.length > 0 && (
        <>
          <h2>Sonidos</h2>
          {sounds.slice(0, 3).map((m, i) => (
            <Card key={i}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
                <Badge variant="violet">xeno-canto</Badge>
                {m.source_url && <a href={m.source_url} target="_blank" rel="noreferrer">fuente</a>}
              </div>
              <audio controls preload="none" src={m.url} />
            </Card>
          ))}
          {sounds.length > 3 && (
            <button className="btn" onClick={() => onOpenMedia?.({ type: 'sound', items: sounds, index: 0 })}>
              Ver los {sounds.length} sonidos
            </button>
          )}
        </>
      )}

      {/* ── Descripciones ── */}
      {data.descriptions?.length > 0 && (
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
      )}

      {/* ── Distribucion ── */}
      {data.countries?.length > 0 && (
        <>
          <h2>Distribucion</h2>
          <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', marginBottom: 12 }}>
            {data.countries.slice(0, 30).map((c, i) => <Badge key={i}>{c}</Badge>)}
            {data.countries.length > 30 && <Badge variant="accent">+{data.countries.length - 30}</Badge>}
          </div>
          <button className="btn primary" onClick={() => onOpenMedia?.({ type: 'map', countries: data.countries })}>
            Ver en mapa
          </button>
        </>
      )}
    </div>
  )
}