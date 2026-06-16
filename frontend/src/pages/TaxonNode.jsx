import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { MapContainer, TileLayer, GeoJSON } from 'react-leaflet'
import 'leaflet/dist/leaflet.css'
import { api } from '../api/client'
import '../theme/bookworm.css'

const GEO_URL = 'https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson'
function featureISO(props) {
  return (props['ISO3166-1-Alpha-2'] || props.ISO_A2 || props.iso_a2 || '').toUpperCase()
}

const RANK_ES = {
  kingdom: 'Reino', phylum: 'Filo', class: 'Clase', order: 'Orden',
  family: 'Familia', genus: 'Genero', species: 'Especie',
}

// Mini-mapa agregado: pinta todos los paises de las especies descendientes.
function AggregateMap({ countries }) {
  const [geo, setGeo] = useState(null)
  const targets = new Set((countries || []).map((c) => String(c).toUpperCase()))
  useEffect(() => {
    let alive = true
    fetch(GEO_URL).then((r) => r.json()).then((d) => { if (alive) setGeo(d) }).catch(() => {})
    return () => { alive = false }
  }, [])
  const styleFn = (f) => {
    const active = targets.has(featureISO(f.properties))
    return { fillColor: active ? '#4A6040' : '#cabfa0', fillOpacity: active ? 0.8 : 0.15,
             color: active ? '#6A8050' : '#b8a888', weight: active ? 1 : 0.3 }
  }
  return (
    <div className="bw-map">
      <MapContainer center={[20, 0]} zoom={1} minZoom={1} style={{ height: 340, width: '100%', background: '#cec4ac' }}
        attributionControl={false} zoomControl={true} scrollWheelZoom={false} worldCopyJump={true}>
        <TileLayer url="https://{s}.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}{r}.png" subdomains="abcd" maxZoom={6} />
        {geo && <GeoJSON data={geo} style={styleFn} />}
      </MapContainer>
    </div>
  )
}

// Sellos de contenido disponible para un hijo (especie o taxon superior).
// Para especie: cuenta su propio contenido. Para taxon: cuantas especies descendientes lo tienen.
function ContentFlags({ flags, isSpecies, conservation }) {
  if (!flags) return null
  const items = []
  const Seal = ({ title, icon, n, color }) => (
    <span title={title} style={{
      display: 'inline-flex', alignItems: 'center', gap: 3,
      fontFamily: 'var(--bw-mono)', fontSize: 10, padding: '1px 6px',
      borderRadius: 10, border: `1px solid ${color}`, color,
      background: 'var(--bw-bg3)', lineHeight: 1.6,
    }}>
      <span>{icon}</span>{n != null && n > 0 ? <span>{n}</span> : null}
    </span>
  )

  if (isSpecies) {
    if (flags.images > 0) items.push(<Seal key="i" title="Imagenes" icon={'\u25A3'} n={flags.images} color="var(--bw-moss)" />)
    if (flags.sounds > 0) items.push(<Seal key="s" title="Sonidos" icon={'\u266A'} n={flags.sounds} color="var(--bw-amber)" />)
    if (flags.descriptions > 0) items.push(<Seal key="d" title="Descripciones" icon={'\u2261'} n={flags.descriptions} color="var(--bw-bark)" />)
    if (flags.etymology > 0) items.push(<Seal key="e" title="Etimologia" icon={'\u00A7'} color="var(--bw-ochre)" />)
    if (conservation) items.push(<Seal key="c" title={`Conservacion ${conservation}`} icon={conservation} color="var(--bw-danger)" />)
  } else {
    // taxon superior: cuantas especies descendientes con cada contenido
    if (flags.species > 0) items.push(<Seal key="sp" title="Especies descendientes" icon={'\u273F'} n={flags.species} color="var(--bw-accent)" />)
    if (flags.images > 0) items.push(<Seal key="i" title="Especies con imagenes" icon={'\u25A3'} n={flags.images} color="var(--bw-moss)" />)
    if (flags.sounds > 0) items.push(<Seal key="s" title="Especies con sonidos" icon={'\u266A'} n={flags.sounds} color="var(--bw-amber)" />)
    if (flags.descriptions > 0) items.push(<Seal key="d" title="Especies con descripcion" icon={'\u2261'} n={flags.descriptions} color="var(--bw-bark)" />)
  }
  if (!items.length) return null
  return <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap', marginTop: 6 }}>{items}</div>
}

export default function TaxonNode({ rank, nodeKey }) {
  const [data, setData] = useState(null)
  const [error, setError] = useState(null)
  const [loading, setLoading] = useState(true)
  const navigate = useNavigate()

  useEffect(() => {
    let alive = true
    setLoading(true); setError(null); setData(null)
    api.taxon(rank, nodeKey)
      .then((d) => { if (alive) { setData(d); setLoading(false) } })
      .catch((e) => { if (alive) { setError(e.message); setLoading(false) } })
    return () => { alive = false }
  }, [rank, nodeKey])

  const openChild = (child) => {
    if (child.rank === 'species') navigate(`/species/${child.key}`)
    else navigate(`/taxon/${child.rank}/${child.key}`)
  }

  return (
    <div className="bookworm-scope">
      <div className="bw-page">
        <button className="bw-btn" onClick={() => navigate(-1)}>{'\u2039'} volver</button>

        {loading && <p className="bw-muted" style={{ marginTop: 20 }}>Cargando...</p>}
        {error && <p style={{ marginTop: 20, color: 'var(--bw-danger)' }}>{error}</p>}

        {data && (
          <>
            <div style={{ marginTop: 18 }}>
              <div className="bw-rank">{RANK_ES[data.rank] || data.rank}</div>
              <h1>{data.name}</h1>
              <p className="bw-muted">
                {data.children.length} {RANK_ES[data.child_rank]?.toLowerCase() || data.child_rank}(s) directos
                {data.species_count ? ` - ${data.species_count} especies descendientes` : ''}
              </p>
            </div>

            {data.countries?.length > 0 && (
              <>
                <h2>Distribucion agregada</h2>
                <p className="bw-muted">Paises donde habita alguna especie de este grupo ({data.countries.length}).</p>
                <AggregateMap countries={data.countries} />
              </>
            )}

            <h2>{RANK_ES[data.child_rank] || data.child_rank}s</h2>
            <div className="bw-children">
              {data.children.map((c, i) => (
                <div key={i} className="bw-child" onClick={() => openChild(c)}>
                  <div className="bw-rank">{RANK_ES[c.rank] || c.rank}</div>
                  <div style={{ fontStyle: c.rank === 'species' ? 'italic' : 'normal' }}>{c.name}</div>
                  <ContentFlags flags={c.flags} isSpecies={c.rank === 'species'} conservation={c.conservation} />
                </div>
              ))}
            </div>
            {data.children.length === 0 && <p className="bw-muted">Sin hijos directos registrados.</p>}
          </>
        )}
      </div>
    </div>
  )
}