import { useEffect, useState } from 'react'
import { MapContainer, TileLayer, GeoJSON } from 'react-leaflet'
import 'leaflet/dist/leaflet.css'

// GeoJSON mundial ligero (paises). Fuente publica de uso comun.
const GEO_URL = 'https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson'

// Normaliza nombres para comparar (quita acentos, minusculas, sinonimos comunes).
function norm(s) {
  if (!s) return ''
  let v = s.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').trim()
  const alias = {
    'united states of america': 'united states',
    'usa': 'united states',
    'russian federation': 'russia',
    'czechia': 'czech republic',
    'republic of korea': 'south korea',
    "democratic people's republic of korea": 'north korea',
    'united republic of tanzania': 'tanzania',
    'bolivia (plurinational state of)': 'bolivia',
    'viet nam': 'vietnam',
    'syrian arab republic': 'syria',
    'iran (islamic republic of)': 'iran',
    'venezuela (bolivarian republic of)': 'venezuela',
    'lao people\u2019s democratic republic': 'laos',
  }
  return alias[v] || v
}

export default function DistributionMap({ countries = [] }) {
  const [geo, setGeo] = useState(null)
  const [error, setError] = useState(false)
  const targets = new Set(countries.map(norm))

  useEffect(() => {
    let alive = true
    fetch(GEO_URL)
      .then((r) => r.json())
      .then((d) => { if (alive) setGeo(d) })
      .catch(() => { if (alive) setError(true) })
    return () => { alive = false }
  }, [])

  const styleFn = (feature) => {
    const name = norm(feature.properties.ADMIN || feature.properties.name)
    const active = targets.has(name)
    return {
      fillColor: active ? 'var(--accent)' : 'transparent',
      fillOpacity: active ? 0.55 : 0,
      color: active ? 'var(--accent)' : 'var(--border)',
      weight: active ? 1 : 0.4,
    }
  }

  if (error) {
    return <p className="muted">No se pudo cargar el mapa. Paises: {countries.join(', ')}</p>
  }

  return (
    <div style={{ border: '1px solid var(--border)', borderRadius: 'var(--radius)', overflow: 'hidden' }}>
      <MapContainer
        center={[20, 0]}
        zoom={1}
        style={{ height: 280, width: '100%', background: 'var(--bg3)' }}
        attributionControl={false}
        zoomControl={false}
        scrollWheelZoom={false}
        dragging={true}
      >
        {/* Sin tiles de fondo para un look limpio tipo esquema; solo los poligonos */}
        {geo && <GeoJSON key={countries.join(',')} data={geo} style={styleFn} />}
      </MapContainer>
    </div>
  )
}