import { useEffect, useState } from 'react'
import { MapContainer, TileLayer, GeoJSON } from 'react-leaflet'
import 'leaflet/dist/leaflet.css'

const GEO_URL = 'https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson'

// Conversor de codigo ISO-A2 -> nombre legible (en espanol), nativo del navegador.
let displayNames = null
try { displayNames = new Intl.DisplayNames(['es'], { type: 'region' }) } catch { /* no soportado */ }
export function isoToName(code) {
  if (!code) return code
  try { return displayNames ? displayNames.of(code) || code : code } catch { return code }
}

// Extrae el codigo ISO-A2 de una feature del GeoJSON (prueba varias propiedades).
function featureISO(props) {
  return (props['ISO3166-1-Alpha-2'] || props.ISO_A2 || props.iso_a2 || props.WB_A2 || '').toUpperCase()
}

export default function DistributionMap({ countries = [] }) {
  const [geo, setGeo] = useState(null)
  const [error, setError] = useState(false)
  // set de codigos ISO en mayuscula
  const targets = new Set(countries.map((c) => String(c).toUpperCase().trim()))

  useEffect(() => {
    let alive = true
    fetch(GEO_URL)
      .then((r) => r.json())
      .then((d) => {
        if (!alive) return
        setGeo(d)
        const geoCodes = new Set(d.features.map((f) => featureISO(f.properties)))
        const missing = [...targets].filter((c) => !geoCodes.has(c))
        if (missing.length) console.warn('[mapa] codigos sin coincidencia en GeoJSON:', missing)
      })
      .catch(() => { if (alive) setError(true) })
    return () => { alive = false }
  }, [countries.join(',')])

  const styleFn = (feature) => {
    const code = featureISO(feature.properties)
    const active = targets.has(code)
    return {
      fillColor: active ? '#00E8D8' : '#1a2535',
      fillOpacity: active ? 0.85 : 0.25,
      color: active ? '#00F0E0' : '#33415a',
      weight: active ? 1.2 : 0.4,
    }
  }

  if (error) {
    return <p className="muted">No se pudo cargar el mapa.</p>
  }

  return (
    <div style={{ border: '1px solid var(--border)', borderRadius: 'var(--radius)', overflow: 'hidden' }}>
      <MapContainer
        center={[20, 0]}
        zoom={1}
        minZoom={1}
        style={{ height: 300, width: '100%', background: '#0a1018' }}
        attributionControl={false}
        zoomControl={true}
        scrollWheelZoom={false}
        worldCopyJump={true}
      >
        <TileLayer
          url="https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png"
          subdomains="abcd"
          maxZoom={6}
        />
        {geo && <GeoJSON key={countries.join(',')} data={geo} style={styleFn} />}
      </MapContainer>
    </div>
  )
}