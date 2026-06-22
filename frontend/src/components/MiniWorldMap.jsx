import { useEffect, useState } from 'react'
import { MapContainer, TileLayer, GeoJSON } from 'react-leaflet'
import 'leaflet/dist/leaflet.css'

/*
  MiniWorldMap -- version estatica, sin interaccion, del mapa agregado de
  paises (AggregateMap de TaxonNode), pensada para el poster/infografia:
  solo pinta los paises donde habita el taxon, sin click ni hover especial.
*/
const GEO_URL = 'https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson'

function featureISO(props) {
  return (props['ISO3166-1-Alpha-2'] || props.ISO_A2 || props.iso_a2 || props.WB_A2 || '').toUpperCase()
}

export default function MiniWorldMap({ countries, accent = '#4A6040', height = 160 }) {
  const [geo, setGeo] = useState(null)
  const targets = new Set((countries || []).map((c) => String(c).toUpperCase()))

  useEffect(() => {
    let alive = true
    fetch(GEO_URL).then((r) => r.json()).then((d) => { if (alive) setGeo(d) }).catch(() => {})
    return () => { alive = false }
  }, [])

  const styleFn = (f) => {
    const active = targets.has(featureISO(f.properties))
    return {
      fillColor: active ? accent : '#cabfa0', fillOpacity: active ? 0.85 : 0.12,
      color: active ? accent : '#b8a888', weight: active ? 0.6 : 0.2,
    }
  }

  return (
    <div className="ti-map" style={{ height }}>
      <MapContainer center={[20, 0]} zoom={1} minZoom={1} style={{ height: '100%', width: '100%', background: '#cec4ac' }}
        attributionControl={false} zoomControl={false} scrollWheelZoom={false} dragging={false}
        doubleClickZoom={false} touchZoom={false} boxZoom={false} keyboard={false} worldCopyJump={true}>
        <TileLayer url="https://{s}.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}{r}.png" subdomains="abcd" maxZoom={4} />
        {geo && <GeoJSON data={geo} style={styleFn} />}
      </MapContainer>
    </div>
  )
}