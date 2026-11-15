import { useEffect, useState } from 'react'
import { MapContainer, GeoJSON } from 'react-leaflet'
import 'leaflet/dist/leaflet.css'
import { isoToName, ISO_CONTINENT } from './DistributionMap'

// Mapa al margen: países como vector sobre el papel, sin teselas.
// countries: ISO-2 presentes; onSelect(iso) opcional; activeCountry/activeContinent resaltan.
const GEO_URL = 'https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson'
// Este GeoJSON codifica Taiwán como CN-TW; GBIF usa TW.
const featureISO = (p) => (p['ISO3166-1-Alpha-2'] || p.ISO_A2 || p.iso_a2 || '').toUpperCase().replace(/^CN-TW$/, 'TW')

export default function MarginMap({ countries, onSelect, activeCountry, activeContinent }) {
  const [geo, setGeo] = useState(null)
  useEffect(() => {
    let alive = true
    fetch(GEO_URL).then((r) => r.json()).then((d) => { if (alive) setGeo(d) }).catch(() => {})
    return () => { alive = false }
  }, [])
  const present = new Set(countries)
  const style = (f) => {
    const iso = featureISO(f.properties)
    const selected = iso === activeCountry || (!activeCountry && activeContinent && ISO_CONTINENT[iso] === activeContinent)
    return { className: `country${present.has(iso) ? ' on' : ''}${selected ? ' sel' : ''}`, interactive: !!onSelect && present.has(iso) }
  }
  const onEach = (f, layer) => {
    const iso = featureISO(f.properties)
    if (!present.has(iso)) return
    layer.bindTooltip(isoToName(iso) || iso, { sticky: true, className: 'map-tip' })
    if (onSelect) layer.on('click', () => onSelect(iso))
  }
  return (
    <div className="margin-map">
      <MapContainer center={[20, 0]} zoom={0} minZoom={0} maxZoom={5} zoomSnap={0.25}
        attributionControl={false} scrollWheelZoom={false} worldCopyJump>
        {geo && <GeoJSON key={`${activeCountry}|${activeContinent}`} data={geo} style={style} onEachFeature={onEach} />}
      </MapContainer>
    </div>
  )
}
