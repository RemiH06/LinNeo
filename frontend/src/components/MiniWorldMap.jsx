import WorldMap from './WorldMap'

// Mapa estático del póster (infografía): el mundo completo con los países
// donde vive el taxón. Sin interacción. `accent` pinta lo presente.
export default function MiniWorldMap({ countries, accent, height = 160 }) {
  return (
    <div className="ti-map" style={{ height, '--k-ink': accent }}>
      <WorldMap highlight={countries} label={`Presente en ${countries.length} países`} />
    </div>
  )
}
