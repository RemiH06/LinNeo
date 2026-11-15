import WorldMap from './WorldMap'
import { ISO_CONTINENT } from '../geo/countries'

// Mapa al margen (lámina de taxón y ficha de especie): encuadra los países
// presentes. onSelect(iso) opcional; activeCountry/activeContinent resaltan.
export default function MarginMap({ countries, onSelect, activeCountry, activeContinent }) {
  const selected = (iso) => iso === activeCountry || (!activeCountry && activeContinent && ISO_CONTINENT[iso] === activeContinent)
  return (
    <div className="margin-map">
      <WorldMap highlight={countries} view="fit" aspect={1.5} onSelect={onSelect}
        focusable={!!onSelect && countries.length <= 25}
        classFor={(iso) => (selected(iso) ? 'sel' : (activeCountry || activeContinent) && countries.includes(iso) ? 'dim' : '')}
        label={`Distribución: ${countries.length} ${countries.length === 1 ? 'país' : 'países'}`} />
    </div>
  )
}
