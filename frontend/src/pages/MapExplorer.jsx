import { useEffect, useState } from 'react'
import { Link, useLocation, useNavigate } from 'react-router-dom'
import { useTheme } from '../theme/ThemeContext'
import { useSetKingdom } from '../theme/KingdomContext'
import { useKeyboardShortcuts } from '../hooks/useKeyboardShortcuts'
import { ISO_CONTINENT, CONTINENT_ES, isoToName, loadWorld } from '../geo/countries'
import SiteHeader from '../components/SiteHeader'
import WorldMap from '../components/WorldMap'
import '../theme/maps.css'

// Navegación geográfica en una sola ruta (/map y /map/<continente>): el mapa
// sigue montado y se acerca al continente; un país lleva a /map/country/<ISO>.
// Bajo el mapa, los mismos destinos como lista (teclado, lector de pantalla, dedo).

const CONTINENTS = ['Africa', 'North America', 'South America', 'Asia', 'Europe', 'Oceania', 'Antarctica']
const byName = (a, b) => (isoToName(a) || a).localeCompare(isoToName(b) || b, 'es')

export default function MapExplorer() {
  const location = useLocation()
  const navigate = useNavigate()
  const { toggle: toggleTheme } = useTheme()
  const rest = decodeURIComponent(location.pathname.replace(/^\/map\/?/, ''))
  const continent = CONTINENTS.includes(rest) ? rest : null
  const [hover, setHover] = useState(null)
  const [world, setWorld] = useState(null)
  useSetKingdom(null)
  useKeyboardShortcuts({ navigate, toggleTheme })
  useEffect(() => { loadWorld().then(setWorld).catch(() => {}) }, [])
  useEffect(() => setHover(null), [continent])

  const isos = world ? world.countries.map((c) => c.iso) : []
  const inContinent = continent ? isos.filter((iso) => ISO_CONTINENT[iso] === continent).sort(byName) : []
  const hoverContinent = !continent && hover ? ISO_CONTINENT[hover] : null
  const caption = continent
    ? (hover ? isoToName(hover) : `Elige un país de ${CONTINENT_ES[continent]} para ver sus especies.`)
    : (hoverContinent ? CONTINENT_ES[hoverContinent] : 'Elige un continente.')

  return (
    <div className="folio">
      <SiteHeader back={continent ? { to: '/map', label: 'El mundo' } : { to: '/', label: 'Inicio' }} />

      <header className="taxon-head map-head">
        <p className="taxon-rank">Geografía</p>
        <h1>{continent ? CONTINENT_ES[continent] : 'El mundo'}</h1>
        <p className="taxon-summary" aria-live="polite">{caption}</p>
      </header>

      <div className="map-plate">
        <WorldMap
          view={continent ? world?.continents[continent] || 'world' : 'world'}
          aspect={continent ? 1.6 : 2.05}
          highlight={continent ? inContinent : []}
          isInteractive={(iso) => (continent ? ISO_CONTINENT[iso] === continent : !!ISO_CONTINENT[iso])}
          classFor={(iso) => ((hoverContinent && ISO_CONTINENT[iso] === hoverContinent) || (continent && hover === iso) ? 'sel' : '')}
          onHover={setHover}
          onSelect={(iso) => (continent
            ? navigate(`/map/country/${iso}`)
            : navigate(`/map/${encodeURIComponent(ISO_CONTINENT[iso])}`))}
          label={continent ? `Mapa de ${CONTINENT_ES[continent]}` : 'Mapa del mundo por continentes'}
        />
      </div>

      {!continent && (
        <nav className="map-list" aria-label="Continentes">
          <ul className="map-continents">
            {CONTINENTS.map((c) => (
              <li key={c}>
                <Link to={`/map/${encodeURIComponent(c)}`}
                  onPointerEnter={() => setHover(isos.find((i) => ISO_CONTINENT[i] === c) || null)}
                  onFocus={() => setHover(isos.find((i) => ISO_CONTINENT[i] === c) || null)}>{CONTINENT_ES[c]}</Link>
              </li>
            ))}
          </ul>
        </nav>
      )}
      {continent && (
        <nav className="map-list" aria-label={`Países de ${CONTINENT_ES[continent]}`}>
          <h2 className="folio-h2">Países de {CONTINENT_ES[continent]}</h2>
          <ul className="map-countries">
            {inContinent.map((iso) => (
              <li key={iso}>
                <Link to={`/map/country/${iso}`} onPointerEnter={() => setHover(iso)} onFocus={() => setHover(iso)}>{isoToName(iso)}</Link>
              </li>
            ))}
          </ul>
        </nav>
      )}
    </div>
  )
}
