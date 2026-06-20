import { useEffect, useState, useRef } from 'react'
import { useNavigate, useLocation } from 'react-router-dom'
import { MapContainer, TileLayer, GeoJSON, useMap } from 'react-leaflet'
import 'leaflet/dist/leaflet.css'
import { useTheme } from '../theme/ThemeContext'
import { useSetKingdom } from '../theme/KingdomContext'
import { useKeyboardShortcuts } from '../hooks/useKeyboardShortcuts'
import { useElapsedTimer, formatElapsed } from '../hooks/useElapsedTimer'
import LoadingSpinner from '../components/LoadingSpinner'
import { ISO_CONTINENT, isoToName } from '../components/DistributionMap'
import LinNeoLogo from '../components/LinNeoLogo'
import '../theme/bookworm.css'

/*
  MapExplorer -- mapa persistente, montado en la ruta wildcard /map/*.
  Un solo <MapContainer> sobrevive el cambio de URL (/map -> /map/Asia);
  el "modo" (mundo vs continente) se lee de la URL via useLocation en vez de
  useParams, porque la ruta es un wildcard, no un parametro declarado.
  Al entrar a un continente la camara hace flyToBounds (efecto de "viaje") y
  el estilo/clic de los paises cambia de "elige un continente" a "elige un
  pais de este continente". Clic en pais navega a /map/country/:code (ruta
  aparte, fuera de este wildcard, con su propia vista sin mapa).
*/

const GEO_URL = 'https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson'

function featureISO(props) {
  return (props['ISO3166-1-Alpha-2'] || props.ISO_A2 || props.iso_a2 || props.WB_A2 || '').toUpperCase()
}

const CONTINENT_COLOR = {
  Africa: '#B08030', Asia: '#8A6040', Europe: '#4A6040', 'North America': '#6A8050',
  'South America': '#C89040', Oceania: '#4A8090', Antarctica: '#8898A0',
}

// Bounds aproximados [[south, west], [north, east]] para el flyToBounds.
const CONTINENT_BOUNDS = {
  Africa: [[-35, -20], [38, 52]],
  Asia: [[-12, 25], [78, 180]],
  Europe: [[34, -25], [72, 45]],
  'North America': [[5, -170], [83, -50]],
  'South America': [[-57, -82], [13, -33]],
  Oceania: [[-50, 110], [22, 180]],
  Antarctica: [[-90, -180], [-60, 180]],
}
const WORLD_CENTER = [20, 0]
const WORLD_ZOOM = 2

// Componente hijo (vive DENTRO de MapContainer) que controla la camara.
// useMap() solo funciona dentro del contexto de react-leaflet.
function CameraController({ continent }) {
  const map = useMap()
  const prevContinent = useRef(undefined)

  useEffect(() => {
    if (prevContinent.current === continent) return
    prevContinent.current = continent
    if (continent && CONTINENT_BOUNDS[continent]) {
      map.flyToBounds(CONTINENT_BOUNDS[continent], { duration: 1.1, padding: [20, 20] })
    } else {
      map.flyTo(WORLD_CENTER, WORLD_ZOOM, { duration: 1.1 })
    }
  }, [continent, map])

  return null
}

export default function MapExplorer() {
  const location = useLocation()
  // pathname es '/map' (vista mundo) o '/map/Nombre%20Del%20Continente'.
  // decodeURIComponent porque navigate() codifica el nombre al construir el link.
  const rest = location.pathname.replace(/^\/map\/?/, '')
  const continent = rest ? decodeURIComponent(rest) : undefined

  const navigate = useNavigate()
  const { toggle: toggleTheme } = useTheme()
  const [geo, setGeo] = useState(null)
  const [error, setError] = useState(false)
  const timer = useElapsedTimer()
  const [hoverKey, setHoverKey] = useState(null) // continente (vista mundo) o ISO de pais (vista continente)

  useSetKingdom(null)
  useKeyboardShortcuts({ navigate, toggleTheme })

  useEffect(() => {
    let alive = true
    timer.start()
    fetch(GEO_URL).then((r) => r.json()).then((d) => { if (alive) { setGeo(d); timer.stop() } })
      .catch(() => { if (alive) { setError(true); timer.stop() } })
    return () => { alive = false }
  }, [])

  // limpiar el hover al cambiar de modo (mundo <-> continente) para que no
  // quede una region resaltada de la vista anterior
  useEffect(() => { setHoverKey(null) }, [continent])

  function continentOf(feature) {
    return ISO_CONTINENT[featureISO(feature.properties)]
  }

  const styleFn = (feature) => {
    const iso = featureISO(feature.properties)
    const cont = continentOf(feature)

    if (!continent) {
      // vista mundo: colorear por continente, clicable por continente
      const color = CONTINENT_COLOR[cont] || '#9A9080'
      return { fillColor: color, fillOpacity: 0.55, color: '#5A4E38', weight: 0.5 }
    }
    // vista continente: solo los paises de ESTE continente son clicables/destacados
    const belongs = cont === continent
    return {
      fillColor: belongs ? '#4A6040' : '#cabfa0',
      fillOpacity: belongs ? 0.65 : 0.12,
      color: belongs ? '#6A8050' : '#b8a888',
      weight: belongs ? 0.8 : 0.3,
    }
  }

  // estilos de hover, aplicados directo a la capa de leaflet (layer.setStyle)
  // sin pasar por React state -- evita recrear el GeoJSON completo en cada
  // mouseover/mouseout, que seria muy costoso y rompe el flyTo en curso.
  const HOVER_WORLD = { fillOpacity: 0.85, color: '#2A2010', weight: 1.4 }
  const HOVER_COUNTRY = { fillOpacity: 0.9, color: '#2A2010', weight: 1.6 }

  const onEach = (feature, layer) => {
    const iso = featureISO(feature.properties)
    const cont = continentOf(feature)
    if (!continent) {
      if (!cont) return
      const base = styleFn(feature)
      layer.on('click', () => navigate(`/map/${encodeURIComponent(cont)}`))
      layer.on('mouseover', () => { layer.setStyle(HOVER_WORLD); setHoverKey(cont) })
      layer.on('mouseout', () => { layer.setStyle(base); setHoverKey(null) })
    } else {
      if (cont !== continent) return
      const base = styleFn(feature)
      layer.on('click', () => navigate(`/map/country/${encodeURIComponent(iso)}`))
      layer.on('mouseover', () => { layer.setStyle(HOVER_COUNTRY); setHoverKey(iso) })
      layer.on('mouseout', () => { layer.setStyle(base); setHoverKey(null) })
    }
  }

  return (
    <div className="bookworm-scope">
      <div className="bw-page">
        <div className="bw-topbar">
          <LinNeoLogo />
          {continent ? (
            <button className="bw-btn" onClick={() => navigate('/map')}>{'\u2039'} mapa mundial</button>
          ) : (
            <button className="bw-btn" onClick={() => navigate('/')}>{'\u2039'} volver a Shui</button>
          )}
        </div>

        <div className="bw-header">
          <div className="bw-header-title">
            <div className="bw-rank">Navegacion geografica</div>
            <h1>{continent || 'Mapa mundial'}</h1>
            <p className="bw-muted">
              {continent
                ? <>Clic en un pais para ver sus especies.{hoverKey && <> · {isoToName(hoverKey)}</>}</>
                : <>Clic en un continente para explorar sus paises.{hoverKey && <> · {hoverKey}</>}</>
              }
            </p>
          </div>
        </div>

        {error && <p style={{ color: 'var(--bw-danger)' }}>No se pudo cargar el mapa.</p>}

        <div className="bw-map" style={{ marginTop: 12, position: 'relative' }}>
          {!geo && !error && (
            <div className="ln-map-overlay">
              <LoadingSpinner inline={false} timeText={timer.elapsedMs != null ? formatElapsed(timer.elapsedMs) : null} />
            </div>
          )}
          <MapContainer center={WORLD_CENTER} zoom={WORLD_ZOOM} minZoom={1.5}
            style={{ height: 560, width: '100%', background: '#cec4ac' }}
            attributionControl={false} zoomControl={true} scrollWheelZoom={true} worldCopyJump={true}>
            <TileLayer url="https://{s}.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}{r}.png" subdomains="abcd" maxZoom={6} />
            <CameraController continent={continent} />
            {geo && (
              <GeoJSON
                key={continent || 'world'}
                data={geo}
                style={styleFn}
                onEachFeature={onEach}
              />
            )}
          </MapContainer>
        </div>
      </div>
    </div>
  )
}