import { useEffect, useState } from 'react'
import { MapContainer, TileLayer, GeoJSON } from 'react-leaflet'
import 'leaflet/dist/leaflet.css'
import { ESTABLISHMENT } from '../api/client'

const GEO_URL = 'https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson'

// ISO-A2 -> continente (mismo mapeo que el backend) para el sombreado tenue.
const ISO_CONTINENT = {
 AD:'Europe',AE:'Asia',AF:'Asia',AG:'North America',AI:'North America',AL:'Europe',AM:'Asia',AO:'Africa',AQ:'Antarctica',AR:'South America',AS:'Oceania',AT:'Europe',AU:'Oceania',AW:'North America',AX:'Europe',AZ:'Asia',
 BA:'Europe',BB:'North America',BD:'Asia',BE:'Europe',BF:'Africa',BG:'Europe',BH:'Asia',BI:'Africa',BJ:'Africa',BL:'North America',BM:'North America',BN:'Asia',BO:'South America',BQ:'North America',BR:'South America',BS:'North America',BT:'Asia',BV:'Antarctica',BW:'Africa',BY:'Europe',BZ:'North America',
 CA:'North America',CC:'Asia',CD:'Africa',CF:'Africa',CG:'Africa',CH:'Europe',CI:'Africa',CK:'Oceania',CL:'South America',CM:'Africa',CN:'Asia',CO:'South America',CR:'North America',CU:'North America',CV:'Africa',CW:'North America',CX:'Asia',CY:'Asia',CZ:'Europe',
 DE:'Europe',DJ:'Africa',DK:'Europe',DM:'North America',DO:'North America',DZ:'Africa',
 EC:'South America',EE:'Europe',EG:'Africa',EH:'Africa',ER:'Africa',ES:'Europe',ET:'Africa',
 FI:'Europe',FJ:'Oceania',FK:'South America',FM:'Oceania',FO:'Europe',FR:'Europe',
 GA:'Africa',GB:'Europe',GD:'North America',GE:'Asia',GF:'South America',GG:'Europe',GH:'Africa',GI:'Europe',GL:'North America',GM:'Africa',GN:'Africa',GP:'North America',GQ:'Africa',GR:'Europe',GS:'Antarctica',GT:'North America',GU:'Oceania',GW:'Africa',GY:'South America',
 HK:'Asia',HM:'Antarctica',HN:'North America',HR:'Europe',HT:'North America',HU:'Europe',
 ID:'Asia',IE:'Europe',IL:'Asia',IM:'Europe',IN:'Asia',IO:'Asia',IQ:'Asia',IR:'Asia',IS:'Europe',IT:'Europe',
 JE:'Europe',JM:'North America',JO:'Asia',JP:'Asia',
 KE:'Africa',KG:'Asia',KH:'Asia',KI:'Oceania',KM:'Africa',KN:'North America',KP:'Asia',KR:'Asia',KW:'Asia',KY:'North America',KZ:'Asia',
 LA:'Asia',LB:'Asia',LC:'North America',LI:'Europe',LK:'Asia',LR:'Africa',LS:'Africa',LT:'Europe',LU:'Europe',LV:'Europe',LY:'Africa',
 MA:'Africa',MC:'Europe',MD:'Europe',ME:'Europe',MF:'North America',MG:'Africa',MH:'Oceania',MK:'Europe',ML:'Africa',MM:'Asia',MN:'Asia',MO:'Asia',MP:'Oceania',MQ:'North America',MR:'Africa',MS:'North America',MT:'Europe',MU:'Africa',MV:'Asia',MW:'Africa',MX:'North America',MY:'Asia',MZ:'Africa',
 NA:'Africa',NC:'Oceania',NE:'Africa',NF:'Oceania',NG:'Africa',NI:'North America',NL:'Europe',NO:'Europe',NP:'Asia',NR:'Oceania',NU:'Oceania',NZ:'Oceania',
 OM:'Asia',
 PA:'North America',PE:'South America',PF:'Oceania',PG:'Oceania',PH:'Asia',PK:'Asia',PL:'Europe',PM:'North America',PN:'Oceania',PR:'North America',PS:'Asia',PT:'Europe',PW:'Oceania',PY:'South America',
 QA:'Asia',
 RE:'Africa',RO:'Europe',RS:'Europe',RU:'Europe',RW:'Africa',
 SA:'Asia',SB:'Oceania',SC:'Africa',SD:'Africa',SE:'Europe',SG:'Asia',SH:'Africa',SI:'Europe',SJ:'Europe',SK:'Europe',SL:'Africa',SM:'Europe',SN:'Africa',SO:'Africa',SR:'South America',SS:'Africa',ST:'Africa',SV:'North America',SX:'North America',SY:'Asia',SZ:'Africa',
 TC:'North America',TD:'Africa',TF:'Antarctica',TG:'Africa',TH:'Asia',TJ:'Asia',TK:'Oceania',TL:'Asia',TM:'Asia',TN:'Africa',TO:'Oceania',TR:'Asia',TT:'North America',TV:'Oceania',TW:'Asia',TZ:'Africa',
 UA:'Europe',UG:'Africa',UM:'Oceania',US:'North America',UY:'South America',UZ:'Asia',
 VA:'Europe',VC:'North America',VE:'South America',VG:'North America',VI:'North America',VN:'Asia',VU:'Oceania',
 WF:'Oceania',WS:'Oceania',XK:'Europe',YE:'Asia',YT:'Africa',ZA:'Africa',ZM:'Africa',ZW:'Africa',
}

let displayNames = null
try { displayNames = new Intl.DisplayNames(['es'], { type: 'region' }) } catch { /* */ }
export function isoToName(code) {
  if (!code) return code
  try { return displayNames ? displayNames.of(code) || code : code } catch { return code }
}

function featureISO(props) {
  return (props['ISO3166-1-Alpha-2'] || props.ISO_A2 || props.iso_a2 || props.WB_A2 || '').toUpperCase()
}

/*
  distribution: [{ country, establishment_means, conservation_code, ... }]
  continents:   ['Africa', 'Asia', ...]  (derivados de los paises por el backend)

  - Paises con dato: color solido segun establishment_means.
  - Paises sin dato pero en un continente presente: sombreado MUY tenue de acento.
*/
export default function DistributionMap({ distribution = [], continents = [] }) {
  const [geo, setGeo] = useState(null)
  const [error, setError] = useState(false)
  const [active, setActive] = useState(() => new Set(Object.keys(ESTABLISHMENT)))
  const [showContinents, setShowContinents] = useState(true)

  const statusByCode = {}
  for (const d of distribution) {
    const code = String(d.country || '').toUpperCase().trim()
    if (code) statusByCode[code] = d.establishment_means || ''
  }
  const present = new Set(Object.values(statusByCode))
  const continentSet = new Set(continents)

  useEffect(() => {
    let alive = true
    fetch(GEO_URL).then((r) => r.json()).then((d) => { if (alive) setGeo(d) })
      .catch(() => { if (alive) setError(true) })
    return () => { alive = false }
  }, [])

  const styleFn = (feature) => {
    const code = featureISO(feature.properties)
    const status = statusByCode[code]
    const hasData = code in statusByCode && active.has(status)
    if (hasData) {
      const color = (ESTABLISHMENT[status] || ESTABLISHMENT['']).color
      return { fillColor: color, fillOpacity: 0.85, color, weight: 1.2 }
    }
    // sombreado tenue: pais sin dato pero en un continente donde la especie esta presente
    if (showContinents && continentSet.has(ISO_CONTINENT[code])) {
      return { fillColor: '#00E8D8', fillOpacity: 0.12, color: '#33415a', weight: 0.3 }
    }
    return { fillColor: '#1a2535', fillOpacity: 0.25, color: '#33415a', weight: 0.4 }
  }

  const toggle = (status) => {
    setActive((prev) => {
      const next = new Set(prev)
      next.has(status) ? next.delete(status) : next.add(status)
      return next
    })
  }

  if (error) return <p className="muted">No se pudo cargar el mapa.</p>

  const repaintKey = JSON.stringify([...active]) + distribution.length + showContinents

  return (
    <div>
      <div style={{ border: '1px solid var(--border)', borderRadius: 'var(--radius)', overflow: 'hidden' }}>
        <MapContainer center={[20, 0]} zoom={1} minZoom={1}
          style={{ height: 300, width: '100%', background: '#0a1018' }}
          attributionControl={false} zoomControl={true} scrollWheelZoom={false} worldCopyJump={true}>
          <TileLayer url="https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png"
            subdomains="abcd" maxZoom={6} />
          {geo && <GeoJSON key={repaintKey} data={geo} style={styleFn} />}
        </MapContainer>
      </div>

      {/* Leyenda + filtro de establishment_means */}
      <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', marginTop: 8, alignItems: 'center' }}>
        {Object.keys(ESTABLISHMENT).filter((s) => present.has(s)).map((s) => {
          const on = active.has(s)
          const { color, label } = ESTABLISHMENT[s]
          return (
            <button key={s || 'none'} onClick={() => toggle(s)}
              style={{
                display: 'flex', alignItems: 'center', gap: 5, cursor: 'pointer',
                fontFamily: 'var(--mono)', fontSize: 9, padding: '3px 8px',
                border: `1px solid ${on ? color : 'var(--border)'}`,
                borderRadius: 'var(--radius)', background: 'var(--bg2)',
                color: on ? 'var(--text)' : 'var(--text2)', opacity: on ? 1 : 0.5,
                textTransform: 'uppercase', letterSpacing: '.04em',
              }}>
              <span style={{ width: 9, height: 9, borderRadius: 2, background: color, display: 'inline-block' }} />
              {label}
            </button>
          )
        })}
        {/* toggle del sombreado continental */}
        {continents.length > 0 && (
          <button onClick={() => setShowContinents((v) => !v)}
            style={{
              display: 'flex', alignItems: 'center', gap: 5, cursor: 'pointer',
              fontFamily: 'var(--mono)', fontSize: 9, padding: '3px 8px',
              border: `1px dashed ${showContinents ? 'var(--accent)' : 'var(--border)'}`,
              borderRadius: 'var(--radius)', background: 'var(--bg2)',
              color: showContinents ? 'var(--text)' : 'var(--text2)', opacity: showContinents ? 1 : 0.5,
              textTransform: 'uppercase', letterSpacing: '.04em',
            }}>
            <span style={{ width: 9, height: 9, borderRadius: 2, background: 'var(--accent)', opacity: 0.3, display: 'inline-block' }} />
            Continente
          </button>
        )}
      </div>
    </div>
  )
}