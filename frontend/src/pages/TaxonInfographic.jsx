import { useEffect, useState } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import { api, IUCN } from '../api/client'
import { useTheme } from '../theme/ThemeContext'
import { kingdomStyleVars, kingdomAccent } from '../theme/kingdomColor'
import { useSetKingdom } from '../theme/KingdomContext'
import { useKeyboardShortcuts } from '../hooks/useKeyboardShortcuts'
import { useElapsedTimer, formatElapsed } from '../hooks/useElapsedTimer'
import LoadingSpinner from '../components/LoadingSpinner'
import LinNeoLogo from '../components/LinNeoLogo'
import MiniWorldMap from '../components/MiniWorldMap'
import '../theme/bookworm.css'

/*
  TaxonInfographic -- poster de una sola pantalla, sin scroll: stats
  agregadas del taxon (especies, paises, imagenes, sonidos, descripciones),
  mini-mapa, desglose de conservacion, desglose por reino (si aplica), y
  hasta 3 especies destacadas (mas tipos de contenido).
*/

const RANK_ES = {
  domain: 'Dominio', kingdom: 'Reino', phylum: 'Filo', class: 'Clase', order: 'Orden',
  family: 'Familia', genus: 'Genero', species: 'Especie',
}

export default function TaxonInfographic() {
  const { rank, key } = useParams()
  const navigate = useNavigate()
  const { dark, toggle: toggleTheme } = useTheme()
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const timer = useElapsedTimer()

  useSetKingdom(data?.kingdom)
  useKeyboardShortcuts({ navigate, toggleTheme })

  useEffect(() => {
    let alive = true
    setLoading(true); setError(null); setData(null)
    timer.start()
    api.taxonInfographic(rank, key)
      .then((d) => { if (alive) { setData(d); setLoading(false); timer.stop() } })
      .catch((e) => { if (alive) { setError(e.message); setLoading(false); timer.stop() } })
    return () => { alive = false }
  }, [rank, key])

  if (loading) {
    return (
      <div className="bookworm-scope">
        <div className="bw-page">
          <LoadingSpinner inline={false} timeText={timer.elapsedMs != null ? formatElapsed(timer.elapsedMs) : null} />
        </div>
      </div>
    )
  }
  if (error) {
    return (
      <div className="bookworm-scope">
        <div className="bw-page">
          <p style={{ color: 'var(--bw-danger)' }}>{error}</p>
        </div>
      </div>
    )
  }
  if (!data) return null

  const { accent } = kingdomAccent(data.kingdom, dark)
  const totalConservation = data.conservation.reduce((s, c) => s + c.count, 0)
  const maxConservation = Math.max(1, ...data.conservation.map((c) => c.count))
  const totalByKingdom = data.by_kingdom.reduce((s, k) => s + k.count, 0)

  return (
    <div className="bookworm-scope ti-poster-scope" style={kingdomStyleVars(data.kingdom, dark)}>
      <div className="bw-page ti-poster-page">
        <div className="bw-topbar">
          <LinNeoLogo />
          <button className="bw-btn" onClick={() => navigate(-1)}>{'\u2039'} volver</button>
        </div>

        <div className="ti-poster">
          {/* Encabezado */}
          <div className="ti-head">
            <div className="bw-rank">Infografia · {RANK_ES[data.rank] || data.rank}</div>
            <h1>{data.name}</h1>
            {data.kingdom && <div className="ti-kingdom-tag" style={{ borderColor: accent, color: accent }}>{data.kingdom}</div>}
          </div>

          {/* Grid principal: stats + mapa a la izquierda, conservacion +
              reino a la derecha, destacadas abajo a lo ancho */}
          <div className="ti-grid">
            <div className="ti-col">
              <div className="ti-stats">
                <Stat value={data.species_count} label="especies" />
                <Stat value={data.countries.length} label="paises" />
                <Stat value={data.images_count} label="imagenes" />
                <Stat value={data.sounds_count} label="sonidos" />
                <Stat value={data.descriptions_count} label="descripciones" />
              </div>
              {data.countries.length > 0 && (
                <MiniWorldMap countries={data.countries} accent={accent} height={170} />
              )}
            </div>

            <div className="ti-col">
              {data.conservation.length > 0 && (
                <div className="ti-block">
                  <h3>Estado de conservacion</h3>
                  <div className="ti-bars">
                    {data.conservation.map((c) => {
                      const info = IUCN[c.code] || { label: c.code, color: 'var(--bw-text2)' }
                      return (
                        <div key={c.code} className="ti-bar-row">
                          <span className="ti-bar-code" style={{ color: info.color }}>{c.code}</span>
                          <div className="ti-bar-track">
                            <div className="ti-bar-fill" style={{ width: `${(c.count / maxConservation) * 100}%`, background: info.color }} />
                          </div>
                          <span className="ti-bar-n">{c.count}</span>
                        </div>
                      )
                    })}
                  </div>
                  <p className="bw-muted ti-small">{totalConservation} especie(s) evaluada(s).</p>
                </div>
              )}

              {data.by_kingdom.length > 1 && (
                <div className="ti-block">
                  <h3>Por reino</h3>
                  <div className="ti-bars">
                    {data.by_kingdom.map((k) => {
                      const { accent: kAccent } = kingdomAccent(k.kingdom, dark)
                      return (
                        <div key={k.kingdom} className="ti-bar-row">
                          <span className="ti-bar-code" style={{ color: kAccent, minWidth: 70 }}>{k.kingdom}</span>
                          <div className="ti-bar-track">
                            <div className="ti-bar-fill" style={{ width: `${(k.count / totalByKingdom) * 100}%`, background: kAccent }} />
                          </div>
                          <span className="ti-bar-n">{k.count}</span>
                        </div>
                      )
                    })}
                  </div>
                </div>
              )}
            </div>
          </div>

          {/* Especies destacadas */}
          {data.featured.length > 0 && (
            <div className="ti-featured">
              <h3>Especies destacadas</h3>
              <div className="ti-featured-row">
                {data.featured.map((f) => {
                  const { accent: fAccent } = kingdomAccent(f.kingdom, dark)
                  return (
                    <a key={f.key} className="ti-featured-card" href={`/species/${f.key}`}
                      onClick={(e) => { e.preventDefault(); navigate(`/species/${f.key}`) }}>
                      <div className="ti-featured-disc" style={{ borderColor: fAccent }}>
                        {f.image
                          ? <img src={f.image} alt={f.name} loading="lazy" />
                          : <span className="ti-featured-icon">{'\u273F'}</span>}
                      </div>
                      <div className="ti-featured-name">{f.name}</div>
                      {f.common_names?.length > 0 && (
                        <div className="bw-muted ti-small">{f.common_names[0]}</div>
                      )}
                    </a>
                  )
                })}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

function Stat({ value, label }) {
  return (
    <div className="ti-stat">
      <div className="ti-stat-val">{(value || 0).toLocaleString()}</div>
      <div className="ti-stat-lbl">{label}</div>
    </div>
  )
}