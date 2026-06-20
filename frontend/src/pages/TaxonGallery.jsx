import { useEffect, useState, useMemo } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import { api } from '../api/client'
import { useTheme } from '../theme/ThemeContext'
import { kingdomStyleVars, kingdomAccent } from '../theme/kingdomColor'
import { useSetKingdom } from '../theme/KingdomContext'
import { useKeyboardShortcuts } from '../hooks/useKeyboardShortcuts'
import { useElapsedTimer, formatElapsed } from '../hooks/useElapsedTimer'
import LoadingSpinner from '../components/LoadingSpinner'
import LinNeoLogo from '../components/LinNeoLogo'
import { packTetris } from '../theme/tetrisPacker'
import '../theme/bookworm.css'

/*
  TaxonGallery -- mosaico tipo tetris de las especies con imagen de un taxon.
  Cada especie es UN bloque (no una imagen suelta): 1 imagen -> intenta 3x3,
  2x2 o 1x1 segun espacio; 2-5 imagenes -> formas tetris/L de ese tamano.
  El packing real (bin-packing simple) vive en theme/tetrisPacker.js.
*/

const CELL_PX = 90 // tamano base de celda en px (se ajusta responsivo via CSS)
const GAP_PX = 6

export default function TaxonGallery() {
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
    api.taxon(rank, key)
      .then((d) => { if (alive) { setData(d); setLoading(false); timer.stop() } })
      .catch((e) => { if (alive) { setError(e.message); setLoading(false); timer.stop() } })
    return () => { alive = false }
  }, [rank, key])

  // especies con al menos 1 imagen, con su conteo real (tope 5, ya viene asi del backend)
  const withImages = useMemo(() => {
    if (!data?.children) return []
    return data.children
      .filter((c) => c.rank === 'species' && (c.images?.length > 0 || c.image))
      .map((c) => ({
        ...c,
        imageCount: c.images?.length || (c.image ? 1 : 0),
        imageList: c.images?.length > 0 ? c.images : (c.image ? [c.image] : []),
      }))
  }, [data])

  const { placements, gridHeight, cols } = useMemo(
    () => packTetris(withImages, 5),
    [withImages]
  )

  return (
    <div className="bookworm-scope" style={kingdomStyleVars(data?.kingdom, dark)}>
      <div className="bw-page">
        <div className="bw-topbar">
          <LinNeoLogo />
          <button className="bw-btn" onClick={() => navigate(-1)}>{'\u2039'} volver</button>
        </div>

        {loading && (
          <div style={{ marginTop: 20 }}>
            <LoadingSpinner inline={false} timeText={timer.elapsedMs != null ? formatElapsed(timer.elapsedMs) : null} />
          </div>
        )}
        {error && <p style={{ marginTop: 20, color: 'var(--bw-danger)' }}>{error}</p>}

        {data && (
          <>
            <div className="bw-header">
              <div className="bw-header-title">
                <div className="bw-rank">Galeria · {RANK_LABEL(data.rank)}</div>
                <h1>{data.name}</h1>
                <p className="bw-muted">{withImages.length} especie(s) con imagen.</p>
              </div>
            </div>

            {withImages.length === 0 ? (
              <p className="bw-muted" style={{ marginTop: 20 }}>Ninguna especie de este grupo tiene imagenes.</p>
            ) : (
              <div
                className="tg-grid"
                style={{
                  gridTemplateColumns: `repeat(${cols}, ${CELL_PX}px)`,
                  gridTemplateRows: `repeat(${gridHeight}, ${CELL_PX}px)`,
                  gap: GAP_PX,
                }}
              >
                {placements.map(({ item, x, y, shape }, i) => {
                  const w = Math.max(...shape.map(([dx]) => dx)) + 1
                  const h = Math.max(...shape.map(([, dy]) => dy)) + 1
                  const { accent } = kingdomAccent(item.kingdom, dark)
                  return (
                    <a
                      key={item.key || i}
                      className="tg-block"
                      href={`/species/${item.key}`}
                      onClick={(e) => { e.preventDefault(); navigate(`/species/${item.key}`) }}
                      title={item.name}
                      style={{
                        gridColumn: `${x + 1} / span ${w}`,
                        gridRow: `${y + 1} / span ${h}`,
                        borderColor: accent,
                      }}
                    >
                      <img src={item.imageList[0]} alt={item.name} loading="lazy" />
                      <span className="tg-block-label">{item.name}</span>
                      {item.imageList.length > 1 && (
                        <span className="tg-block-count">+{item.imageList.length - 1}</span>
                      )}
                    </a>
                  )
                })}
              </div>
            )}
          </>
        )}
      </div>
    </div>
  )
}

function RANK_LABEL(rank) {
  const map = {
    domain: 'Dominio', kingdom: 'Reino', phylum: 'Filo', class: 'Clase', order: 'Orden',
    family: 'Familia', genus: 'Genero', species: 'Especie',
  }
  return map[rank] || rank
}