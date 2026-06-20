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

const CELL_PX = 90 // tamano base de celda en px
const GAP_PX = 6
const TARGET_WIDTH_RATIO = 0.7 // ~70% del ancho de viewport
const MIN_COLS = 5 // piso para que en pantallas chicas no colapse a muy pocas columnas

// Cuantas columnas de CELL_PX (+ gap) caben en el 70% del ancho de viewport
// actual. Se recalcula en cada resize para que la galeria respire igual sin
// importar el tamano real de pantalla del usuario.
function useDynamicColumns() {
  const [cols, setCols] = useState(() => computeCols())
  useEffect(() => {
    function onResize() { setCols(computeCols()) }
    window.addEventListener('resize', onResize)
    return () => window.removeEventListener('resize', onResize)
  }, [])
  return cols
}
function computeCols() {
  if (typeof window === 'undefined') return MIN_COLS
  const targetWidth = window.innerWidth * TARGET_WIDTH_RATIO
  const perCol = CELL_PX + GAP_PX
  const fit = Math.floor(targetWidth / perCol)
  return Math.max(MIN_COLS, fit)
}

export default function TaxonGallery() {
  const { rank, key } = useParams()
  const navigate = useNavigate()
  const { dark, toggle: toggleTheme } = useTheme()
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const timer = useElapsedTimer()
  const dynamicCols = useDynamicColumns()

  useSetKingdom(data?.kingdom)
  useKeyboardShortcuts({ navigate, toggleTheme })

  useEffect(() => {
    let alive = true
    setLoading(true); setError(null); setData(null)
    timer.start()
    api.taxonGallery(rank, key, 250)
      .then((d) => { if (alive) { setData(d); setLoading(false); timer.stop() } })
      .catch((e) => { if (alive) { setError(e.message); setLoading(false); timer.stop() } })
    return () => { alive = false }
  }, [rank, key])

  // el backend ya filtra a especies con imagen (cualquier profundidad, no
  // solo hijos directos); aqui solo se normaliza el shape para el packer.
  const withImages = useMemo(() => {
    if (!data?.species) return []
    return data.species.map((c) => ({
      ...c,
      imageCount: c.images?.length || (c.image ? 1 : 0),
      imageList: c.images?.length > 0 ? c.images : (c.image ? [c.image] : []),
    }))
  }, [data])

  const { placements, gridHeight, cols } = useMemo(
    () => packTetris(withImages, dynamicCols),
    [withImages, dynamicCols]
  )

  return (
    <div className="bookworm-scope" style={kingdomStyleVars(data?.kingdom, dark)}>
      <div className="bw-page bw-page-wide">
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
                <p className="bw-muted">
                  {withImages.length} especie(s) con imagen.
                  {data.total === 250 && ' Mostrando las primeras 250.'}
                </p>
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