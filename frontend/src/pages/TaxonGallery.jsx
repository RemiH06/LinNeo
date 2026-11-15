import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { api } from '../api/client'
import { thumb } from '../api/media'
import { useTheme } from '../theme/ThemeContext'
import { useSetKingdom } from '../theme/KingdomContext'
import { useKeyboardShortcuts } from '../hooks/useKeyboardShortcuts'
import { useElapsedTimer, formatElapsed } from '../hooks/useElapsedTimer'
import LoadingSpinner from '../components/LoadingSpinner'
import SiteHeader from '../components/SiteHeader'
import { packTetris } from '../theme/tetrisPacker'
import '../theme/gallery.css'

/*
  Galería: mosaico de láminas de las especies con imagen bajo un taxón (a
  cualquier profundidad). Cada especie es una pieza: con 1 imagen, un cuadro de
  1, 2 o 3 celdas de lado; con 2 a 5, una forma de tantas celdas como imágenes
  (dominó, L, tetrominó), una imagen por celda. El empaque vive en
  theme/tetrisPacker.js. Cada página cargada se empaca aparte, así "cargar más"
  agrega abajo sin mover lo anterior.
*/
const RANK_ES = {
  biota: 'Biota', domain: 'Dominio', kingdom: 'Reino', phylum: 'Filo', class: 'Clase', order: 'Orden',
  family: 'Familia', genus: 'Género', species: 'Especie',
}
const PAGE = 120
const GAP = 6
const fmt = (n) => (n ?? 0).toLocaleString('es-MX')

// Columnas según el ancho real del contenedor (antes: 70 % de la ventana y mínimo 5, que desbordaba en celular).
function useCells(ref) {
  const [w, setW] = useState(0)
  useLayoutEffect(() => {
    const ro = new ResizeObserver(([e]) => setW(Math.floor(e.contentRect.width)))
    ro.observe(ref.current)
    return () => ro.disconnect()
  }, [ref])
  const target = w < 640 ? 84 : 104
  const cols = Math.max(3, Math.floor((w + GAP) / (target + GAP)))
  const cell = w ? (w - GAP * (cols - 1)) / cols : target
  return { cols, cell }
}

function Cell({ src, x, y, active, first, item, onEnter }) {
  const [step, setStep] = useState(0)        // 0 miniatura, 1 original, 2 sin imagen
  const url = step === 0 ? thumb(src, 250) : src
  const style = { gridColumn: x + 1, gridRow: y + 1 }
  const a11y = first ? { 'aria-label': item.name } : { tabIndex: -1, 'aria-hidden': true }
  return (
    <Link to={`/species/${item.key}`} className={`tg-cell${active ? ' on' : ''}`} style={style}
      data-kingdom={item.kingdom} title={item.name} onPointerEnter={onEnter} onFocus={onEnter} {...a11y}>
      {step < 2 && src && <img src={url} alt="" loading="lazy" onError={() => setStep((s) => (s === 0 && url !== src ? 1 : 2))} />}
    </Link>
  )
}

function Mosaic({ items, cols, cell, hover, setHover }) {
  const { placements, gridHeight } = useMemo(() => packTetris(items, cols), [items, cols])
  return (
    <div className="tg-grid" style={{ gridTemplateColumns: `repeat(${cols}, 1fr)`, gridTemplateRows: `repeat(${gridHeight}, ${cell}px)`, gap: GAP }}
      onPointerLeave={() => setHover(null)}>
      {placements.flatMap(({ item, x, y, shape }) => {
        const square = shape.length > 1 && item.imageList.length === 1
        if (square) {   // una imagen en un cuadro de 2×2 o 3×3: una sola celda que abarca el cuadro
          const n = Math.sqrt(shape.length)
          return [
            <Link key={item.key} to={`/species/${item.key}`} className={`tg-cell${hover?.key === item.key ? ' on' : ''}`}
              style={{ gridColumn: `${x + 1} / span ${n}`, gridRow: `${y + 1} / span ${n}` }} data-kingdom={item.kingdom}
              title={item.name} aria-label={item.name} onPointerEnter={() => setHover(item)} onFocus={() => setHover(item)}>
              <BigImage src={item.imageList[0]} />
            </Link>,
          ]
        }
        return shape.map(([dx, dy], i) => (
          <Cell key={`${item.key}-${i}`} src={item.imageList[i] || item.imageList[0]} x={x + dx} y={y + dy}
            first={i === 0} item={item} active={hover?.key === item.key} onEnter={() => setHover(item)} />
        ))
      })}
    </div>
  )
}

function BigImage({ src }) {
  const [step, setStep] = useState(0)
  const url = step === 0 ? thumb(src, 330) : src
  return step < 2 ? <img src={url} alt="" loading="lazy" onError={() => setStep((s) => (s === 0 && url !== src ? 1 : 2))} /> : null
}

export default function TaxonGallery() {
  const { key } = useParams()
  const navigate = useNavigate()
  const { toggle: toggleTheme } = useTheme()
  const [data, setData] = useState(null)
  const [pages, setPages] = useState([])
  const [loading, setLoading] = useState(true)
  const [more, setMore] = useState(false)
  const [error, setError] = useState(null)
  const [hover, setHover] = useState(null)
  const timer = useElapsedTimer()
  const wrapRef = useRef(null)
  const { cols, cell } = useCells(wrapRef)
  useSetKingdom(data?.kingdom)
  useKeyboardShortcuts({ navigate, toggleTheme })

  const norm = (list) => list.map((c) => ({ ...c, imageList: c.images?.length ? c.images.slice(0, 5) : (c.image ? [c.image] : []) }))
    .map((c) => ({ ...c, imageCount: c.imageList.length }))

  useEffect(() => {
    let alive = true
    setLoading(true); setError(null); setData(null); setPages([]); setHover(null)
    timer.start()
    api.taxonGallery(key, PAGE)
      .then((d) => { if (alive) { setData(d); setPages([norm(d.species)]) } })
      .catch((e) => { if (alive) setError(e.message) })
      .finally(() => { if (alive) { setLoading(false); timer.stop() } })
    return () => { alive = false }
  }, [key])

  const shown = pages.reduce((s, p) => s + p.length, 0)
  const loadMore = () => {
    setMore(true)
    api.taxonGallery(key, PAGE, shown)
      .then((d) => setPages((p) => [...p, norm(d.species)]))
      .catch((e) => setError(e.message))
      .finally(() => setMore(false))
  }

  return (
    <div className="folio">
      <SiteHeader />
      {data && (
        <header className="taxon-head" data-rank={data.rank}>
          <p className="taxon-rank">Galería · {RANK_ES[data.rank] || data.rank}</p>
          <h1 className="taxon-name"><Link to={`/taxon/${data.rank}/${data.key}`}>{data.name}</Link></h1>
          <p className="taxon-summary">{fmt(data.total)} {data.total === 1 ? 'especie tiene' : 'especies tienen'} imagen. Cada pieza es una especie; sus celdas, sus láminas.</p>
        </header>
      )}
      <p className="tg-caption" aria-live="polite">
        {hover ? <><span className="binomial">{hover.name}</span>{hover.common_names?.[0] ? ` · ${hover.common_names[0]}` : ''}</> : ' '}
      </p>
      <div ref={wrapRef} className="tg-wrap">
        {loading && <LoadingSpinner inline={false} timeText={formatElapsed(timer.elapsedMs)} />}
        {error && <p className="taxon-error" role="alert">No se pudo cargar la galería: {error}</p>}
        {data && shown === 0 && <p className="taxon-count">Ninguna especie de este grupo tiene imagen.</p>}
        {pages.map((p, i) => <Mosaic key={i} items={p} cols={cols} cell={cell} hover={hover} setHover={setHover} />)}
      </div>
      {data && shown < data.total && (
        <button type="button" className="ctl load-more tg-more" onClick={loadMore} disabled={more}>
          {more ? 'Cargando…' : `Cargar ${fmt(Math.min(PAGE, data.total - shown))} más · van ${fmt(shown)} de ${fmt(data.total)}`}
        </button>
      )}
    </div>
  )
}
