import { useEffect, useMemo, useRef, useState } from 'react'
import { loadWorld, isoToName } from '../geo/countries'
import '../theme/worldmap.css'

/*
  Mapa del mundo como grabado: países en SVG ya proyectados (Equal Earth,
  scripts/build_world.py), con su contorno y retícula, sin teselas ni terceros.
  - highlight: ISO-2 que se pintan con la tinta del reino (clase "on").
  - classFor(iso): clases extra por país (selección, establecimiento…).
  - onSelect(iso) + isInteractive(iso): países que responden al clic; con
    `focusable` también al teclado (solo cuando son pocos: para muchos, la página
    pone una lista de enlaces).
  - view: 'world' | 'fit' (encuadra lo resaltado) | [x0, y0, x1, y1].
  El encuadre se anima; con prefers-reduced-motion salta directo.
*/
const PAD = 0.18
const MIN_W = 150      // ancho mínimo del encuadre, en unidades del mapa (de 1000)
const TINY = 2.5       // un país más chico que esto lleva un punto para que se vea

function fitBox(boxes, aspect, W, H) {
  if (!boxes.length) return [0, 0, W, H]
  let [x0, y0, x1, y1] = [Math.min(...boxes.map((b) => b[0])), Math.min(...boxes.map((b) => b[1])),
    Math.max(...boxes.map((b) => b[2])), Math.max(...boxes.map((b) => b[3]))]
  let w = Math.max(x1 - x0, MIN_W) * (1 + 2 * PAD), h = Math.max(y1 - y0, MIN_W / aspect) * (1 + 2 * PAD)
  if (w / h > aspect) h = w / aspect; else w = h * aspect
  if (w >= W) return [0, 0, W, H]
  const cx = (x0 + x1) / 2, cy = (y0 + y1) / 2
  const x = Math.min(Math.max(cx - w / 2, 0), W - w)
  const y = Math.min(Math.max(cy - h / 2, 0), Math.max(H - h, 0))
  return [x, y, x + w, y + h]
}

export default function WorldMap({
  highlight = [], classFor, onSelect, isInteractive, onHover, view = 'world', aspect = 2,
  focusable = false, label = 'Mapa del mundo', className = '',
}) {
  const [world, setWorld] = useState(null)
  const [failed, setFailed] = useState(false)
  const [vb, setVb] = useState(null)
  const vbRef = useRef(null)
  const anim = useRef(0)
  useEffect(() => {
    let alive = true
    loadWorld().then((w) => { if (alive) setWorld(w) }).catch(() => { if (alive) setFailed(true) })
    return () => { alive = false }
  }, [])

  const on = useMemo(() => new Set(highlight), [highlight])
  const target = useMemo(() => {
    if (!world) return null
    if (Array.isArray(view)) return fitBox([view], aspect, world.w, world.h)
    if (view === 'fit') return fitBox(world.countries.filter((c) => on.has(c.iso)).map((c) => c.mbox), aspect, world.w, world.h)
    return [0, 0, world.w, world.h]
  }, [world, view, on, aspect])

  // Encuadre animado hacia `target`.
  useEffect(() => {
    if (!target) return
    const from = vbRef.current
    const reduce = window.matchMedia('(prefers-reduced-motion: reduce)').matches
    cancelAnimationFrame(anim.current)
    if (!from || reduce) { vbRef.current = target; setVb(target); return }
    const t0 = performance.now(), dur = 650
    const step = (t) => {
      const k = Math.min(1, (t - t0) / dur), e = 1 - Math.pow(1 - k, 3)
      const cur = from.map((v, i) => v + (target[i] - v) * e)
      vbRef.current = cur; setVb(cur)
      if (k < 1) anim.current = requestAnimationFrame(step)
    }
    anim.current = requestAnimationFrame(step)
    return () => cancelAnimationFrame(anim.current)
  }, [target])

  if (failed) return <p className="margin-note">No se pudo cargar el mapa.</p>
  if (!world || !vb) return <div className={`world-map ${className}`} style={{ aspectRatio: aspect }} aria-busy="true" />

  const [x0, y0, x1, y1] = vb
  const dotR = (x1 - x0) / 220
  const canClick = (iso) => !!onSelect && (isInteractive ? isInteractive(iso) : on.has(iso))
  return (
    <svg className={`world-map ${className}`} viewBox={`${x0} ${y0} ${x1 - x0} ${y1 - y0}`}
      preserveAspectRatio="xMidYMid meet" role={focusable ? 'group' : 'img'} aria-label={label}
      onPointerLeave={() => onHover?.(null)}>
      <path className="wm-sphere" d={world.sphere} />
      <path className="wm-grid" d={world.graticule} />
      {world.countries.map((c) => {
        const click = canClick(c.iso)
        const name = isoToName(c.iso) || c.iso
        const cls = ['country', on.has(c.iso) && 'on', click && 'live', classFor?.(c.iso)].filter(Boolean).join(' ')
        const tiny = on.has(c.iso) && c.mbox[2] - c.mbox[0] < TINY && c.mbox[3] - c.mbox[1] < TINY
        const props = click ? {
          onClick: () => onSelect(c.iso),
          onPointerEnter: () => onHover?.(c.iso),
          ...(focusable ? {
            tabIndex: 0, role: 'button', 'aria-label': name,
            onFocus: () => onHover?.(c.iso),
            onKeyDown: (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onSelect(c.iso) } },
          } : {}),
        } : { onPointerEnter: () => onHover?.(null) }
        return (
          <g key={c.iso} className={cls} {...props}>
            <path d={c.d}><title>{name}</title></path>
            {tiny && <circle cx={(c.mbox[0] + c.mbox[2]) / 2} cy={(c.mbox[1] + c.mbox[3]) / 2} r={dotR} />}
          </g>
        )
      })}
    </svg>
  )
}
