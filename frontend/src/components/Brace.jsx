import { useLayoutEffect, useRef, useState } from 'react'

// Llave tipográfica `{` que crece con su contenido, como en las tablas del
// Systema Naturae. Es un SVG trazado a la altura medida: un glifo escalado
// engordaría el trazo y estiraría las curvas.
const W = 16

function bracePath(h) {
  const r = Math.min(10, h / 4)      // radio de las curvas
  const m = W / 2                    // eje vertical de la llave
  const c = h / 2
  return `M${W},1 Q${m},1 ${m},${1 + r} L${m},${c - r} Q${m},${c} 1,${c} ` +
         `Q${m},${c} ${m},${c + r} L${m},${h - 1 - r} Q${m},${h - 1} ${W},${h - 1}`
}

export default function Brace({ children, className = '' }) {
  const ref = useRef(null)
  const [h, setH] = useState(0)
  useLayoutEffect(() => {
    const el = ref.current
    if (!el) return
    const ro = new ResizeObserver(([e]) => setH(Math.round(e.contentRect.height)))
    ro.observe(el)
    return () => ro.disconnect()
  }, [])
  return (
    <div className={`brace ${className}`.trim()}>
      <svg className="brace-glyph" width={W} height={h} viewBox={`0 0 ${W} ${h}`} aria-hidden="true">
        {h > 0 && <path d={bracePath(h)} />}
      </svg>
      <div className="brace-body" ref={ref}>{children}</div>
    </div>
  )
}
