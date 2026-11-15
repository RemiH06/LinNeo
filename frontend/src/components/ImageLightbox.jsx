import { useEffect, useRef, useState } from 'react'
import { thumb, webUrl } from '../api/media'
import '../theme/lightbox.css'

/*
  Visor de láminas (diálogo modal).
  - Acercar con la rueda o los botones; arrastrar para mover (mouse o dedo).
  - Cuentagotas: clic en la imagen copia el color HEX. Solo con imágenes que
    permiten CORS (Wikimedia); en las demás el lienzo queda "manchado" y se avisa.
  - Escape cierra, flechas cambian de lámina; el foco queda dentro y vuelve al
    botón que abrió el visor.
  Props: items [{url, title, creator, source, source_url, license}], index, onClose, speciesName.
*/
const CORS_OK = /^https:\/\/upload\.wikimedia\.org\//

export default function ImageLightbox({ items = [], index = 0, onClose, speciesName = '' }) {
  const [current, setCurrent] = useState(index)
  const [attempt, setAttempt] = useState(0)          // 0 = miniatura, 1 = original
  const [zoom, setZoom] = useState(1)
  const [pan, setPan] = useState({ x: 0, y: 0 })
  const [dropper, setDropper] = useState(false)
  const [picked, setPicked] = useState(null)
  const [msg, setMsg] = useState('')
  const dialogRef = useRef(null)
  const closeRef = useRef(null)
  const imgRef = useRef(null)
  const drag = useRef(null)
  const img = items[current]
  const src = img && (attempt === 0 ? thumb(img.url, 1280) : img.url)

  const resetView = () => { setZoom(1); setPan({ x: 0, y: 0 }); setPicked(null); setDropper(false); setMsg('') }
  const go = (d) => { setCurrent((c) => (c + d + items.length) % items.length); setAttempt(0); resetView() }

  // Foco: entra al abrir, vuelve al cerrar; Tab no sale del diálogo.
  useEffect(() => {
    const opener = document.activeElement
    closeRef.current?.focus()
    const prevOverflow = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    return () => { document.body.style.overflow = prevOverflow; opener?.focus?.() }
  }, [])

  useEffect(() => {
    const onKey = (e) => {
      if (e.key === 'Escape') { e.preventDefault(); onClose() }
      else if (e.key === 'ArrowRight' && items.length > 1) go(1)
      else if (e.key === 'ArrowLeft' && items.length > 1) go(-1)
      else if (e.key === 'Tab') {
        const f = [...dialogRef.current.querySelectorAll('button, a[href]')].filter((el) => !el.disabled)
        if (!f.length) return
        const first = f[0], last = f[f.length - 1]
        if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus() }
        else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus() }
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [items.length, onClose])

  const clampZoom = (z) => Math.max(0.5, Math.min(6, z))
  const onWheel = (e) => setZoom((z) => clampZoom(z * (e.deltaY < 0 ? 1.15 : 1 / 1.15)))
  const onPointerDown = (e) => {
    if (dropper) return
    e.currentTarget.setPointerCapture(e.pointerId)
    drag.current = { x: e.clientX, y: e.clientY, px: pan.x, py: pan.y }
  }
  const onPointerMove = (e) => {
    if (!drag.current) return
    setPan({ x: drag.current.px + e.clientX - drag.current.x, y: drag.current.py + e.clientY - drag.current.y })
  }
  const onPointerUp = () => { drag.current = null }

  function pickColor(e) {
    const el = imgRef.current
    if (!dropper || !el) return
    const r = el.getBoundingClientRect()
    const c = document.createElement('canvas')
    c.width = el.naturalWidth; c.height = el.naturalHeight
    const ctx = c.getContext('2d')
    ctx.drawImage(el, 0, 0)
    try {
      const [R, G, B] = ctx.getImageData(Math.floor(((e.clientX - r.left) / r.width) * c.width),
        Math.floor(((e.clientY - r.top) / r.height) * c.height), 1, 1).data
      const hex = '#' + [R, G, B].map((v) => v.toString(16).padStart(2, '0')).join('').toUpperCase()
      setPicked(hex)
      navigator.clipboard?.writeText(hex).then(() => setMsg(`Copiado ${hex}`), () => setMsg(hex))
    } catch {
      setMsg('Esta imagen no deja leer sus colores.')
    }
  }

  if (!img) return null
  const cors = CORS_OK.test(src)

  return (
    <div className="lb-overlay" onClick={(e) => { if (e.target === e.currentTarget) onClose() }}>
      <div className="lb-dialog" role="dialog" aria-modal="true" aria-label={`Láminas de ${speciesName}`} ref={dialogRef}>
        <div className="lb-bar">
          <span className="lb-count">{items.length > 1 ? `Lámina ${current + 1} de ${items.length}` : 'Lámina'}</span>
          <div className="lb-tools" role="group" aria-label="Vista">
            <button type="button" className="ctl" onClick={() => setZoom((z) => clampZoom(z * 1.25))}>Acercar</button>
            <button type="button" className="ctl" onClick={() => setZoom((z) => clampZoom(z / 1.25))}>Alejar</button>
            <button type="button" className="ctl" onClick={resetView}>Restablecer</button>
            {cors && (
              <button type="button" className="ctl" aria-pressed={dropper} onClick={() => { setDropper((v) => !v); setMsg('') }}>
                Cuentagotas
              </button>
            )}
            {picked && <span className="lb-swatch" aria-hidden="true" style={{ background: picked }} />}
            <span className="lb-msg" role="status">{msg}</span>
          </div>
          <button type="button" className="ctl lb-close" ref={closeRef} onClick={onClose}>Cerrar</button>
        </div>

        <div className={`lb-stage${dropper ? ' dropper' : ''}`} onWheel={onWheel}
          onPointerDown={onPointerDown} onPointerMove={onPointerMove} onPointerUp={onPointerUp} onPointerCancel={onPointerUp}>
          <img key={src} ref={imgRef} src={src} alt={`${speciesName}${img.title ? `: ${img.title}` : ''}`}
            crossOrigin={cors ? 'anonymous' : undefined} draggable={false} hidden={attempt > 1}
            style={{ transform: `translate(${pan.x}px, ${pan.y}px) scale(${zoom})` }}
            onClick={pickColor}
            onError={() => setAttempt((a) => (a === 0 && thumb(img.url, 1280) !== img.url ? 1 : 2))} />
          {attempt > 1 && <p className="lb-msg lb-broken">No se pudo cargar esta lámina.</p>}
        </div>

        <div className="lb-foot">
          {items.length > 1 && <button type="button" className="ctl" onClick={() => go(-1)}>‹ Anterior</button>}
          <p className="lb-credit">
            {img.creator}{img.creator && img.source_url && ' · '}
            {webUrl(img.source_url) && <a href={webUrl(img.source_url)} target="_blank" rel="noreferrer">Ver la fuente</a>}
          </p>
          {items.length > 1 && <button type="button" className="ctl" onClick={() => go(1)}>Siguiente ›</button>}
        </div>
      </div>
    </div>
  )
}
