import { useEffect, useRef, useState, useCallback } from 'react'

/*
  ImageLightbox — overlay de imagen con:
  - Fondo: captura del DOM actual blurreada (html2canvas no disponible, usamos
    un div con backdrop-filter: blur sobre el contenido existente)
  - Imagen centrada con zoom via rueda del raton (0.5x – 5x)
  - Cuentagotas: sampler de color bajo el cursor sobre la imagen via canvas,
    clic copia el HEX al portapapeles
  - Galeria: flechas prev/next si hay multiples imagenes
  - Cerrar: clic fuera de la imagen, tecla Escape

  Props:
    items    - array de { url, type } (solo se muestran las de type === 'image')
    index    - indice inicial
    onClose  - callback al cerrar
    speciesName - nombre de la especie (para alt)
*/
export default function ImageLightbox({ items = [], index = 0, onClose, speciesName = '' }) {
  const [current, setCurrent] = useState(index)
  const [zoom, setZoom] = useState(1)
  const [pan, setPan] = useState({ x: 0, y: 0 })
  const [pickedColor, setPickedColor] = useState(null)
  const [copyMsg, setCopyMsg] = useState('')
  const [eyedropperActive, setEyedropperActive] = useState(false)
  const imgRef = useRef(null)
  const canvasRef = useRef(null)
  const dragging = useRef(false)
  const dragStart = useRef({ x: 0, y: 0, px: 0, py: 0 })

  const images = items.filter((m) => m.type === 'image')
  const img = images[current]

  // cerrar con Escape, navegar con flechas
  useEffect(() => {
    const handler = (e) => {
      if (e.key === 'Escape') onClose()
      if (e.key === 'ArrowRight') next()
      if (e.key === 'ArrowLeft') prev()
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [current, images.length])

  function next() { setCurrent((c) => (c + 1) % images.length); resetView() }
  function prev() { setCurrent((c) => (c - 1 + images.length) % images.length); resetView() }
  function resetView() { setZoom(1); setPan({ x: 0, y: 0 }); setPickedColor(null); setEyedropperActive(false) }

  // zoom con rueda
  function onWheel(e) {
    e.preventDefault()
    const factor = e.deltaY < 0 ? 1.15 : 1 / 1.15
    setZoom((z) => Math.max(0.5, Math.min(5, z * factor)))
  }

  // pan con drag
  function onMouseDown(e) {
    if (eyedropperActive) return
    dragging.current = true
    dragStart.current = { x: e.clientX, y: e.clientY, px: pan.x, py: pan.y }
  }
  function onMouseMove(e) {
    if (!dragging.current) return
    setPan({ x: dragStart.current.px + e.clientX - dragStart.current.x, y: dragStart.current.py + e.clientY - dragStart.current.y })
  }
  function onMouseUp() { dragging.current = false }

  // cuentagotas: samplear el color bajo el cursor usando un canvas temporal
  function pickColor(e) {
    if (!eyedropperActive || !imgRef.current) return
    const imgEl = imgRef.current
    const rect = imgEl.getBoundingClientRect()
    const sx = (e.clientX - rect.left) / rect.width
    const sy = (e.clientY - rect.top) / rect.height
    const canvas = canvasRef.current
    const ctx = canvas.getContext('2d')
    canvas.width = imgEl.naturalWidth
    canvas.height = imgEl.naturalHeight
    ctx.drawImage(imgEl, 0, 0)
    const px = Math.floor(sx * imgEl.naturalWidth)
    const py = Math.floor(sy * imgEl.naturalHeight)
    const [r, g, b] = ctx.getImageData(px, py, 1, 1).data
    const hex = '#' + [r, g, b].map((v) => v.toString(16).padStart(2, '0')).join('')
    setPickedColor(hex)
    navigator.clipboard?.writeText(hex).then(() => {
      setCopyMsg(`Copiado ${hex}`)
      setTimeout(() => setCopyMsg(''), 2000)
    })
  }

  if (!img) return null

  return (
    <div
      className="lb-overlay"
      onClick={(e) => { if (e.target === e.currentTarget) onClose() }}
      onMouseMove={onMouseMove}
      onMouseUp={onMouseUp}
    >
      {/* Canvas oculto para el cuentagotas */}
      <canvas ref={canvasRef} style={{ display: 'none' }} />

      {/* Controles superiores */}
      <div className="lb-controls">
        <button className="lb-btn" onClick={() => setZoom((z) => Math.min(5, z * 1.2))}>＋</button>
        <button className="lb-btn" onClick={() => setZoom((z) => Math.max(0.5, z / 1.2))}>－</button>
        <button className="lb-btn" onClick={resetView} title="Restablecer vista">⊙</button>
        <button
          className={`lb-btn ${eyedropperActive ? 'active' : ''}`}
          onClick={() => setEyedropperActive((v) => !v)}
          title="Cuentagotas: clic en la imagen para copiar el color"
        >✦</button>
        {pickedColor && (
          <span className="lb-color-preview">
            <span style={{ display: 'inline-block', width: 14, height: 14, borderRadius: 3, background: pickedColor, border: '1px solid rgba(255,255,255,.4)', verticalAlign: 'middle', marginRight: 5 }} />
            {pickedColor}
          </span>
        )}
        {copyMsg && <span className="lb-copy-msg">{copyMsg}</span>}
        <button className="lb-btn lb-close" onClick={onClose} title="Cerrar (Esc)">✕</button>
      </div>

      {/* Imagen centrada con zoom y pan */}
      <div
        className="lb-stage"
        onWheel={onWheel}
        onMouseDown={onMouseDown}
        style={{ cursor: eyedropperActive ? 'crosshair' : (dragging.current ? 'grabbing' : 'grab') }}
      >
        <img
          ref={imgRef}
          src={img.url}
          alt={speciesName}
          className="lb-image"
          style={{
            transform: `translate(${pan.x}px, ${pan.y}px) scale(${zoom})`,
            cursor: eyedropperActive ? 'crosshair' : undefined,
          }}
          onClick={eyedropperActive ? pickColor : undefined}
          draggable={false}
          crossOrigin="anonymous"
        />
      </div>

      {/* Navegacion galeria */}
      {images.length > 1 && (
        <>
          <button className="lb-nav lb-prev" onClick={prev}>‹</button>
          <button className="lb-nav lb-next" onClick={next}>›</button>
          <div className="lb-counter">{current + 1} / {images.length}</div>
        </>
      )}
    </div>
  )
}