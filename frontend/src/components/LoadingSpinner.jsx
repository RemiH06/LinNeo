// Aviso de carga: qué se espera y cuánto lleva, con un filete que avanza.
// Sin porcentaje: una consulta de red no tiene progreso medible y una barra
// simulada promete algo que no sabe. `inline` lo deja en la línea del texto.
export default function LoadingSpinner({ label = 'Consultando el árbol', timeText = null, inline = false }) {
  const Tag = inline ? 'span' : 'div'
  return (
    <Tag className={`loading${inline ? ' inline' : ''}`} role="status" aria-live="polite">
      <span className="loading-text">{label}…{timeText && <span className="loading-time"> {timeText}</span>}</span>
      {!inline && <span className="loading-rule" aria-hidden="true" />}
    </Tag>
  )
}
