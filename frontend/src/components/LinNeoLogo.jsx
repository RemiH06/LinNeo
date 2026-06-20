import { useNavigate } from 'react-router-dom'

// Wordmark "LinNeo_" reusado en todas las vistas como link de vuelta a la
// landing del grafo (Shui, /). className extra permite que cada vista lo
// integre con sus propios estilos de header sin perder el comportamiento.
export default function LinNeoLogo({ className = '' }) {
  const navigate = useNavigate()
  return (
    <a
      href="/"
      className={`linneo-logo ${className}`.trim()}
      onClick={(e) => { e.preventDefault(); navigate('/') }}
      title="Volver al grafo (Shui)"
    >
      LinNeo<span className="linneo-logo-underscore">_</span>
    </a>
  )
}