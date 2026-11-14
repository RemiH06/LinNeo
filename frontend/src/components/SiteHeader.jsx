import { useNavigate } from 'react-router-dom'
import LinNeoLogo from './LinNeoLogo'
import ThemeToggle from './ThemeToggle'

// Cabeza de página del folio: nombre, volver, una ranura opcional (la búsqueda
// de Shui) y el modo. back: undefined = atrás en el historial (o a / si se
// entró directo), { to, label } = destino fijo, false = sin botón.
export default function SiteHeader({ back, children }) {
  const navigate = useNavigate()
  const goBack = () => {
    if (back?.to) navigate(back.to)
    else if (window.history.state?.idx > 0) navigate(-1)
    else navigate('/')
  }
  return (
    <header className="site-head">
      <div className="site-head-start">
        <LinNeoLogo />
        {back !== false && (
          <button type="button" className="ctl" onClick={goBack}>
            <span className="ctl-arrow" aria-hidden="true">‹</span>{back?.label || 'Volver'}
          </button>
        )}
      </div>
      {children && <div className="site-head-slot">{children}</div>}
      <ThemeToggle />
    </header>
  )
}
