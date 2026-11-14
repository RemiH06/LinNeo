import { useNavigate } from 'react-router-dom'

// Nombre en la cabeza de página; lleva al inicio (Shui).
export default function LinNeoLogo() {
  const navigate = useNavigate()
  return (
    <a href="/" className="linneo-logo" onClick={(e) => { e.preventDefault(); navigate('/') }}>
      LinNeo
    </a>
  )
}
