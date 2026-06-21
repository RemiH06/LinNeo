import { useState } from 'react'
import { BrowserRouter, Routes, Route, useNavigate, useParams, useLocation } from 'react-router-dom'
import { ThemeProvider } from './theme/ThemeContext'
import { KingdomProvider } from './theme/KingdomContext'
import ElixirGraph from './backgrounds/ElixirGraph'
import BookwormBackground from './backgrounds/BookwormBackground'
import KingdomBackdrop from './backgrounds/KingdomBackdrop'
import ThemeToggle from './components/ThemeToggle'
import LinNeoLogo from './components/LinNeoLogo'
import SpeciesDetail from './pages/SpeciesDetail'
import TaxonNode from './pages/TaxonNode'
import Shui from './pages/Shui'
import SearchResults from './pages/SearchResults'
import MapExplorer from './pages/MapExplorer'
import MapCountry from './pages/MapCountry'
import TaxonGallery from './pages/TaxonGallery'
import SoundTree from './pages/SoundTree'
import ErrorBoundary from './components/ErrorBoundary'
import { Callout } from './components/ui'

function Header() {
  return (
    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 40 }}>
      <div style={{ fontSize: 26, fontWeight: 700, letterSpacing: '-1px' }}>
        LinNeo<span style={{ color: 'var(--accent)' }}>_</span>
      </div>
      <ThemeToggle />
    </div>
  )
}

// Home provisional hasta tener el shell shui: permite abrir una ficha por species_key.
function Home() {
  const [key, setKey] = useState('')
  const navigate = useNavigate()
  const go = () => { if (key.trim()) navigate(`/species/${key.trim()}`) }
  return (
    <div>
      <Header />
      <h2>Ficha de especie</h2>
      <p className="muted">Introduce un species_key de GBIF para abrir su ficha. (El buscador shui vendra despues.)</p>
      <div style={{ display: 'flex', gap: 8, marginTop: 12 }}>
        <input
          value={key}
          onChange={(e) => setKey(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && go()}
          placeholder="ej. 5219404"
          style={{
            flex: 1, fontFamily: 'var(--mono)', fontSize: 13, padding: '8px 12px',
            background: 'var(--bg2)', border: '1px solid var(--border)',
            borderRadius: 'var(--radius)', color: 'var(--text)',
          }}
        />
        <button className="btn primary" onClick={go}>Abrir</button>
      </div>
      <Callout title="Nota" variant="warn">
        Necesitas el backend FastAPI corriendo en localhost:8000 (Vite hace proxy de /api).
      </Callout>
    </div>
  )
}

function SpeciesPage() {
  const { key } = useParams()
  const navigate = useNavigate()
  // onOpenMedia: por ahora navega/loguea. Aqui conectaremos la transicion a bookworm.
  const onOpenMedia = (payload) => {
    console.log('Abrir media (futuro: transicion a bookworm)', payload)
  }
  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 24 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
          <LinNeoLogo />
          <button className="btn" onClick={() => navigate('/')}>{'\u2039'} volver</button>
        </div>
        <ThemeToggle />
      </div>
      <SpeciesDetail speciesKey={key} onOpenMedia={onOpenMedia} />
    </div>
  )
}

// Elige el fondo global segun la ruta: TaxonNode, SearchResults y las vistas
// de mapa usan el fondo bookworm (papel/herbario); el resto sigue usando el
// grafo neural elixir.
function GlobalBackground() {
  const { pathname } = useLocation()
  if (pathname.startsWith('/taxon/') || pathname.startsWith('/search') || pathname.startsWith('/map')) {
    return <BookwormBackground />
  }
  return <ElixirGraph />
}

export default function App() {
  return (
    <ThemeProvider>
      <KingdomProvider>
        <BrowserRouter>
          <KingdomBackdrop />
          <GlobalBackground />
          <Routes>
            <Route path="/" element={<ErrorBoundary><Shui /></ErrorBoundary>} />
            <Route path="/species/:key" element={<div className="page wide"><ErrorBoundary><SpeciesPage /></ErrorBoundary></div>} />
            <Route path="/taxon/:rank/:key/gallery" element={<ErrorBoundary><TaxonGallery /></ErrorBoundary>} />
            <Route path="/taxon/:rank/:key/sounds" element={<ErrorBoundary><SoundTree /></ErrorBoundary>} />
            <Route path="/taxon/:rank/:key" element={<ErrorBoundary><TaxonPage /></ErrorBoundary>} />
            <Route path="/search" element={<ErrorBoundary><SearchResults /></ErrorBoundary>} />
            <Route path="/map/country/:code" element={<ErrorBoundary><MapCountry /></ErrorBoundary>} />
            <Route path="/map/*" element={<ErrorBoundary><MapExplorer /></ErrorBoundary>} />
          </Routes>
        </BrowserRouter>
      </KingdomProvider>
    </ThemeProvider>
  )
}

function TaxonPage() {
  const { rank, key } = useParams()
  return <TaxonNode rank={rank} nodeKey={key} />
}