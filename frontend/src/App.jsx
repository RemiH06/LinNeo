import { BrowserRouter, Routes, Route, useParams } from 'react-router-dom'
import { ThemeProvider } from './theme/ThemeContext'
import { KingdomProvider } from './theme/KingdomContext'
import SiteHeader from './components/SiteHeader'
import SpeciesDetail from './pages/SpeciesDetail'
import TaxonNode from './pages/TaxonNode'
import Shui from './pages/Shui'
import SearchResults from './pages/SearchResults'
import MapExplorer from './pages/MapExplorer'
import MapCountry from './pages/MapCountry'
import TaxonGallery from './pages/TaxonGallery'
import SoundTree from './pages/SoundTree'
import TaxonInfographic from './pages/TaxonInfographic'
import ErrorBoundary from './components/ErrorBoundary'

function SpeciesPage() {
  const { key } = useParams()
  // onOpenMedia: por ahora navega/loguea. Aqui conectaremos la transicion a bookworm.
  const onOpenMedia = (payload) => {
    console.log('Abrir media (futuro: transicion a bookworm)', payload)
  }
  return (
    <div>
      <SiteHeader />
      <SpeciesDetail speciesKey={key} onOpenMedia={onOpenMedia} />
    </div>
  )
}

export default function App() {
  return (
    <ThemeProvider>
      <KingdomProvider>
        <BrowserRouter>
          <Routes>
            <Route path="/" element={<ErrorBoundary><Shui /></ErrorBoundary>} />
            <Route path="/species/:key" element={<div className="page wide"><ErrorBoundary><SpeciesPage /></ErrorBoundary></div>} />
            <Route path="/taxon/:rank/:key/gallery" element={<ErrorBoundary><TaxonGallery /></ErrorBoundary>} />
            <Route path="/taxon/:rank/:key/sounds" element={<ErrorBoundary><SoundTree /></ErrorBoundary>} />
            <Route path="/taxon/:rank/:key/infographic" element={<ErrorBoundary><TaxonInfographic /></ErrorBoundary>} />
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