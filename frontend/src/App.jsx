import { lazy, Suspense } from 'react'
import { BrowserRouter, Routes, Route, Link, useParams } from 'react-router-dom'
import { ThemeProvider } from './theme/ThemeContext'
import { KingdomProvider } from './theme/KingdomContext'
import ErrorBoundary from './components/ErrorBoundary'
import SiteHeader from './components/SiteHeader'
import LoadingSpinner from './components/LoadingSpinner'

// Cada vista es su propio fragmento: entrar directo a una ficha no baja el grafo
// del inicio (d3-force) ni el mapa del mundo.
const Shui = lazy(() => import('./pages/Shui'))
const SpeciesDetail = lazy(() => import('./pages/SpeciesDetail'))
const TaxonNode = lazy(() => import('./pages/TaxonNode'))
const SearchResults = lazy(() => import('./pages/SearchResults'))
const MapExplorer = lazy(() => import('./pages/MapExplorer'))
const MapCountry = lazy(() => import('./pages/MapCountry'))
const TaxonGallery = lazy(() => import('./pages/TaxonGallery'))
const SoundTree = lazy(() => import('./pages/SoundTree'))
const TaxonInfographic = lazy(() => import('./pages/TaxonInfographic'))

function SpeciesPage() {
  const { key } = useParams()
  return <SpeciesDetail speciesKey={key} />
}

function TaxonPage() {
  const { key } = useParams()
  return <TaxonNode nodeKey={key} />
}

function NotFound() {
  return (
    <div className="folio">
      <SiteHeader />
      <header className="taxon-head">
        <p className="taxon-rank">Página no encontrada</p>
        <h1>Esta lámina no está en el folio</h1>
        <p className="taxon-summary">La dirección no corresponde a ninguna vista. <Link to="/">Volver al árbol de la vida</Link> o usar el buscador.</p>
      </header>
    </div>
  )
}

const page = (el) => (
  <ErrorBoundary>
    <Suspense fallback={<div className="folio"><LoadingSpinner label="Abriendo la lámina" /></div>}>{el}</Suspense>
  </ErrorBoundary>
)

export default function App() {
  return (
    <ThemeProvider>
      <KingdomProvider>
        <BrowserRouter>
          <Routes>
            <Route path="/" element={page(<Shui />)} />
            <Route path="/species/:key" element={page(<SpeciesPage />)} />
            <Route path="/taxon/:rank/:key/gallery" element={page(<TaxonGallery />)} />
            <Route path="/taxon/:rank/:key/sounds" element={page(<SoundTree />)} />
            <Route path="/taxon/:rank/:key/infographic" element={page(<TaxonInfographic />)} />
            <Route path="/taxon/:rank/:key" element={page(<TaxonPage />)} />
            <Route path="/search" element={page(<SearchResults />)} />
            <Route path="/map/country/:code" element={page(<MapCountry />)} />
            <Route path="/map/*" element={page(<MapExplorer />)} />
            <Route path="*" element={page(<NotFound />)} />
          </Routes>
        </BrowserRouter>
      </KingdomProvider>
    </ThemeProvider>
  )
}
