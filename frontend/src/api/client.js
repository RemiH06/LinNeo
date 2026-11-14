const API_BASE = import.meta.env.VITE_API_BASE || '/api'
async function get(path) {
  const res = await fetch(`${API_BASE}${path}`)
  if (!res.ok) {
    const detail = await res.json().catch(() => ({}))
    throw new Error(detail.detail || `Error ${res.status}`)
  }
  return res.json()
}
export const api = {
  stats: () => get('/stats'),
  // API v2: los taxones se piden solo por taxonID; `rank` queda en las rutas del front.
  search: (q, limit = 25, offset = 0) => get(`/search?q=${encodeURIComponent(q)}&limit=${limit}&offset=${offset}`),
  searchClades: (q, limitPerGroup = 100, mode = 'contains') => get(`/search/clades?q=${encodeURIComponent(q)}&limit_per_group=${limitPerGroup}&mode=${mode}`),
  searchCladesByRank: (rank, q, limitPerGroup = 100, mode = 'contains') => get(`/search/clades/${rank}?q=${encodeURIComponent(q)}&limit_per_group=${limitPerGroup}&mode=${mode}`),
  species: (key) => get(`/species/${key}`),
  taxon: (key) => get(`/taxon/${key}`),
  taxonGallery: (key, limit = 250) => get(`/taxon/${key}/gallery?limit=${limit}`),
  taxonSounds: (key, limit = 250) => get(`/taxon/${key}/sounds?limit=${limit}`),
  taxonInfographic: (key) => get(`/taxon/${key}/infographic`),
  filter: (params) => {
    const qs = new URLSearchParams(
      Object.entries(params).filter(([, v]) => v != null && v !== '')
    ).toString()
    return get(`/filter?${qs}`)
  },
  // shui
  kingdoms: () => get('/kingdoms'),
  graph: () => get('/graph'),
  graphFocus: (key) => get(`/graph/${key}`),
  randomPool: (kingdoms = [], n = 8) => {
    const qs = new URLSearchParams()
    kingdoms.forEach((k) => qs.append('kingdoms', k))
    qs.set('n', n)
    return get(`/random/pool?${qs.toString()}`)
  },
  randomDescendants: (key, n = 9) => get(`/random/${key}?n=${n}`),
  mapCountry: (code) => get(`/map/country/${encodeURIComponent(code)}`),
}
// Paleta y etiquetas de los tipos de presencia (establishment_means)
export const ESTABLISHMENT = {
  native:      { color: '#00E8D8', label: 'Nativo' },
  introduced:  { color: '#F0C000', label: 'Introducido' },
  naturalised: { color: '#00F090', label: 'Naturalizado' },
  invasive:    { color: '#F04050', label: 'Invasor' },
  managed:     { color: '#8060FF', label: 'Manejado/cautiverio' },
  uncertain:   { color: '#7090C0', label: 'Incierto' },
  '':          { color: '#4D8FFF', label: 'Presente (sin clasificar)' },
}
// Estados IUCN: codigo -> {label, color}
export const IUCN = {
  LC: { label: 'Preocupacion menor', color: '#00F090' },
  NT: { label: 'Casi amenazada', color: '#9ACD32' },
  VU: { label: 'Vulnerable', color: '#F0C000' },
  EN: { label: 'En peligro', color: '#F08000' },
  CR: { label: 'En peligro critico', color: '#F04050' },
  RE: { label: 'Extinta regionalmente', color: '#A04060' },
  EW: { label: 'Extinta en estado silvestre', color: '#8060FF' },
  EX: { label: 'Extinta', color: '#606060' },
  DD: { label: 'Datos insuficientes', color: '#7090C0' },
  NE: { label: 'No evaluada', color: '#5A6080' },
}