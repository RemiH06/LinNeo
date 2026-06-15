// Cliente de la API LinNeo.
// En desarrollo, Vite hace proxy de /api -> http://localhost:8000 (ver vite.config.js).
// En produccion, ajusta API_BASE a la URL publica de tu backend.
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
  search: (q, limit = 25) => get(`/search?q=${encodeURIComponent(q)}&limit=${limit}`),
  searchDescription: (q, limit = 25) => get(`/search/description?q=${encodeURIComponent(q)}&limit=${limit}`),
  species: (key) => get(`/species/${key}`),
  filter: (params) => {
    const qs = new URLSearchParams(
      Object.entries(params).filter(([, v]) => v != null && v !== '')
    ).toString()
    return get(`/filter?${qs}`)
  },
}