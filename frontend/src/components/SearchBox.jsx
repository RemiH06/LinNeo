import { useEffect, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { api } from '../api/client'
import { kingdomInk } from '../theme/kingdomColor'
import { useTheme } from '../theme/ThemeContext'
import '../theme/search.css'

/*
  Buscador con sugerencias (patrón combobox de ARIA). Junta dos fuentes:
  primero el índice de texto completo (/search: nombre, sinónimo y nombre
  común, así "jaguar" encuentra Panthera onca) y luego los nombres científicos
  que contienen la cadena (/search/clades). Enter abre la sugerencia marcada,
  una coincidencia exacta o la página de resultados.
*/
const RANK_ES = {
  biota: 'Biota', domain: 'Dominio', kingdom: 'Reino', phylum: 'Filo', class: 'Clase',
  order: 'Orden', family: 'Familia', genus: 'Género', species: 'Especie',
}
const MAX = 10
export const CLADE_MIN = 3   // la API rechaza subcadenas más cortas (con 2 letras tardan segundos)
export const hrefOf = (t) => (t.rank === 'species' ? `/species/${t.key}` : `/taxon/${t.rank}/${t.key}`)

export function highlight(text, query) {
  const idx = query ? text.toLowerCase().indexOf(query.toLowerCase()) : -1
  if (idx === -1) return text
  return <>{text.slice(0, idx)}<mark>{text.slice(idx, idx + query.length)}</mark>{text.slice(idx + query.length)}</>
}

// Por qué salió un resultado que no contiene la cadena en su nombre.
export function matchNote(r, query) {
  const q = query.toLowerCase()
  if (r.name.toLowerCase().includes(q)) return null
  if (r.synonym_match) return <>sinónimo <i>{r.synonym_match}</i></>
  const common = r.common_names?.find((n) => n.toLowerCase().includes(q))
  return common ? `«${common}»` : null
}

export default function SearchBox({ initial = '', inputRef, onNavigate }) {
  const navigate = useNavigate()
  const { dark } = useTheme()
  const [q, setQ] = useState(initial)
  const [results, setResults] = useState([])
  const [loading, setLoading] = useState(false)
  const [failed, setFailed] = useState(false)
  const [active, setActive] = useState(-1)
  const [open, setOpen] = useState(false)
  const timer = useRef(null)
  const seq = useRef(0)
  const wrapRef = useRef(null)
  const ownRef = useRef(null)
  const ref = inputRef || ownRef

  useEffect(() => { setQ(initial) }, [initial])
  useEffect(() => {
    const onDown = (e) => { if (wrapRef.current && !wrapRef.current.contains(e.target)) close() }
    document.addEventListener('mousedown', onDown)
    return () => document.removeEventListener('mousedown', onDown)
  }, [])

  function close() { setOpen(false); setActive(-1) }

  function onChange(value) {
    setQ(value); setActive(-1); setOpen(true)
    clearTimeout(timer.current)
    if (value.trim().length < 2) { setResults([]); setFailed(false); return }
    timer.current = setTimeout(() => {
      const id = ++seq.current
      const term = value.trim()
      setLoading(true); setFailed(false)
      // La subcadena pide 3 letras (con 2 tarda segundos); antes, solo texto completo.
      const cladesP = term.length >= CLADE_MIN ? api.searchClades(term, 6) : Promise.resolve({ groups: {} })
      Promise.allSettled([api.search(term, 6), cladesP]).then(([full, clades]) => {
        if (id !== seq.current) return   // llegó tarde: ya se escribió otra cosa
        if (full.status === 'rejected' && clades.status === 'rejected') { setResults([]); setFailed(true) }
        else {
          const merged = [...(full.value?.results || [])]
          for (const rank of Object.keys(clades.value?.groups || {}))
            for (const g of clades.value.groups[rank]) merged.push(...g.items)
          const seen = new Set()
          setResults(merged.filter((r) => !seen.has(r.key) && seen.add(r.key)).slice(0, MAX))
        }
        setLoading(false)
      })
    }, 250)
  }

  function go(path) { navigate(path); close(); onNavigate?.() }
  function openResult(r) { setQ(''); setResults([]); go(hrefOf(r)) }
  function submit() {
    const term = q.trim()
    if (!term) return
    if (active >= 0 && results[active]) return openResult(results[active])
    const exact = results.find((r) => r.name?.toLowerCase() === term.toLowerCase())
    if (exact) return openResult(exact)
    go(`/search?q=${encodeURIComponent(term)}`)
  }
  function onKeyDown(e) {
    if (e.key === 'ArrowDown') { e.preventDefault(); setOpen(true); setActive((i) => Math.min(i + 1, results.length - 1)) }
    else if (e.key === 'ArrowUp') { e.preventDefault(); setActive((i) => Math.max(i - 1, -1)) }
    else if (e.key === 'Escape') close()
    else if (e.key === 'Enter') { e.preventDefault(); submit() }
  }

  const show = open && q.trim().length >= 2
  const term = q.trim()
  return (
    <div className="search" ref={wrapRef}>
      <label htmlFor="buscar" className="sr-only">Buscar una especie o un grupo</label>
      <input id="buscar" ref={ref} className="search-input" type="search" value={q} autoComplete="off"
        role="combobox" aria-expanded={show} aria-controls="sugerencias" aria-autocomplete="list"
        aria-activedescendant={active >= 0 ? `sug-${active}` : undefined}
        onChange={(e) => onChange(e.target.value)} onKeyDown={onKeyDown} onFocus={() => setOpen(true)}
        placeholder="Buscar especie o grupo…" />
      {show && (
        <ul id="sugerencias" className="search-list" role="listbox" aria-label="Sugerencias">
          {loading && results.length === 0 && <li className="search-status" role="presentation">Buscando…</li>}
          {!loading && failed && <li className="search-status" role="presentation">No se pudo buscar.</li>}
          {!loading && !failed && results.length === 0 && (
            <li className="search-status" role="presentation">Sin coincidencias. Enter busca «{term}» en todo el árbol.</li>
          )}
          {results.map((r, i) => {
            const note = matchNote(r, term)
            return (
              <li key={r.key} id={`sug-${i}`} role="option" aria-selected={i === active}
                className="search-option" data-rank={r.rank}
                onMouseDown={(e) => { e.preventDefault(); openResult(r) }} onMouseEnter={() => setActive(i)}>
                <span className="dot" style={{ background: kingdomInk(r.kingdom, dark) }} aria-hidden="true" />
                <span className="taxon-name">{highlight(r.name, term)}</span>
                <span className="search-meta">
                  {RANK_ES[r.rank] || r.rank}{r.parent ? ` en ${r.parent}` : ''}{r.kingdom && r.kingdom !== r.parent ? ` · ${r.kingdom}` : ''}
                </span>
                {(note || r.common_names?.length > 0) && (
                  <span className="search-common">{note || r.common_names.slice(0, 2).join(', ')}</span>
                )}
              </li>
            )
          })}
          {results.length > 0 && (
            <li className="search-status search-all" role="presentation">
              <button type="button" className="ctl" onMouseDown={(e) => { e.preventDefault(); go(`/search?q=${encodeURIComponent(term)}`) }}>
                Ver todos los resultados de «{term}»
              </button>
            </li>
          )}
        </ul>
      )}
    </div>
  )
}
