import { useEffect, useState } from 'react'
import { Link, useNavigate, useSearchParams } from 'react-router-dom'
import { api } from '../api/client'
import { useTheme } from '../theme/ThemeContext'
import { useSetKingdom } from '../theme/KingdomContext'
import { useKeyboardShortcuts } from '../hooks/useKeyboardShortcuts'
import SiteHeader from '../components/SiteHeader'
import SearchBox, { CLADE_MIN, hrefOf, highlight, matchNote } from '../components/SearchBox'
import Brace from '../components/Brace'
import '../theme/taxon.css'

// Resultados de búsqueda en dos partes: coincidencias del índice de texto
// completo (nombre, sinónimo, nombre común) y nombres científicos que contienen
// la cadena, agrupados por rango bajo la llave.

const RANKS = ['domain', 'kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species']
const RANK_ES = {
  domain: ['Dominio', 'Dominios'], kingdom: ['Reino', 'Reinos'], phylum: ['Filo', 'Filos'], class: ['Clase', 'Clases'],
  order: ['Orden', 'Órdenes'], family: ['Familia', 'Familias'], genus: ['Género', 'Géneros'], species: ['Especie', 'Especies'],
}
const rankName = (r, n = 1) => RANK_ES[r]?.[n === 1 ? 0 : 1] ?? r
const fmt = (n) => (n ?? 0).toLocaleString('es-MX')
const PAGE = 25
const CONTENT = [['images', 'imagen'], ['descriptions', 'texto'], ['sounds', 'sonido'], ['common_names', 'nombre común']]
const hasContent = (it, k) => (k === 'common_names' ? it.common_names?.length > 0 : it.flags?.[k] > 0)

function Where({ r }) {
  return (
    <span className="entry-note">
      {rankName(r.rank)}{r.parent ? ` en ${r.parent}` : ''}{r.kingdom && r.kingdom !== r.parent && r.rank !== 'kingdom' ? ` · ${r.kingdom}` : ''}
    </span>
  )
}

export default function SearchResults() {
  const [params] = useSearchParams()
  const q = (params.get('q') || '').trim()
  const navigate = useNavigate()
  const { toggle: toggleTheme } = useTheme()
  useSetKingdom(null)
  useKeyboardShortcuts({ navigate, toggleTheme })

  // Coincidencias del índice de texto completo, paginadas.
  const [hits, setHits] = useState([])
  const [hitsTotal, setHitsTotal] = useState(0)
  const [hitsState, setHitsState] = useState('idle')   // idle | loading | done | error

  // Nombres científicos por rango: ocho llamadas que pintan en cuanto responden.
  const [mode, setMode] = useState('contains')
  const [byRank, setByRank] = useState({})
  const [rankState, setRankState] = useState({})
  const [only, setOnly] = useState({})

  useEffect(() => {
    if (!q) return
    let alive = true
    setHits([]); setHitsTotal(0); setHitsState('loading')
    api.search(q, PAGE, 0)
      .then((d) => { if (alive) { setHits(d.results); setHitsTotal(d.total); setHitsState('done') } })
      .catch(() => { if (alive) setHitsState('error') })
    return () => { alive = false }
  }, [q])

  const clades = q.length >= CLADE_MIN
  useEffect(() => {
    setByRank({}); setOnly({}); setRankState({})
    if (!clades) return
    let alive = true
    setRankState(Object.fromEntries(RANKS.map((r) => [r, 'loading'])))
    for (const rank of RANKS) {
      api.searchCladesByRank(rank, q, 100, mode)
        .then((d) => { if (alive) { setByRank((p) => ({ ...p, [rank]: (d?.groups || []).flatMap((g) => g.items) })); setRankState((p) => ({ ...p, [rank]: 'done' })) } })
        .catch(() => { if (alive) setRankState((p) => ({ ...p, [rank]: 'error' })) })
    }
    return () => { alive = false }
  }, [q, mode, clades])

  const moreHits = () => {
    setHitsState('loading')
    api.search(q, PAGE, hits.length)
      .then((d) => { setHits((p) => [...p, ...d.results]); setHitsState('done') })
      .catch(() => setHitsState('error'))
  }

  const pending = RANKS.some((r) => rankState[r] === 'loading')
  const failed = RANKS.filter((r) => rankState[r] === 'error')
  const cladeTotal = RANKS.reduce((s, r) => s + (byRank[r]?.length || 0), 0)
  const anyOnly = Object.values(only).some(Boolean)
  const speciesOk = (it) => it.rank !== 'species' || CONTENT.every(([k]) => !only[k] || hasContent(it, k))
  const groups = RANKS.map((r) => ({ rank: r, all: byRank[r] || [], items: (byRank[r] || []).filter(speciesOk) }))
    .filter((g) => g.items.length > 0)
  const verb = mode === 'starts' ? 'empiezan con' : 'contienen'

  return (
    <div className="folio">
      <SiteHeader><SearchBox initial={q} /></SiteHeader>

      <header className="taxon-head">
        <p className="taxon-rank">Búsqueda</p>
        <h1>«{q || '…'}»</h1>
        {q && (
          <p className="taxon-summary" aria-live="polite">
            {hitsState === 'done' && `${fmt(hitsTotal)} ${hitsTotal === 1 ? 'coincidencia' : 'coincidencias'} por nombre, sinónimo o nombre común${hitsTotal >= 200 ? ' (las 200 mejores)' : ''}. `}
            {clades && !pending && `${fmt(cladeTotal)} nombres científicos ${verb} «${q}».`}
            {pending && 'Buscando en el árbol…'}
          </p>
        )}
      </header>

      {!q && <p className="taxon-count">Escribe un nombre en el buscador de arriba.</p>}

      {q && (
        <section className="sr-section" aria-labelledby="coinc">
          <h2 id="coinc" className="folio-h2">Coincidencias</h2>
          {hitsState === 'error' && <p className="taxon-error" role="alert">No se pudo consultar el índice.</p>}
          {hitsState === 'done' && hits.length === 0 && <p className="taxon-count">Ningún nombre, sinónimo ni nombre común coincide.</p>}
          <ol className="sr-hits">
            {hits.map((r) => {
              const note = matchNote(r, q)
              return (
                <li key={r.key} data-rank={r.rank}>
                  <Link className="taxon-name" to={hrefOf(r)}>{highlight(r.name, q)}</Link> <Where r={r} />
                  {(note || r.common_names?.length > 0) && (
                    <span className="sr-common">{note || r.common_names.slice(0, 4).join(', ')}</span>
                  )}
                </li>
              )
            })}
          </ol>
          {hits.length < hitsTotal && (
            <button type="button" className="ctl load-more" onClick={moreHits} disabled={hitsState === 'loading'}>
              {hitsState === 'loading' ? 'Cargando…' : `Ver ${fmt(Math.min(PAGE, hitsTotal - hits.length))} más`}
            </button>
          )}
        </section>
      )}

      {q && !clades && (
        <p className="taxon-count">Con {CLADE_MIN} letras o más también se busca dentro de los nombres científicos.</p>
      )}

      {clades && (
        <section className="sr-section" aria-labelledby="nombres">
          <h2 id="nombres" className="folio-h2">Nombres científicos que {verb} «{q}»</h2>
          <div className="taxon-tools">
            <div role="group" aria-label="Cómo comparar" className="tool-group">
              <button type="button" className="ctl" aria-pressed={mode === 'contains'} onClick={() => setMode('contains')}>Contienen</button>
              <button type="button" className="ctl" aria-pressed={mode === 'starts'} onClick={() => setMode('starts')}
                title="En especies se compara el epíteto, porque la primera palabra es el género">Empiezan con</button>
            </div>
            {byRank.species?.length > 0 && (
              <div role="group" aria-labelledby="sp-solo" className="tool-group">
                <span id="sp-solo" className="tool-label">Especies solo con</span>
                {CONTENT.map(([k, label]) => (
                  <label key={k}>
                    <input type="checkbox" checked={!!only[k]} onChange={(e) => setOnly((o) => ({ ...o, [k]: e.target.checked }))} />
                    {label}
                  </label>
                ))}
                {anyOnly && <button type="button" className="ctl" onClick={() => setOnly({})}>Quitar filtros</button>}
              </div>
            )}
          </div>
          {failed.length > 0 && <p className="taxon-error" role="alert">No se pudo buscar en: {failed.map((r) => rankName(r, 2).toLowerCase()).join(', ')}.</p>}
          {!pending && cladeTotal === 0 && <p className="taxon-count">Ningún nombre científico {mode === 'starts' ? 'empieza con' : 'contiene'} «{q}».</p>}

          {groups.map((g) => (
            <div className="rank-group" key={g.rank}>
              <div className="rank-label">
                <h3>{rankName(g.rank, g.all.length)}</h3>
                <span className="n">{anyOnly && g.rank === 'species' ? `${fmt(g.items.length)} de ${fmt(g.all.length)}` : fmt(g.all.length)}</span>
              </div>
              <Brace>
                <ul className="entries sr-entries">
                  {g.items.map((it) => (
                    <li key={it.key} data-rank={it.rank}>
                      <Link className="taxon-name" to={hrefOf(it)}>{highlight(it.name, q)}</Link>
                      <span className="entry-note">{it.parent ? `en ${it.parent}` : ''}{it.kingdom && g.rank !== 'kingdom' && it.kingdom !== it.parent ? `${it.parent ? ' · ' : ''}${it.kingdom}` : ''}</span>
                    </li>
                  ))}
                </ul>
              </Brace>
            </div>
          ))}
          {pending && <p className="taxon-count">Buscando en {RANKS.filter((r) => rankState[r] === 'loading').map((r) => rankName(r, 2).toLowerCase()).join(', ')}…</p>}
        </section>
      )}
    </div>
  )
}
