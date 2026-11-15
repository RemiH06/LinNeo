import { Link } from 'react-router-dom'
import { IUCN } from '../api/client'

// Una entrada de lista bajo la llave: nombre con enlace y, en especies, el nombre
// común y la categoría UICN si está amenazada; en taxones superiores, sus especies.
const THREATENED = new Set(['VU', 'EN', 'CR', 'EW', 'EX'])
const fmt = (n) => (n ?? 0).toLocaleString('es-MX')
const hrefOf = (t) => (t.rank === 'species' ? `/species/${t.key}` : `/taxon/${t.rank}/${t.key}`)

export default function TaxonEntry({ child }) {
  const sp = (child.rank || 'species') === 'species'
  return (
    <li data-rank={child.rank || 'species'}>
      <Link className="taxon-name" to={hrefOf(child)}>{child.name}</Link>
      {sp && child.common_names?.[0] && <span className="entry-note">{child.common_names[0]}</span>}
      {sp && THREATENED.has(child.conservation) && (
        <abbr className="iucn" title={IUCN[child.conservation]?.label}>{child.conservation}</abbr>
      )}
      {!sp && <span className="entry-note">{child.flags?.species ? `${fmt(child.flags.species)} esp.` : 'sin especies'}</span>}
    </li>
  )
}
