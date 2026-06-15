import { useState } from 'react'
import { useNavigate } from 'react-router-dom'

// Arbol de parentesco de dos niveles: Familia > generos hermanos / genero > especies hermanas.
export default function RelativesTree({ relatives, currentName }) {
  const [openGenera, setOpenGenera] = useState(false)
  const [openSpecies, setOpenSpecies] = useState(true)
  const navigate = useNavigate()

  if (!relatives || (!relatives.genus && !relatives.family)) return null

  const { family, genus, sibling_genera = [], sibling_species = [] } = relatives

  const Row = ({ children, depth = 0, onClick, dim, current }) => (
    <div
      onClick={onClick}
      style={{
        paddingLeft: 10 + depth * 14,
        paddingTop: 3, paddingBottom: 3,
        fontSize: 11,
        cursor: onClick ? 'pointer' : 'default',
        color: current ? 'var(--accent)' : dim ? 'var(--text2)' : 'var(--text)',
        fontStyle: depth >= 2 ? 'italic' : 'normal',
        borderLeft: current ? '2px solid var(--accent)' : '2px solid transparent',
        whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
      }}
      title={typeof children === 'string' ? children : undefined}
    >
      {children}
    </div>
  )

  const caret = (open) => (open ? '\u25BE' : '\u25B8')

  return (
    <div style={{ fontFamily: 'var(--mono)' }}>
      {family?.name && (
        <Row depth={0}>
          <span className="pipeline-step" style={{ marginRight: 6 }}>FAM</span>{family.name}
        </Row>
      )}

      {/* Generos hermanos (colapsable) */}
      {sibling_genera.length > 0 && (
        <Row depth={1} dim onClick={() => setOpenGenera((v) => !v)}>
          {caret(openGenera)} generos hermanos ({sibling_genera.length})
        </Row>
      )}
      {openGenera && sibling_genera.map((g, i) => (
        <Row key={`g${i}`} depth={2} dim>{g.name}</Row>
      ))}

      {/* Genero propio */}
      {genus?.name && (
        <Row depth={1}>
          <span className="pipeline-step" style={{ marginRight: 6 }}>GEN</span>{genus.name}
        </Row>
      )}

      {/* Especies hermanas (colapsable, navegables) */}
      {sibling_species.length > 0 && (
        <Row depth={2} dim onClick={() => setOpenSpecies((v) => !v)}>
          {caret(openSpecies)} especies hermanas ({sibling_species.length})
        </Row>
      )}
      {openSpecies && sibling_species.map((sp, i) => (
        <Row
          key={`s${i}`}
          depth={3}
          onClick={sp.key ? () => navigate(`/species/${sp.key}`) : undefined}
        >
          {sp.name}
        </Row>
      ))}

      {/* La especie actual, resaltada, al nivel de sus hermanas */}
      <Row depth={3} current>{currentName} {'\u25C0'}</Row>
    </div>
  )
}