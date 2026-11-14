// ============================================================
// LinNeo · color por reino (concepto Systema Naturae)
// La tinta impresa (acento) no cambia por reino. Cada reino aporta su
// pigmento: de día un lavado y una tinta para marcas; de noche, el papel
// virado y una tinta clara. Valores en kingdoms.generated.js (fuente:
// scripts/kingdom-palette.cjs). En CSS se usa data-kingdom + var(--k-ink).
// ============================================================
import { KINGDOMS } from './kingdoms.generated'

const UNKNOWN = 'incertae sedis'
const pick = (kingdom) => KINGDOMS[kingdom] || KINGDOMS[UNKNOWN]

/** Tinta del reino para marcas, nodos y texto corto (≥ 5:1 sobre el papel). */
export const kingdomInk = (kingdom, dark) => (dark ? pick(kingdom).night.ink : pick(kingdom).day.ink)

/** De día, el lavado de acuarela; de noche, el papel virado. */
export const kingdomWash = (kingdom, dark) => (dark ? pick(kingdom).night.paper : pick(kingdom).day.wash)

/** Variables para un elemento que no puede llevar data-kingdom (estilos inline). */
export function kingdomStyleVars(kingdom, dark) {
  return { '--k-ink': kingdomInk(kingdom, dark), '--k-wash': kingdomWash(kingdom, dark) }
}

// Compatibilidad mientras migran las páginas: antes el reino sobrescribía el acento.
export function kingdomAccent(kingdom, dark) {
  const ink = kingdomInk(kingdom, dark)
  return { accent: ink, accent2: ink }
}

// El rango ya no se marca con luminosidad sino con tipografía (itálica en género y especie).
export const nodeColor = (kingdom, _rank, dark) => kingdomInk(kingdom, dark)

// Orden de los reinos en la leyenda y en el grafo de inicio.
export const KINGDOM_WHEEL = [
  'Viruses', 'Archaea', 'Animalia', 'Plantae', 'Chromista', 'Bacteria', 'Fungi', 'Protozoa', 'incertae sedis',
]

// Reinos que agrupa cada dominio.
export const DOMAIN_KINGDOMS = {
  Eukaryota: ['Animalia', 'Plantae', 'Fungi', 'Chromista', 'Protozoa'],
  Prokaryota: ['Archaea', 'Bacteria'],
  Viruses: ['Viruses'],
  'incertae sedis': ['incertae sedis'],
}

// Un color por reino hijo, para pintar un dominio como canicas superpuestas.
export const domainColors = (domain, dark) => (DOMAIN_KINGDOMS[domain] || []).map((k) => kingdomInk(k, dark))
