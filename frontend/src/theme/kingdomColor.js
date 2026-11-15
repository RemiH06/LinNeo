// ============================================================
// LinNeo · color por reino (concepto Systema Naturae)
// La tinta impresa (acento) no cambia por reino. Cada reino aporta su
// pigmento: de día un lavado y una tinta para marcas; de noche, el papel
// virado y una tinta clara. Valores en kingdoms.generated.js (fuente:
// scripts/kingdom-palette.cjs). En CSS se usa data-kingdom + var(--k-ink);
// esto es para lo que se pinta desde JS (canvas del grafo, puntos de reino).
// ============================================================
import { KINGDOMS } from './kingdoms.generated'

const UNKNOWN = 'incertae sedis'
const pick = (kingdom) => KINGDOMS[kingdom] || KINGDOMS[UNKNOWN]

/** Tinta del reino para marcas, nodos y texto corto (≥ 5:1 sobre el papel). */
export const kingdomInk = (kingdom, dark) => (dark ? pick(kingdom).night.ink : pick(kingdom).day.ink)

// Reinos que agrupa cada dominio.
const DOMAIN_KINGDOMS = {
  Eukaryota: ['Animalia', 'Plantae', 'Fungi', 'Chromista', 'Protozoa'],
  Prokaryota: ['Archaea', 'Bacteria'],
  Viruses: ['Viruses'],
  'incertae sedis': ['incertae sedis'],
}

/** Un color por reino hijo, para pintar un dominio como puntos de pigmento juntos. */
export const domainColors = (domain, dark) => (DOMAIN_KINGDOMS[domain] || []).map((k) => kingdomInk(k, dark))
