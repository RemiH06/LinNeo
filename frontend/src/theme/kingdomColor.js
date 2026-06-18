// ============================================================
// LinNeo -- Color por reino
// hue = reino; los shades se derivan fijando el hue y reusando la
// relacion luz/saturacion del acento elixir (#00E8D8 dark / #0A5C58 light).
// Tambien: color de nodo del grafo = hue del reino + luminosidad por rango.
// ============================================================

// Matiz (hue 0-360) por reino. 'incertae sedis' usa saturacion ~0 (gris).
export const KINGDOM_HUE = {
  Archaea: 28,
  Animalia: 48,
  Plantae: 135,
  Chromista: 172,
  Bacteria: 212,
  Fungi: 280,
  Protozoa: 328,
  Viruses: 2,
  'incertae sedis': null, // gris
}

// Saturacion especial por reino (por defecto la del tema). Gris = baja.
function satFor(kingdom, base) {
  if (kingdom === 'incertae sedis' || KINGDOM_HUE[kingdom] == null) return 8
  return base
}

function hueFor(kingdom) {
  const h = KINGDOM_HUE[kingdom]
  return h == null ? 210 : h // gris: hue neutro, sat baja lo vuelve gris
}

// Acento del reino para tintar la vista. Replica L/S del acento elixir:
//   dark  accent  ~ hsl(h, 100%, 45%)
//   light accent  ~ hsl(h, 80%, 22%)
export function kingdomAccent(kingdom, dark) {
  const h = hueFor(kingdom)
  if (dark) {
    return {
      accent: `hsl(${h}, ${satFor(kingdom, 100)}%, 46%)`,
      accent2: `hsl(${h}, ${satFor(kingdom, 70)}%, 60%)`,
    }
  }
  return {
    accent: `hsl(${h}, ${satFor(kingdom, 80)}%, 24%)`,
    accent2: `hsl(${h}, ${satFor(kingdom, 60)}%, 38%)`,
  }
}

// Devuelve un objeto de estilo con las variables --accent/--accent2 sobreescritas,
// para envolver la vista y que toda la UI se tinte al reino.
export function kingdomStyleVars(kingdom, dark) {
  const { accent, accent2 } = kingdomAccent(kingdom, dark)
  return { '--accent': accent, '--accent2': accent2 }
}

// Color de un nodo del grafo: hue del reino, luminosidad segun rango.
// Kingdom mas oscuro -> Species mas claro (degradado por profundidad).
const RANK_LIGHT = {
  kingdom: 30, phylum: 38, class: 46, order: 54, family: 62, genus: 70, species: 78,
}
export function nodeColor(kingdom, rank, dark) {
  const h = hueFor(kingdom)
  const r = (rank || 'species').toLowerCase()
  const baseL = RANK_LIGHT[r] ?? 60
  const L = dark ? baseL : Math.max(22, baseL - 28) // en claro, mas oscuro para contraste
  const S = satFor(kingdom, dark ? 85 : 70)
  return `hsl(${h}, ${S}%, ${L}%)`
}

// Lista ordenada de reinos en circulo de color (para el grafote de shui).
export const KINGDOM_WHEEL = [
  'Viruses', 'Archaea', 'Animalia', 'Plantae', 'Chromista', 'Bacteria', 'Fungi', 'Protozoa', 'incertae sedis',
]

// Reinos (Kingdom) que agrupa cada Domain. Determina el patron de color del nodo Domain.
export const DOMAIN_KINGDOMS = {
  Eukaryota: ['Animalia', 'Plantae', 'Fungi', 'Chromista', 'Protozoa'],
  Prokaryota: ['Archaea', 'Bacteria'],
  Viruses: ['Viruses'],
  'incertae sedis': ['incertae sedis'],
}

// Colores (uno por kingdom hijo) para pintar un Domain como 'canicas' superpuestas.
// Eukaryota/Prokaryota -> varios colores (uno por reino). Viruses/incertae sedis ->
// un solo color fijo (su propio hue/gris), sin necesidad de patron.
export function domainColors(domain, dark) {
  const kingdoms = DOMAIN_KINGDOMS[domain] || []
  return kingdoms.map((k) => nodeColor(k, 'kingdom', dark))
}