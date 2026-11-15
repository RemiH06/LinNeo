// Hábito y hábitat (POWO y FishBase vienen en inglés) en español.
// El vocabulario es combinatorio ("climbing tuberous geophyte", "shrub or tree",
// "brackish/marine"), así que se traduce por palabras: el sustantivo va primero y
// los modificadores concuerdan con su género. Si aparece una palabra que no está
// en el diccionario, esa parte se deja en inglés en vez de inventar.

const NOUN = {   // [español, género]
  tree: ['árbol', 'm'], shrub: ['arbusto', 'm'], subshrub: ['subarbusto', 'm'], geophyte: ['geófito', 'm'],
  hydrogeophyte: ['geófito acuático', 'm'], hydrosubshrub: ['subarbusto acuático', 'm'],
  epiphyte: ['epífita', 'f'], lithophyte: ['litófita', 'f'], helophyte: ['helófita', 'f'], liana: ['liana', 'f'],
  climber: ['trepadora', 'f'], bamboo: ['bambú', 'm'], holoparasite: ['holoparásita', 'f'],
  perennial: ['hierba perenne', 'f'], annual: ['hierba anual', 'f'], biennial: ['hierba bienal', 'f'],
  hydroperennial: ['hierba perenne acuática', 'f'], hydroannual: ['hierba anual acuática', 'f'],
}
const ADJ = {    // [masculino, femenino]
  climbing: ['trepador', 'trepadora'], scrambling: ['sarmentoso', 'sarmentosa'], succulent: ['suculento', 'suculenta'],
  tuberous: ['tuberoso', 'tuberosa'], rhizomatous: ['rizomatoso', 'rizomatosa'], bulbous: ['bulboso', 'bulbosa'],
  pseudobulbous: ['pseudobulboso', 'pseudobulbosa'], epiphytic: ['epífito', 'epífita'],
  hemiparasitic: ['hemiparásito', 'hemiparásita'], holoparasitic: ['holoparásito', 'holoparásita'],
  holomycotrophic: ['micoheterótrofo', 'micoheterótrofa'], monocarpic: ['monocárpico', 'monocárpica'],
  perennial: ['perenne', 'perenne'], annual: ['anual', 'anual'], biennial: ['bienal', 'bienal'],
}
const WATER = {  // FishBase: medio y posición en la columna de agua
  marine: 'marino', freshwater: 'de agua dulce', brackish: 'de agua salobre', demersal: 'demersal',
  benthopelagic: 'bentopelágico', bathydemersal: 'batidemersal', bathypelagic: 'batipelágico', pelagic: 'pelágico',
  'pelagic-neritic': 'pelágico nerítico', 'pelagic-oceanic': 'pelágico oceánico', 'reef-associated': 'de arrecife',
}

function phrase(words) {
  const ws = words.toLowerCase().trim().split(/\s+/)
  if (ws.length === 1 && WATER[ws[0]]) return WATER[ws[0]]
  const noun = NOUN[ws[ws.length - 1]]
  if (!noun) return null
  const mods = ws.slice(0, -1).map((w) => ADJ[w]?.[noun[1] === 'f' ? 1 : 0])
  if (mods.some((m) => !m)) return null
  return [noun[0], ...mods].join(' ')
}

/** "marine; demersal" → "Marino; demersal"; "shrub or tree" → "Arbusto o árbol". */
export function habitEs(text) {
  if (!text) return null
  const parts = text.split(';').map((part) => {
    const alts = part.split(/\s+or\s+|\//).map((a) => a.trim()).filter((a) => a && a.toLowerCase() !== 'unknown')
    if (!alts.length) return null
    const es = alts.map(phrase)
    return es.every(Boolean) ? es.join(' o ') : part.trim()
  }).filter(Boolean)
  if (!parts.length) return null
  const out = parts.join('; ')
  return out.charAt(0).toUpperCase() + out.slice(1)
}
