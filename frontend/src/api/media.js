// Imágenes remotas. Las de Wikimedia Commons se piden como miniatura en un
// ancho estándar: el original puede pesar varios MB y Wikimedia responde 429
// a quien baja originales de más. Cualquier otra URL pasa tal cual.
const COMMONS = /^https:\/\/upload\.wikimedia\.org\/wikipedia\/commons\/([0-9a-f])\/([0-9a-f]{2})\/([^/]+)$/
const STEPS = [120, 250, 330, 500, 960, 1280]   // anchos de miniatura que Wikimedia sirve de caché
const RASTER = new Set(['jpg', 'jpeg', 'png', 'gif', 'webp'])

export function thumb(url, width = 330) {
  const m = url?.match(COMMONS)
  if (!m) return url
  const [, a, ab, file] = m
  const ext = file.split('.').pop().toLowerCase()
  const suffix = ext === 'svg' ? '.png' : RASTER.has(ext) ? '' : null   // TIFF, PDF, etc.: el original
  if (suffix === null) return url
  const w = STEPS.find((s) => s >= width) ?? STEPS[STEPS.length - 1]
  return `https://upload.wikimedia.org/wikipedia/commons/thumb/${a}/${ab}/${file}/${w}px-${file}${suffix}`
}
