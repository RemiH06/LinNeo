/*
  tetrisPacker -- empaqueta bloques de tamano variable (segun cuantas
  imagenes tiene cada especie) en una grid de N columnas, sin solapar.
  Bin-packing simple: cada bloque busca la primera celda libre (recorriendo
  fila por fila, columna por columna) donde su forma completa quepa sin
  salirse de los limites de columna ni pisar celdas ya ocupadas.

  Formas por cantidad de imagenes (1-5), con rotacion aleatoria cuando hay
  varias orientaciones validas:
    1 -> intenta 3x3, si no cabe 2x2, si no cabe 1x1
    2 -> 2x1 o 1x2 (al azar)
    3 -> forma L de 3 celdas (4 rotaciones, al azar)
    4 -> forma tetris de 4 celdas (L, T, O, S, al azar)
    5 -> forma L de 5 celdas (4 rotaciones, al azar)

  Cada celda de una forma es [dx, dy] relativo a la celda ancla (arriba-izq).
*/

const rnd = (arr) => arr[Math.floor(Math.random() * arr.length)]

// Variantes de L de 3 celdas (todas las rotaciones de una L tromino)
const L3_VARIANTS = [
  [[0, 0], [1, 0], [0, 1]],
  [[0, 0], [1, 0], [1, 1]],
  [[0, 0], [0, 1], [1, 1]],
  [[1, 0], [0, 1], [1, 1]],
]

// Variantes de tetromino de 4 celdas: O, T, L, S (una rotacion representativa
// cada una; "al azar" se resuelve eligiendo entre estas 4 formas distintas)
const TETROMINO_VARIANTS = [
  // O (cuadrado 2x2)
  [[0, 0], [1, 0], [0, 1], [1, 1]],
  // T
  [[0, 0], [1, 0], [2, 0], [1, 1]],
  // L
  [[0, 0], [0, 1], [0, 2], [1, 2]],
  // S
  [[1, 0], [2, 0], [0, 1], [1, 1]],
]

// Variantes de L de 5 celdas (4 rotaciones de una L pentomino, brazo 4+1)
const L5_VARIANTS = [
  [[0, 0], [0, 1], [0, 2], [0, 3], [1, 3]],
  [[0, 0], [1, 0], [2, 0], [3, 0], [0, 1]],
  [[0, 0], [1, 0], [1, 1], [1, 2], [1, 3]],
  [[3, 0], [0, 1], [1, 1], [2, 1], [3, 1]],
]

function shapeFor(count) {
  if (count >= 5) return rnd(L5_VARIANTS)
  if (count === 4) return rnd(TETROMINO_VARIANTS)
  if (count === 3) return rnd(L3_VARIANTS)
  if (count === 2) return rnd([
    [[0, 0], [1, 0]], // 2x1
    [[0, 0], [0, 1]], // 1x2
  ])
  // count === 1 o 0: se resuelve aparte (intenta 3x3 -> 2x2 -> 1x1)
  return [[0, 0]]
}

function shapeWidth(shape) { return Math.max(...shape.map(([dx]) => dx)) + 1 }
function shapeHeight(shape) { return Math.max(...shape.map(([, dy]) => dy)) + 1 }

function squareShape(n) {
  const cells = []
  for (let y = 0; y < n; y++) for (let x = 0; x < n; x++) cells.push([x, y])
  return cells
}

// Busca la primera celda (fila por fila, columna por columna) donde `shape`
// (anclada ahi) cabe sin salirse de `cols` ni pisar celdas ocupadas.
function findFit(occupied, cols, shape, maxRowsToScan) {
  const w = shapeWidth(shape)
  const h = shapeHeight(shape)
  for (let y = 0; y < maxRowsToScan; y++) {
    for (let x = 0; x <= cols - w; x++) {
      const fits = shape.every(([dx, dy]) => !occupied.has(`${x + dx},${y + dy}`))
      if (fits) return { x, y }
    }
  }
  return null
}

function markOccupied(occupied, x, y, shape) {
  let maxY = y
  for (const [dx, dy] of shape) {
    occupied.add(`${x + dx},${y + dy}`)
    maxY = Math.max(maxY, y + dy)
  }
  return maxY
}

/**
 * Empaqueta `items` (cada uno con `.imageCount`, 1-5) en una grid de `cols`
 * columnas. Devuelve [{ item, x, y, shape }] con la posicion y forma final
 * de cada bloque (en unidades de celda, no px).
 */
export function packTetris(items, cols = 5) {
  // orden de colocacion: primero los de 4 imagenes (formas tetris grandes),
  // luego el resto en el orden original que ya traen.
  const withIndex = items.map((item, i) => ({ item, i }))
  const fours = withIndex.filter((x) => x.item.imageCount === 4)
  const rest = withIndex.filter((x) => x.item.imageCount !== 4)
  const ordered = [...fours, ...rest]

  const occupied = new Set()
  const placements = []
  let gridHeight = 0
  // margen de filas a explorar: generoso, crece si se necesita
  let scanRows = Math.max(20, items.length * 2)

  for (const { item } of ordered) {
    const count = item.imageCount || 1
    let shape = null
    let pos = null

    if (count <= 1) {
      // 1 imagen: elige un tamano objetivo al azar (3x3, 2x2 o 1x1) y lo
      // intenta PRIMERO; si no cabe, desciende solo hacia tamanos MAS CHICOS
      // (nunca prueba uno mas grande que el elegido). 1x1 siempre cabe al
      // final. Asi el resultado varia entre bloques chicos y grandes al azar,
      // en vez de siempre salir el mas grande posible.
      const target = rnd([3, 2, 1])
      const sizesToTry = [3, 2, 1].filter((n) => n <= target)
      for (const n of sizesToTry) {
        const candidate = squareShape(n)
        const found = findFit(occupied, cols, candidate, scanRows)
        if (found) { shape = candidate; pos = found; break }
      }
    } else {
      shape = shapeFor(count)
      pos = findFit(occupied, cols, shape, scanRows)
      // si por mala suerte no cabe (grid muy llena), reintenta con 1x1
      if (!pos) {
        shape = [[0, 0]]
        pos = findFit(occupied, cols, shape, scanRows)
      }
    }

    if (!pos) {
      // ultimo recurso: amplia el escaneo y reintenta una vez
      scanRows *= 2
      pos = findFit(occupied, cols, shape || [[0, 0]], scanRows)
    }
    if (!pos) continue // no deberia pasar, pero evita crashear

    const maxY = markOccupied(occupied, pos.x, pos.y, shape)
    gridHeight = Math.max(gridHeight, maxY + 1)
    placements.push({ item, x: pos.x, y: pos.y, shape })
  }

  return { placements, gridHeight, cols }
}