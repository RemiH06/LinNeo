/*
  soundTreeLayout -- calcula posiciones x,y para un arbol jerarquico
  HORIZONTAL (raiz a la izquierda, crece hacia la derecha por nivel), usado
  por SoundTree. No es fuerza-dirigido: es un layout determinista tipo
  organigrama, para que el arbol sea estatico (no movible) y predecible.

  Los niveles (profundidad) son columnas de ancho fijo (LEVEL_GAP), asi el
  arbol siempre tiene el mismo tamano de nodo sin importar cuantos hermanos
  haya -- el ancho total depende solo de la profundidad, nunca de la
  cantidad de hojas. Las hojas (y sus ancestros) se apilan verticalmente
  (LEAF_GAP por fila), creciendo hacia abajo sin limite -- la pagina
  scrollea verticalmente, nunca hace falta scroll horizontal.

  Algoritmo: postorder simple, ejes intercambiados respecto al layout
  vertical original: lo que antes era X (posicion entre hermanos) ahora es
  Y; lo que antes era Y (profundidad por nivel) ahora es X.
*/

const LEAF_GAP = 110   // separacion vertical minima entre hojas vecinas (filas)
const LEVEL_GAP = 130  // separacion horizontal entre niveles del arbol (columnas)

export function layoutTree(nodes, edges, rootId) {
  const byId = Object.fromEntries(nodes.map((n) => [n.id, n]))
  const childrenOf = {}
  for (const e of edges) {
    (childrenOf[e.source] ||= []).push(e.target)
  }

  let nextLeafY = 0
  const positions = {}
  const depthOf = {}

  // postorder: asigna Y a las hojas en orden de aparicion (filas), los
  // padres se centran sobre el rango Y de sus hijos despues de visitarlos.
  function visit(id, depth) {
    depthOf[id] = depth
    const kids = childrenOf[id] || []
    if (kids.length === 0) {
      const y = nextLeafY
      nextLeafY += LEAF_GAP
      positions[id] = { x: depth * LEVEL_GAP, y }
      return y
    }
    const childYs = kids.map((cid) => visit(cid, depth + 1))
    const y = (Math.min(...childYs) + Math.max(...childYs)) / 2
    positions[id] = { x: depth * LEVEL_GAP, y }
    return y
  }

  if (byId[rootId]) visit(rootId, 0)

  const maxDepth = Math.max(0, ...Object.values(depthOf))
  const height = Math.max(LEAF_GAP, nextLeafY)
  const width = (maxDepth + 1) * LEVEL_GAP

  return {
    positions, // { [id]: {x, y} }
    width,
    height,
    childrenOf,
  }
}