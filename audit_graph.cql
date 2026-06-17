// LinNeo -- Auditoria de integridad: nodos sueltos (huerfanos).
// Corre cada bloque por separado y comparte los resultados.
// Un nodo esta "suelto" si no tiene padre HAS_CHILD. Todo nodo taxonomico
// deberia tener uno, salvo los Kingdom (que son la raiz del arbol).

// (1) Listado final de Kingdoms. Debe aparecer Fungi con key 9 (total: 9).
MATCH (k:Kingdom)
RETURN k.kingdom_key AS key, k.name AS name
ORDER BY key;

// (2) El Kingdom Fungi y cuantos hijos directos quedo colgando.
MATCH (k:Kingdom {name:'Fungi'})
OPTIONAL MATCH (k)-[:HAS_CHILD]->(c)
RETURN k.kingdom_key AS key, count(c) AS hijos_directos;

// (3) PRINCIPAL -- huerfanos por rango taxonomico (nodos sin padre HAS_CHILD).
//     Esta es la respuesta directa a "hay nodos sueltos?".
//     Lo ideal: 0 en todos los rangos (o solo residuales de otros reinos).
MATCH (n)
WHERE (n:Phylum OR n:Class OR n:Order OR n:Family OR n:Genus OR n:Species)
  AND NOT ()-[:HAS_CHILD]->(n)
RETURN labels(n)[0] AS rango, count(*) AS huerfanos
ORDER BY huerfanos DESC;

// (4) Familias huerfanas restantes, desglosadas por el reino de sus especies.
//     Revela si las que quedan son de Fungi (mi fix fallo en algo) o de OTROS
//     reinos (mismo patron incertae sedis, que no toque). Puede tardar un poco.
MATCH (f:Family) WHERE NOT ()-[:HAS_CHILD]->(f)
OPTIONAL MATCH (f)-[:HAS_CHILD]->(:Genus)-[:HAS_CHILD]->(s:Species)
WITH f, collect(DISTINCT s.kingdom)[0] AS reino
RETURN coalesce(reino, '(sin especies)') AS reino, count(*) AS familias_huerfanas
ORDER BY familias_huerfanas DESC;

// (5) Especies huerfanas (sin ningun padre). Deberia ser 0 o muy bajo.
MATCH (s:Species) WHERE NOT ()-[:HAS_CHILD]->(s)
RETURN count(s) AS especies_huerfanas;

// (6) Especies de Fungi efectivamente alcanzables desde el Kingdom Fungi.
//     Confirma que el subarbol entero quedo conectado a la raiz.
//     (Acotado por rangos para que no recorra de mas.)
MATCH (k:Kingdom {name:'Fungi'})
      -[:HAS_CHILD*1..6]->(s:Species)
RETURN count(DISTINCT s) AS especies_fungi_conectadas;
