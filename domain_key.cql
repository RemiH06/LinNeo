// LinNeo -- Estructura superior FINAL: Biota -> Domain -> Kingdom.
// Idempotente y autocorrectivo: deja el registro de domain_key final
// (sin huecos) sin importar el estado previo de la base.
// No toca los nodos Kingdom ni el resto del arbol.

// (1) Raiz Biota
MERGE (b:Biota {name:'Biota'}) SET b.biota_key = 1;

// (2) Dominios finales con su registro de keys renumerado
MERGE (d:Domain {name:'Eukaryota'})      SET d.domain_key = 1;
MERGE (d:Domain {name:'Prokaryota'})     SET d.domain_key = 2;
MERGE (d:Domain {name:'Viruses'})        SET d.domain_key = 3;
MERGE (d:Domain {name:'incertae sedis'}) SET d.domain_key = 4;

// (3) Borrar cualquier Domain viejo que no sea de los cuatro finales
//     (p.ej. Bacteria/Archaea de versiones previas). Solo etiqueta Domain.
MATCH (d:Domain)
WHERE NOT d.name IN ['Eukaryota','Prokaryota','Viruses','incertae sedis']
DETACH DELETE d;

// (4) Biota -> Domains
MATCH (b:Biota {name:'Biota'}), (d:Domain)
MERGE (b)-[:HAS_CHILD]->(d);

// (5) Domain -> Kingdom
MATCH (d:Domain {name:'Eukaryota'}), (k:Kingdom)
WHERE k.name IN ['Animalia','Plantae','Fungi','Chromista','Protozoa']
MERGE (d)-[:HAS_CHILD]->(k);

MATCH (d:Domain {name:'Prokaryota'}), (k:Kingdom)
WHERE k.name IN ['Bacteria','Archaea']
MERGE (d)-[:HAS_CHILD]->(k);

MATCH (d:Domain {name:'Viruses'}),        (k:Kingdom {name:'Viruses'})        MERGE (d)-[:HAS_CHILD]->(k);
MATCH (d:Domain {name:'incertae sedis'}), (k:Kingdom {name:'incertae sedis'}) MERGE (d)-[:HAS_CHILD]->(k);

// (6) Verificacion: registro final y reparto
MATCH (b:Biota)-[:HAS_CHILD]->(d:Domain)
OPTIONAL MATCH (d)-[:HAS_CHILD]->(k:Kingdom)
RETURN d.name AS dominio, d.domain_key AS key, collect(k.name) AS reinos
ORDER BY key;
