![Made with Python](https://forthebadge.com/images/badges/made-with-python.svg)
![Uses Git](http://ForTheBadge.com/images/badges/uses-git.svg)
![Build with Love](http://ForTheBadge.com/images/badges/built-with-love.svg)

```ascii
██╗     ██╗███╗   ██╗███╗   ██╗███████╗ ██████╗ 
██║     ██║████╗  ██║████╗  ██║██╔════╝██╔═══██╗
██║     ██║██╔██╗ ██║██╔██╗ ██║█████╗  ██║   ██║
██║     ██║██║╚██╗██║██║╚██╗██║██╔══╝  ██║   ██║
███████╗██║██║ ╚████║██║ ╚████║███████╗╚██████╔╝
╚══════╝╚═╝╚═╝  ╚═══╝╚═╝  ╚═══╝╚══════╝ ╚═════╝ 
        by Hex (@RemiH06)        version 1.0
```

![Maintained](https://img.shields.io/badge/Maintained%3F-yes-green.svg?style=for-the-badge)
![MIT](https://img.shields.io/badge/License-MIT-blue.svg?style=for-the-badge)
![Neo4j](https://img.shields.io/badge/Neo4j-5.x-008CC1?style=for-the-badge&logo=neo4j)
![GBIF](https://img.shields.io/badge/Data-GBIF-4CAF50?style=for-the-badge)

## General Description

**LinNeo** is a biodiversity knowledge graph containing **2.8+ million species** with their complete taxonomic hierarchies and geographic distributions. This project was created as a research tool for my book **[100%](https://github.com/RemiH06/100)** (which might be private for now), helping me understand real-world biodiversity patterns to create more authentic invented species and ecosystems.

The graph models the full taxonomic hierarchy (Kingdom → Phylum → Class → Order → Family → Genus → Species) and connects species to the countries where they are found.

```diff
- This project is in early development. Data pipeline scripts are functional but the graph schema may change.
- Requires Neo4j Desktop installed and a running local instance before importing data.
- Processing the full dataset requires ~8GB of heap memory configured in neo4j.conf.
```

## Installation

1. Install requirements:
   `pip install -r requirements.txt`

2. Install and configure [Neo4j Desktop](https://neo4j.com/download/). Create a new instance called **PROYECTO** (or however you want it) and a database called **LinNeo**.

3. Edit `neo4j.conf` inside your instance folder and set:
   ```
   server.memory.heap.initial_size=4G
   server.memory.heap.max_size=8G
   server.memory.pagecache.size=4G
   ```

4. Create a `.secrets` file at the project root with your Neo4j credentials:
   ```
   URI=bolt://localhost:7687
   USER=neo4j
   PASSWORD=your_password
   DATABASE=LinNeo
   ```

5. Download the source data by running:
   `python download_biodiversity_data.py`

   This will populate the `biodiversity_data/` folder with the GBIF Backbone files.

Note: You might want to transform those jupyter notebooks into python scripts, but if you don't is also ok, they work pretty well.

6. Generate the taxonomy node CSVs:
   `python extract_taxonomy_nodes.py`
   OR

   run all on `transform.ipynb`

7. Generate the geographic and relationship CSVs:
   `python generate_relationships.py`
   OR
   run all `relationships.ipynb`

8. Make sure all generated CSVs are already at `import/` in your Neo4j instance.

9. Import into Neo4j by running the two Cypher files in order inside Neo4j Browser:
   - `cypher_import_taxonomy.cypher` (nodes first)
   - `cypher_import_relationships.cypher` (geography and relationships)

## Data Pipeline

```
download_biodiversity_data.py     Downloads raw GBIF Backbone data
extract_taxonomy_nodes.py         Extracts one CSV per taxonomic rank (Kingdom to Species)
generate_relationships.py         Extracts geographic nodes and all relationship CSVs
cypher_import_taxonomy.cypher     Imports taxonomy nodes into Neo4j
cypher_import_relationships.cypher  Imports geography nodes and all relationships
explore.py                        Data exploration and validation (read-only)
example_queries.cypher            Example Cypher queries for common use cases
```

## Graph Schema

```
(Kingdom)-[:HAS_CHILD]->(Phylum)
(Phylum)-[:HAS_CHILD]->(Class)
(Class)-[:HAS_CHILD]->(Order)
(Order)-[:HAS_CHILD]->(Family)
(Family)-[:HAS_CHILD]->(Genus)
(Genus)-[:HAS_CHILD]->(Species)
(Species)-[:FOUND_IN]->(Country)
(Country)-[:PART_OF]->(Continent)
```

## Features

- Full GBIF Backbone taxonomy (~4M species, accepted only)
- Complete taxonomic hierarchy from Kingdom to Species
- Geographic distribution data for all kingdoms
- Country to Continent mapping
- Idempotent Cypher imports (safe to re-run)
- Data validation script to verify pipeline output
- Example queries for endemism, bioregion analysis, and taxonomic exploration

## Future Features

- Web interface for graph exploration (or not, bloom is neat tbh)
- Species similarity scoring based on shared distribution
- Fictional species added by myself (informed by real distribution patterns)
- Export tools for book research queries