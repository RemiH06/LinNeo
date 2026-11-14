"""Lo que el backbone trae además del árbol, leído directo de sus TSV.

Reemplaza a backbone_extras.py y a la parte de distribución de
download_biodiversity_data.py: mismas reglas, sin CSV intermedios. Cada
relación sale con `species_key` (taxonID del backbone) para que 03_enrich la
resuelva a su nodo.
"""
import html
import re

from config import BACKBONE

TAG = re.compile(r"<[^>]+>")
SPACE = re.compile(r"\s+")

# Tipos narrativos útiles -> tipo normalizado.
DESCRIPTION_TYPES = {
    "description": "description", "general": "description",
    "general description": "description", "morphology": "description",
    "diagnosis": "diagnosis", "habitat": "habitat", "etymology": "etymology",
    "biology_ecology": "biology", "biology": "biology", "ecology": "biology",
    "discussion": "discussion", "habit": "habit",
}
IMAGE_EXTENSIONS = ("png", "jpg", "jpeg", "gif", "svg", "tif", "tiff")
REFERENCES_PER_TAXON = 5

# threatStatus de IUCN -> (código, severidad). Mayor severidad = peor.
IUCN = {
    "least concern": ("LC", 1), "near threatened": ("NT", 2), "vulnerable": ("VU", 3),
    "endangered": ("EN", 4), "critically endangered": ("CR", 5),
    "regionally extinct": ("RE", 6), "extinct in the wild": ("EW", 7), "extinct": ("EX", 8),
    "data deficient": ("DD", 0), "not evaluated": ("NE", 0), "not applicable": ("NA", 0),
}
# Al colapsar (especie, país) repetidos gana el origen más "propio".
ESTABLISHMENT = {"native": 6, "naturalised": 5, "invasive": 4, "introduced": 3,
                 "uncertain": 2, "managed": 1}


def clean_text(value):
    """Quita HTML, decodifica entidades y colapsa espacios."""
    if not value:
        return ""
    return SPACE.sub(" ", html.unescape(TAG.sub(" ", value))).strip()


def register(con):
    con.create_function("clean_text", clean_text, ["VARCHAR"], "VARCHAR", null_handling="special")


def tsv(table):
    return (f"read_csv('{(BACKBONE / f'{table}.tsv').as_posix()}', delim='\t', header=true, "
            "quote='', escape='', all_varchar=true, null_padding=true, strict_mode=false)")


def values(mapping):
    rows = ", ".join(f"('{k}', " + ", ".join(repr(x) for x in (v if isinstance(v, tuple) else (v,))) + ")"
                     for k, v in mapping.items())
    return f"(VALUES {rows})"


DESCRIPTIONS = f"""(
    SELECT * FROM (
        SELECT trim(d.taxonID) AS species_key, m.kind AS type,
               lower(trim(coalesce(d.language, ''))) AS language,
               clean_text(d.description) AS text, clean_text(d.source) AS source_name
        FROM {tsv('Description')} d
        JOIN {values(DESCRIPTION_TYPES)} m(raw, kind) ON lower(trim(d.type)) = m.raw
    )
    WHERE species_key <> '' AND length(text) >= 10
    QUALIFY row_number() OVER (PARTITION BY species_key, type, left(text, 120) ORDER BY text) = 1
)"""

VERNACULAR_ES = f"""(
    SELECT * FROM (
        SELECT trim(taxonID) AS species_key, clean_text(vernacularName) AS vernacular_name
        FROM {tsv('VernacularName')}
        WHERE lower(trim(language)) = 'es'
    )
    WHERE species_key <> '' AND vernacular_name <> ''
    QUALIFY row_number() OVER (PARTITION BY species_key, lower(vernacular_name)
                               ORDER BY vernacular_name) = 1
)"""

IMAGES = f"""(
    SELECT trim(taxonID) AS species_key, replace(trim(identifier), '"', '%22') AS url,
           clean_text(title) AS title, clean_text(creator) AS creator,
           clean_text(license) AS license, clean_text(source) AS source_name
    FROM {tsv('Multimedia')}
    WHERE trim(taxonID) <> ''
      AND lower(regexp_extract(trim(identifier), '\\.([a-zA-Z0-9]{{2,4}})(?:\\?|$)', 1))
          IN {IMAGE_EXTENSIONS}
)"""

REFERENCES = f"""(
    SELECT * FROM (
        SELECT trim(taxonID) AS species_key, clean_text(bibliographicCitation) AS text,
               clean_text(source) AS source_name
        FROM {tsv('Reference')}
    )
    WHERE species_key <> '' AND length(text) >= 10
    QUALIFY row_number() OVER (PARTITION BY species_key ORDER BY text) <= {REFERENCES_PER_TAXON}
)"""

TYPES = f"""(
    SELECT * FROM (
        SELECT trim(taxonID) AS species_key,
               concat_ws('; ', nullif(clean_text(scientificName), ''),
                         'designado por ' || nullif(clean_text(typeDesignatedBy), '')) AS text,
               clean_text(source) AS source_name
        FROM {tsv('TypesAndSpecimen')}
    )
    WHERE species_key <> '' AND text <> ''
    QUALIFY row_number() OVER (PARTITION BY species_key, lower(text) ORDER BY text) = 1
)"""

_DISTRIBUTION = f"""(
    SELECT trim(d.taxonID) AS species_key, upper(trim(coalesce(d.countryCode, ''))) AS country,
           trim(coalesce(d.locality, '')) AS locality,
           lower(trim(coalesce(d.establishmentMeans, ''))) AS establishment_means,
           lower(trim(coalesce(d.occurrenceStatus, ''))) AS occurrence_status,
           lower(trim(coalesce(d.threatStatus, ''))) AS threat,
           coalesce(i.code, '') AS code, coalesce(i.severity, 0) AS severity,
           coalesce(e.priority, 0) AS priority
    FROM {tsv('Distribution')} d
    LEFT JOIN {values(IUCN)} i(status, code, severity)
           ON lower(trim(d.threatStatus)) = i.status
    LEFT JOIN {values(ESTABLISHMENT)} e(means, priority)
           ON lower(trim(d.establishmentMeans)) = e.means
    WHERE TRY_CAST(trim(d.taxonID) AS BIGINT) IS NOT NULL
)"""

# Una fila por (especie, país).
DISTRIBUTION = f"""(
    SELECT species_key, country AS country_key, establishment_means, occurrence_status,
           threat AS conservation_status, code AS conservation_code
    FROM {_DISTRIBUTION}
    WHERE country <> ''
    QUALIFY row_number() OVER (PARTITION BY species_key, country
                               ORDER BY priority DESC, severity DESC, establishment_means) = 1
)"""

# Evaluación global de IUCN (locality = 'Global'); si hay varias, la peor.
CONSERVATION = f"""(
    SELECT species_key, threat AS conservation_overall, code AS conservation_overall_code
    FROM {_DISTRIBUTION}
    WHERE locality = 'Global' AND threat <> ''
    QUALIFY row_number() OVER (PARTITION BY species_key ORDER BY severity DESC, threat) = 1
)"""
