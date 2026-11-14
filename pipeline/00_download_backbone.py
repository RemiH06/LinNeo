"""Etapa 0: baja el GBIF Backbone fechado y extrae los TSV que usa el pipeline.

Se usa la versión fechada y no `current`: las llaves (taxonID) y el caso de
regresión de los invariantes dependen de ella. Al extraer bien se borra el zip.

    python pipeline/00_download_backbone.py
"""
import shutil
import sys
import urllib.request
import zipfile

from config import BACKBONE, BACKBONE_URL

TABLES = ["Taxon", "Description", "Distribution", "Multimedia", "Reference",
          "TypesAndSpecimen", "VernacularName"]


def main():
    BACKBONE.mkdir(parents=True, exist_ok=True)
    zip_path = BACKBONE / "backbone.zip"
    tmp = zip_path.with_suffix(".tmp")

    request = urllib.request.Request(BACKBONE_URL, headers={"User-Agent": "LinNeo-pipeline"})
    with urllib.request.urlopen(request, timeout=600) as response, open(tmp, "wb") as out:
        expected = int(response.headers["Content-Length"])
        shutil.copyfileobj(response, out, length=1 << 20)
    if tmp.stat().st_size != expected:
        sys.exit(f"Descarga incompleta: {tmp.stat().st_size:,} de {expected:,} bytes")
    tmp.replace(zip_path)

    with zipfile.ZipFile(zip_path) as archive:
        names = {name.rsplit("/", 1)[-1]: name for name in archive.namelist()}
        for table in TABLES:
            member = names.get(f"{table}.tsv")
            if member is None:
                sys.exit(f"El zip no trae {table}.tsv")
            target = BACKBONE / f"{table}.tsv"
            with archive.open(member) as src, open(target.with_suffix(".tmp"), "wb") as dst:
                shutil.copyfileobj(src, dst, length=1 << 20)
            target.with_suffix(".tmp").replace(target)
            print(f"{table}.tsv: {target.stat().st_size:,} bytes", flush=True)
    zip_path.unlink()
    print(f"Backbone listo en {BACKBONE}")


if __name__ == "__main__":
    main()
