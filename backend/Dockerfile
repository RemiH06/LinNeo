# API de LinNeo para producción (ops/compose.prod.yml). Versiones fijas.
FROM python:3.13.17-slim-trixie
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && useradd --no-create-home --uid 10001 linneo
WORKDIR /srv
COPY app/ app/
USER linneo
EXPOSE 8035
# 0.0.0.0 dentro de su red interna; la API no publica puerto en el host (la alcanza Caddy).
# Sin log de accesos: Cloudflare ya ve cada petición y aquí solo juntaría IPs.
# --limit-concurrency: más de 64 peticiones a la vez responden 503 en vez de encolarse.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8035", \
     "--no-server-header", "--no-access-log", "--limit-concurrency", "64"]
