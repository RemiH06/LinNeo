# Front de LinNeo para producción: build de Vite servido por Caddy, que también
# pasa /api a la API (ops/compose.prod.yml). Versiones fijas.
FROM node:24.22.1-alpine AS build
WORKDIR /src
COPY package.json package-lock.json ./
# Sin scripts de instalación: ninguna dependencia corre código al instalarse.
RUN npm ci --ignore-scripts
COPY . .
RUN npm run build

FROM caddy:2.11.9-alpine
# La imagen da al binario la capacidad de abrir puertos < 1024. Con 8080 no hace
# falta, y con cap_drop: ALL + no-new-privileges el kernel se niega a ejecutarlo.
RUN setcap -r /usr/bin/caddy
COPY Caddyfile /etc/caddy/Caddyfile
COPY --from=build /src/dist /srv
EXPOSE 8080
