FROM node:lts-alpine as build-web
ARG NPM_REGISTERY="--registry=https://registry.npmmirror.com"
WORKDIR /app
COPY box/web/package*.json ./
RUN npm clean-install --no-audit --verbose ${NPM_REGISTERY}
COPY box/web .
RUN npm run build


FROM python:3.8.6-buster as build-api
RUN mkdir -p /app
WORKDIR /app
# install rssant
ARG PYPI_MIRROR="https://pypi.doubanio.com/simple/"
ENV PIP_INDEX_URL=$PYPI_MIRROR PIP_DISABLE_PIP_VERSION_CHECK=1
COPY requirements.txt .
RUN python -m venv .venv && \
    .venv/bin/pip install -r requirements.txt


FROM python:3.8.6-slim-buster
RUN mkdir -p /app
WORKDIR /app
# install ca-certificates, nginx and postgresql
COPY etc/apt-sources.list /etc/apt/sources.list
RUN apt-get update && \
    apt-get install -y ca-certificates && \
    update-ca-certificates && \
    apt-get install -y nginx postgresql-11
# fix DNS pollution of local network
COPY etc/resolv.conf /etc/resolv.conf
# install api files
COPY --from=build-api /app/.venv /app/.venv
ENV PATH=/app/.venv/bin:$PATH
COPY . .
# build django static files
RUN python manage.py collectstatic
# install web files
COPY --from=build-web /app/dist /var/www/rssant-html
# setup container config
RUN bash box/setup-container.sh
VOLUME /var/lib/postgresql/11/main
VOLUME /var/log/postgresql
VOLUME /app/data
EXPOSE 80 5432 6786 6788 6790 6791 6792 9001
CMD ["/app/.venv/bin/supervisord", "-c", "/etc/supervisord.conf"]
