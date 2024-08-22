FROM node:16-alpine as build-web
ARG NPM_REGISTERY="--registry=https://registry.npmmirror.com"
WORKDIR /app
COPY box/web/package*.json ./
RUN --mount=type=cache,target=/root/.npm \
    npm clean-install --no-audit --verbose ${NPM_REGISTERY}
COPY box/web .
RUN npm run build


FROM python:3.8.19-bullseye as build-api
WORKDIR /app
# install rssant
ARG PYPI_MIRROR="https://mirrors.aliyun.com/pypi/simple/"
ENV PIP_INDEX_URL=$PYPI_MIRROR PIP_DISABLE_PIP_VERSION_CHECK=1
RUN python -m venv .venv
ENV PATH=/app/.venv/bin:$PATH
COPY requirements-pip.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements-pip.txt
COPY constraint.txt .
ENV PIP_CONSTRAINT=/app/constraint.txt
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt


FROM python:3.8.19-slim-bullseye
WORKDIR /app
# install ca-certificates, nginx and postgresql
COPY etc/apt-sources.list /etc/apt/sources.list
RUN apt-get update && \
    apt-get install -y ca-certificates && update-ca-certificates && \
    apt-get install -y curl gnupg nginx && \
    apt-get install -y postgresql-common && \
    /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y && \
    apt-get install -y postgresql-11 && \
    rm -rf /var/lib/apt/lists/*
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
EXPOSE 80 5432 6786 6788 6793 9001
CMD ["/app/.venv/bin/supervisord", "-c", "/etc/supervisord.conf"]
