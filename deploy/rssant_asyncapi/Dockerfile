FROM python:3.8.12-bullseye AS build
WORKDIR /app
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
COPY requirements-build.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt -r requirements-build.txt
COPY . /app
RUN bash deploy/rssant_asyncapi/pyinstaller_build.sh && \
    /app/dist/run-asyncapi/run-asyncapi --help && \
    du -sh /app/dist/run-asyncapi

FROM debian:bullseye-slim AS runtime
ENV LC_ALL=C.UTF-8 LANG=C.UTF-8
WORKDIR /app
ARG PYPI_MIRROR="https://mirrors.aliyun.com/pypi/simple/"
ENV PIP_INDEX_URL=$PYPI_MIRROR PIP_DISABLE_PIP_VERSION_CHECK=1
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

FROM runtime AS check
COPY --from=build /app/dist/run-asyncapi /app
RUN /app/run-asyncapi --help

FROM runtime
COPY --from=check /app /app
ARG EZFAAS_BUILD_ID=''
ARG EZFAAS_COMMIT_ID=''
ENV EZFAAS_BUILD_ID=${EZFAAS_BUILD_ID} EZFAAS_COMMIT_ID=${EZFAAS_COMMIT_ID}
CMD [ "/app/run-asyncapi", "--bind=0.0.0.0:9000", "--keep-alive=7200" ]
