FROM --platform=linux/amd64 python:3.8.19-bullseye as build
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
RUN bash deploy/rssant_scheduler/pyinstaller_build.sh && \
    /app/dist/run-scheduler/run-scheduler --help && \
    du -sh /app/dist/run-scheduler

FROM --platform=linux/amd64 debian:bullseye-slim as runtime
ENV LC_ALL=C.UTF-8 LANG=C.UTF-8
WORKDIR /app
ARG PYPI_MIRROR="https://mirrors.aliyun.com/pypi/simple/"
ENV PIP_INDEX_URL=$PYPI_MIRROR PIP_DISABLE_PIP_VERSION_CHECK=1
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

FROM --platform=linux/amd64 runtime as check
COPY --from=build /app/dist/run-scheduler /app
RUN /app/run-scheduler --help

FROM --platform=linux/amd64 runtime
COPY --from=check /app /app
ARG EZFAAS_BUILD_ID=''
ARG EZFAAS_COMMIT_ID=''
ENV EZFAAS_BUILD_ID=${EZFAAS_BUILD_ID} EZFAAS_COMMIT_ID=${EZFAAS_COMMIT_ID}
CMD [ "/app/run-scheduler", "--port=9000" ]
