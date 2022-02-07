FROM --platform=linux/amd64 python:3.8.6-buster as build
ENV PIP_INDEX_URL="https://mirrors.aliyun.com/pypi/simple/" PIP_DISABLE_PIP_VERSION_CHECK=1
COPY requirements.txt /tmp/requirements.txt
RUN mkdir /code && cd /code && python -m venv .venv && \
    .venv/bin/pip install -r /tmp/requirements.txt
WORKDIR /code
ENV PATH=/code/.venv/bin:$PATH
COPY . /code
RUN bash rssant_async_build.sh && \
    /code/dist/rssant_async_main/rssant_async_main --help && \
    du -sh /code/dist/rssant_async_main

FROM --platform=linux/amd64 debian:buster-slim as runtime
ENV LC_ALL=C.UTF-8 LANG=C.UTF-8
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    update-ca-certificates

FROM --platform=linux/amd64 runtime as check
COPY --from=build /code/dist/rssant_async_main /usr/app
RUN /usr/app/rssant_async_main --help

FROM --platform=linux/amd64 runtime
COPY --from=check /usr/app /usr/app
ARG EZFAAS_BUILD_ID=''
ARG EZFAAS_COMMIT_ID=''
ENV EZFAAS_BUILD_ID=${EZFAAS_BUILD_ID} EZFAAS_COMMIT_ID=${EZFAAS_COMMIT_ID}
CMD [ "/usr/app/rssant_async_main", "--bind=0.0.0.0:9000", "--keep-alive=7200" ]
