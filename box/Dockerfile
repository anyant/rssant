FROM node:lts-alpine as build

ARG NPM_REGISTERY="--registry=https://registry.npmmirror.com"

WORKDIR /app
COPY box/web/package*.json ./
RUN npm clean-install --no-audit --loglevel info ${NPM_REGISTERY}
COPY box/web .
RUN npm run build

FROM python:3.8.6-buster

RUN mkdir -p /app
WORKDIR /app

COPY etc/apt-sources.list /etc/apt/sources.list

# Fix DNS pollution of local network
COPY etc/resolv.conf /etc/resolv.conf

RUN apt-get update && \
    apt-get install -y \
    git neovim tree xz-utils lsof strace htop tcpdump dstat gdb \
    dnsutils iputils-ping iproute2 && \
    ln -s -f /usr/bin/nvim /usr/bin/vim && ln -s -f /usr/bin/nvim /usr/bin/vi

# install nginx
RUN apt-get update && apt-get install -y nginx

# install postgresql
RUN apt-get update && apt-get install -y postgresql-11

# install rssant
ARG PYPI_MIRROR="https://mirrors.aliyun.com/pypi/simple/"
ENV PIP_INDEX_URL=$PYPI_MIRROR PIP_DISABLE_PIP_VERSION_CHECK=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN python manage.py collectstatic

# install web files
COPY --from=build /app/dist /var/www/rssant-html

# setup container config
RUN bash box/setup-container.sh

VOLUME /var/lib/postgresql/11/main
VOLUME /var/log/postgresql
VOLUME /app/data
EXPOSE 80 5432 6786 6788 6790 6791 6792 9001

CMD ["/bin/bash", "-c", "/usr/local/bin/supervisord -c /etc/supervisord.conf"]
