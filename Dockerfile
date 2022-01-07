FROM python:3.8.6-buster

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY etc/apt-sources.list /etc/apt/sources.list

# Fix DNS pollution of local network
COPY etc/resolv.conf /etc/resolv.conf

RUN apt-get update && \
    apt-get install -y \
    git neovim tree xz-utils lsof strace htop tcpdump dstat gdb \
    dnsutils iputils-ping iproute2 && \
    ln -s -f /usr/bin/nvim /usr/bin/vim && ln -s -f /usr/bin/nvim /usr/bin/vi

ARG PYPI_MIRROR="https://pypi.doubanio.com/simple/"
ENV PIP_INDEX_URL=$PYPI_MIRROR PIP_DISABLE_PIP_VERSION_CHECK=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN python manage.py collectstatic

EXPOSE 6788 6786

CMD ["/bin/bash"]
