FROM python:3.7.7-stretch

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

# https://opsx.alibaba.com/mirror debian 9.x (stretch)
COPY etc/apt-sources.list /etc/apt/sources.list
RUN apt-get update && \
    apt-get install -y \
        git vim tree xz-utils lsof strace htop tcpdump dstat gdb \
        dnsutils iputils-ping iproute2

ARG PYPI_MIRROR="https://mirrors.aliyun.com/pypi/simple/"
ENV PIP_INDEX_URL=$PYPI_MIRROR PIP_DISABLE_PIP_VERSION_CHECK=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN python manage.py collectstatic

EXPOSE 6788 6786

CMD ["/bin/bash"]
