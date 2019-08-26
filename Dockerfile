FROM phusion/baseimage:0.11

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

# https://opsx.alibaba.com/mirror ubuntu 18.04
COPY etc/apt-sources.list /etc/apt/sources.list

RUN apt-get update && \
    apt-get install -y python3-venv python3-pip && \
        lsof strace htop tcpdump sysstat dstat && \
        dnsutils iputils-ping net-tools iproute2 && \
    rm -rf /var/lib/apt/lists/* && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    ln -s /usr/bin/pip3 /usr/bin/pip

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/

COPY . .
RUN python3 manage.py collectstatic

EXPOSE 6788 6786

CMD ["/bin/bash"]
