FROM phusion/baseimage:0.11

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

# https://opsx.alibaba.com/mirror ubuntu 18.04
COPY etc/apt-sources.list /etc/apt/sources.list

# TODO: python-gdb in python 3.6 has issue: https://bugs.python.org/issue35132
#     python-gdb error: Python Exception Type does not have a target
# It's fixed in python 3.7: https://github.com/python/cpython/commit/047f8f25b93e2649d234fa565a59383fceb40e16
# To workaround in 3.6, manully edit /usr/share/gdb/auto-load/usr/bin/python3.6-gdb.py
#   - fields = gdb.lookup_type('PyUnicodeObject').target().fields()
#   + fields = gdb.lookup_type('PyUnicodeObject').fields()

RUN apt-get update && \
    apt-get install -y python3-venv python3-pip python3-dbg python3-dev \
        lsof strace htop tcpdump dstat gdb \
        dnsutils iputils-ping net-tools iproute2 && \
    rm -rf /var/lib/apt/lists/* && \
    ln -s --force /usr/bin/python3 /usr/bin/python && \
    ln -s --force /usr/bin/pip3 /usr/bin/pip

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/

COPY . .
RUN python3 manage.py collectstatic

EXPOSE 6788 6786

CMD ["/sbin/my_init", "--", "/bin/bash"]
