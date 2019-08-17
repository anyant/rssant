import socket
import platform
import hashlib

import netifaces
from slugify import slugify


def get_local_ip_list():
    ip_list = []
    for interface_name in netifaces.interfaces():
        addr_list = netifaces.ifaddresses(interface_name).get(netifaces.AF_INET)
        if not addr_list:
            continue
        for item in addr_list:
            ip = item.get('addr')
            if ip:
                ip_list.append((interface_name, ip))
    return ip_list


LOCAL_IP_LIST = get_local_ip_list()


def get_local_node_name():
    """
    same server <-> same node name
    different server <-> different node name
    """
    name = '{}-{}'.format(platform.platform(), socket.getfqdn())
    h = hashlib.md5(name.encode('utf-8'))
    for interface_name, ip in LOCAL_IP_LIST:
        h.update(f'{interface_name}-{ip}'.encode('utf-8'))
    return h.hexdigest()[:6]


LOCAL_NODE_NAME = get_local_node_name()


def get_local_network_ip_list(ip_list=None, prefix=None):
    if ip_list is None:
        ip_list = LOCAL_IP_LIST
    if prefix is None:
        prefix = LOCAL_NODE_NAME
    names = []
    for interface_name, ip in ip_list:
        name = slugify(f'{prefix}-{interface_name}-{ip}')
        names.append((name, ip))
    return names


LOCAL_NETWORK_NAMES = list(set(name for name, __ in get_local_network_ip_list()))


def get_local_networks(ip_list=None, prefix=None, port=None, subpath=None):
    if port is None:
        port = 80
    if subpath is None:
        subpath = ''
    networks = []
    network_ip_list = get_local_network_ip_list(ip_list=ip_list, prefix=prefix)
    for name, ip in network_ip_list:
        url = f'http://{ip}:{port}{subpath}'
        networks.append(dict(name=name, url=url))
    return networks


def get_public_ip_list():
    """
    TODO: 公网服务，随机6个取最快的3个结果 -> network-name
    https://ip4.bramp.net/json?family=IPv4
    https://httpbin.org/ip
    https://www.whatismyip.com/
    https://www.whatsmyip.org/
    https://www.myip.com/
    https://checkmyip.com/
    https://www.iplocation.net/
    https://whoer.net/zh
    http://www.ip138.com/
    https://www.ip.cn/
    http://ip.tool.chinaz.com/
    https://www.ipip.net/ip.html
    https://en.ipip.net/ip.html
    http://ip111.cn/
    https://tool.lu/ip/
    """


if __name__ == "__main__":
    for x in get_local_networks():
        print(x)
