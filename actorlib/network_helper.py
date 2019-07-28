import socket
import platform
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
    return '{}-{}'.format(platform.platform(), socket.getfqdn())


LOCAL_NODE_NAME = get_local_node_name()


def get_local_network_ip_list(ip_list=None, prefix=None):
    if ip_list is None:
        ip_list = LOCAL_IP_LIST
    if prefix is None:
        prefix = LOCAL_NODE_NAME
    names = []
    for interface_name, ip in ip_list:
        name = slugify(f'{prefix}-{interface_name}--{ip}')
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


if __name__ == "__main__":
    for x in get_local_networks():
        print(x)
