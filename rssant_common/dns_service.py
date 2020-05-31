from typing import List, Dict, Any
import logging
import random
import ipaddress
import ssl
import socket
import asyncio
from urllib.parse import urlparse
from collections import defaultdict
from urllib3.util import connection

import aiohttp

from rssant_config import CONFIG
from .rss_proxy import RSSProxyClient, ProxyStrategy
from .helper import get_or_create_event_loop

LOG = logging.getLogger(__name__)


_orig_create_connection = connection.create_connection


_cache_records_text = """
104.26.12.87 rsshub.app
104.26.13.87 rsshub.app
168.235.96.195 kindle4rss.com
168.235.96.195 feedmaker.kindle4rss.com
192.30.255.112 github.com
192.30.255.116 api.github.com
"""


def _read_records(text) -> dict:
    records = defaultdict(set)
    for line in text.strip().splitlines():
        ip, host = line.split()
        records[host].add(ip)
    return records


_CACHE_RECORDS = _read_records(_cache_records_text)


def _is_public_ipv4(value):
    try:
        ip = ipaddress.ip_address(value)
    except ipaddress.AddressValueError:
        return False
    return not ip.is_private


class RssantAsyncResolver(aiohttp.AsyncResolver):

    def __init__(self, *args, dns_service, **kwargs):
        self._dns_service = dns_service
        super().__init__(*args, **kwargs)

    async def resolve(
        self, host: str, port: int = 0,
        family: int = socket.AF_INET
    ) -> List[Dict[str, Any]]:
        hosts = self._dns_service.resolve_aiohttp(host, port)
        if hosts:
            return hosts
        return await super().resolve(host, port, family=family)


class DNSService:

    def __init__(self, client: RSSProxyClient, records: dict = None):
        self.hosts = list(records or {})
        self.update(records or {})
        self.client = client

    def update(self, records: dict):
        new_records = defaultdict(set)
        for host, ip_set in records.items():
            new_records[host].update(ip_set)
        self.records = new_records

    def is_resolved_host(self, host) -> bool:
        return bool(self.records.get(host))

    def is_resolved_url(self, url) -> bool:
        host = urlparse(url).hostname
        return self.is_resolved_host(host)

    def resolve(self, host) -> list:
        ip_set = self.records.get(host)
        return list(ip_set) if ip_set else []

    def resolve_urllib3(self, host):
        ip_set = self.resolve(host)
        if ip_set:
            ip = random.choice(list(ip_set))
            LOG.info('resolve_urllib3 %s to %s', host, ip)
            return ip
        return host

    def aiohttp_resolver(self, **kwargs):
        return RssantAsyncResolver(dns_service=self, **kwargs)

    def resolve_aiohttp(self, host, port):
        hosts = []
        ip_set = self.resolve(host)
        if not ip_set:
            return hosts
        LOG.info('resolve_aiohttp %s to %s', host, ip_set)
        for ip in ip_set:
            hosts.append({
                'hostname': host,
                'host': ip, 'port': port,
                'family': socket.AF_INET, 'proto': 0,
                'flags': socket.AI_NUMERICHOST
            })
        return hosts

    def refresh(self):
        records = defaultdict(set)
        for host, ip_set in self.query_from_cloudflare().items():
            records[host].update(ip_set)
        LOG.info('resolved from cloudflare: %r', dict(records))
        if self.client.has_rss_proxy:
            for host, ip_set in self.query_from_google().items():
                records[host].update(ip_set)
            LOG.info('resolved from google: %r', dict(records))
        records = self.validate_records(records)
        LOG.info('refresh records: %r', dict(records))
        self.update(records)

    async def _verify_record_task(self, host, ip):
        _NetworkErrors = (
            socket.timeout, TimeoutError, asyncio.TimeoutError,
            ssl.SSLError, ssl.CertificateError, ConnectionError,
        )
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(
                host=ip, port=443,
                family=socket.AF_INET,
                ssl=True,
                server_hostname=host,
                ssl_handshake_timeout=10,
            ), timeout=15)
        except _NetworkErrors as ex:
            LOG.info(f'verify_record host={host} ip={ip} {ex!r}')
            return (host, ip, False)
        try:
            writer.close()
            await writer.wait_closed()
        except _NetworkErrors:
            pass  # ignore
        return (host, ip, True)

    async def _validate_records(self, records: dict):
        valid_records = defaultdict(set)
        tasks = []
        for host, ip_set in records.items():
            for ip in ip_set:
                tasks.append(self._verify_record_task(host, ip))
        for item in await asyncio.gather(*tasks):
            host, ip, ok = item
            if ok:
                valid_records[host].add(ip)
        return valid_records

    def validate_records(self, records: dict) -> dict:
        loop = get_or_create_event_loop()
        valid_records = loop.run_until_complete(self._validate_records(records))
        return valid_records

    def query_from_dns_over_tls(self, url_template: str) -> dict:
        headers = {'accept': 'application/dns-json'}
        records = defaultdict(set)
        for host in self.hosts:
            url = url_template.format(name=host)
            LOG.info(f'query {url}')
            try:
                response = self.client.request('GET', url, headers=headers)
                response.raise_for_status()
            except Exception as ex:
                LOG.warning(f'{type(ex).__name__}: {ex}')
                continue
            for item in response.json()['Answer']:
                if item['type'] == 1:  # ipv4
                    ip = item['data']
                    if ip and _is_public_ipv4(ip):
                        records[host].add(ip)
        return records

    def query_from_cloudflare(self):
        url_template = 'https://cloudflare-dns.com/dns-query?name={name}&type=A'
        return self.query_from_dns_over_tls(url_template)

    def query_from_google(self):
        url_template = 'https://dns.google.com/resolve?name={name}&type=A'
        return self.query_from_dns_over_tls(url_template)

    def patch_urllib3(self):
        """
        https://stackoverflow.com/questions/22609385/python-requests-library-define-specific-dns
        """
        connection.create_connection = self._patched_create_connection

    def _patched_create_connection(self, address, *args, **kwargs):
        """Wrap urllib3's create_connection to resolve the name elsewhere"""
        # resolve hostname to an ip address; use your own
        # resolver here, as otherwise the system resolver will be used.
        host, port = address
        hostname = self.resolve_urllib3(host)
        return _orig_create_connection((hostname, port), *args, **kwargs)


def _setup():
    _rss_proxy_options = {}
    if CONFIG.rss_proxy_enable:
        _rss_proxy_options.update(
            rss_proxy_url=CONFIG.rss_proxy_url,
            rss_proxy_token=CONFIG.rss_proxy_token,
        )

    def proxy_strategy(url):
        if 'google.com' in url:
            return ProxyStrategy.PROXY_FIRST
        else:
            return ProxyStrategy.DIRECT_FIRST

    _rss_proxy_client = RSSProxyClient(
        **_rss_proxy_options, proxy_strategy=proxy_strategy)
    service = DNSService(client=_rss_proxy_client, records=_CACHE_RECORDS)
    service.patch_urllib3()
    return service


DNS_SERVICE = _setup()
