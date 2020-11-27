from typing import List, Dict, Any
import logging
import random
import ipaddress
import ssl
import socket
import asyncio
from urllib.parse import urlparse
from collections import defaultdict, OrderedDict

import yarl
import aiohttp
import requests.adapters

from rssant_config import CONFIG
from .rss_proxy import RSSProxyClient, ProxyStrategy
from .helper import get_or_create_event_loop

LOG = logging.getLogger(__name__)


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


class DNSError(Exception):
    """DNS Error"""


class PrivateAddressError(DNSError):
    """
    Private IP address Error.

    Prevent request private address, which will attack local network.
    """


class NameNotResolvedError(DNSError):
    """Name not resolved Error"""


def _is_public_ipv4(value):
    try:
        ip = ipaddress.ip_address(value)
    except ipaddress.AddressValueError:
        return False
    return ip.version == 4 and (not ip.is_private)


class DNSService:

    def __init__(self, client: RSSProxyClient, records: dict = None, allow_private_address: bool = False):
        self.hosts = list(records or {})
        self.update(records or {})
        self.client = client
        self.allow_private_address = allow_private_address

    @staticmethod
    def create(*, rss_proxy_url: str = None, rss_proxy_token: str = None, allow_private_address: bool = False):

        def proxy_strategy(url):
            if 'google.com' in url:
                return ProxyStrategy.PROXY_FIRST
            else:
                return ProxyStrategy.DIRECT_FIRST

        _rss_proxy_client = RSSProxyClient(
            rss_proxy_url=rss_proxy_url,
            rss_proxy_token=rss_proxy_token,
            proxy_strategy=proxy_strategy,
        )
        service = DNSService(
            client=_rss_proxy_client,
            records=_CACHE_RECORDS,
            allow_private_address=allow_private_address,
        )
        return service

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

    def _sync_resolve(self, host) -> list:
        addrinfo = socket.getaddrinfo(host, None)
        for family, __, __, __, sockaddr in addrinfo:
            if family == socket.AF_INET:
                ip, __ = sockaddr
                yield ip
            elif family == socket.AF_INET6:
                ip, __, __, __ = sockaddr
                yield ip

    def _local_resolve(self, host) -> list:
        ip_set = self.records.get(host)
        return list(ip_set) if ip_set else []

    def _select_ip(self, ip_set: list, *, host: str) -> list:
        # Discard private and prefer ipv4
        groups = OrderedDict([
            ((4, False), []),
            ((6, False), []),
            ((4, True), []),
            ((6, True), []),
        ])
        for ip in ip_set:
            ip = ipaddress.ip_address(ip)
            key = (ip.version, ip.is_private)
            if key not in groups:
                LOG.error(f'unknown version IP {ip}')
                continue
            groups[key].append(str(ip))
        public_s = groups[(4, False)] or groups[(6, False)]
        if public_s:
            return random.choice(public_s)
        private_s = groups[(4, True)] or groups[(4, True)]
        if self.allow_private_address:
            if private_s:
                return random.choice(private_s)
        else:
            if private_s:
                raise PrivateAddressError(private_s[0])
        raise NameNotResolvedError(host)

    def resolve_urllib3(self, host) -> str:
        ip_set = self._local_resolve(host)
        if not ip_set:
            ip_set = list(set(self._sync_resolve(host)))
        LOG.debug('resolve_urllib3 %s to %s', host, ip_set)
        ip = self._select_ip(ip_set, host=host)
        return ip

    def aiohttp_resolver(self, **kwargs):
        return RssantAsyncResolver(dns_service=self, **kwargs)

    def requests_http_adapter(self, **kwargs):
        return RssantHttpAdapter(dns_service=self, **kwargs)

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


class RssantAsyncResolver(aiohttp.AsyncResolver):

    def __init__(self, *args, dns_service: DNSService, **kwargs):
        self._dns_service = dns_service
        super().__init__(*args, **kwargs)

    async def _async_resolve(self, hostname) -> list:
        hosts = await super().resolve(hostname, family=socket.AF_INET)
        return list(set(item['host'] for item in hosts))

    async def resolve(
        self, host: str, port: int = 0,
        family: int = socket.AF_INET
    ) -> List[Dict[str, Any]]:
        ip_set = self._dns_service._local_resolve(host)
        if not ip_set:
            ip_set = await self._async_resolve(host)
        LOG.debug('resolve_aiohttp %s to %s', host, ip_set)
        ip = self._dns_service._select_ip(ip_set, host=host)
        return [{
            'hostname': host,
            'host': ip, 'port': port,
            'family': socket.AF_INET, 'proto': 0,
            'flags': socket.AI_NUMERICHOST,
        }]


class RssantHttpAdapter(requests.adapters.HTTPAdapter):
    """
    https://stackoverflow.com/questions/22609385/python-requests-library-define-specific-dns
    """

    def __init__(self, dns_service: DNSService, **kwargs):
        self.dns_service = dns_service
        super().__init__(**kwargs)

    def send(self, request, **kwargs):
        origin_request_url = request.url
        parsed_url = yarl.URL(request.url)
        hostname = parsed_url.raw_host
        ip = self.dns_service.resolve_urllib3(hostname)
        request.url = str(parsed_url.with_host(ip))
        request.headers['Host'] = str(parsed_url.origin()).split('://', 1)[1]
        connection_pool_kwargs = self.poolmanager.connection_pool_kw
        if parsed_url.scheme == 'https':
            connection_pool_kwargs['server_hostname'] = hostname
            connection_pool_kwargs['assert_hostname'] = hostname
        else:
            connection_pool_kwargs.pop('server_hostname', None)
            connection_pool_kwargs.pop('assert_hostname', None)
        response: requests.Response = super().send(request, **kwargs)
        response.url = request.url = origin_request_url
        return response


def _setup():
    _rss_proxy_options = {}
    if CONFIG.rss_proxy_enable:
        _rss_proxy_options.update(
            rss_proxy_url=CONFIG.rss_proxy_url,
            rss_proxy_token=CONFIG.rss_proxy_token,
        )
    service = DNSService.create(
        **_rss_proxy_options,
        allow_private_address=CONFIG.allow_private_address,
    )
    return service


DNS_SERVICE = _setup()
