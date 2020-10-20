import typing
import logging
import os
import stat
import socket
import ssl
from pathlib import Path

import certifi

from .resolver import ChainResolver, CertContainer


LOG = logging.getLogger(__name__)

_here = Path(__file__).parent
_cacert_filepath = str(_here / 'cacert.pem')


HOSTS = """
solidot.org
"""


class CacertHelper:

    @classmethod
    def update(cls):
        import logging
        logging.basicConfig(level='DEBUG')
        resolver = ChainResolver()
        ssl_context = cls._create_ssl_context()
        for host in cls._get_hosts():
            LOG.info('Resolving {}'.format(host))
            cert = cls._get_host_cert(host, ssl_context)
            resolver.resolve(cert)
        cls._save_cacert(resolver.list())

    @classmethod
    def _get_hosts(cls) -> typing.List[str]:
        return [line.strip() for line in HOSTS.strip().splitlines()]

    @classmethod
    def _create_ssl_context(cls) -> ssl.SSLContext:
        context = ssl.SSLContext()
        context.verify_mode = ssl.CERT_REQUIRED
        context.check_hostname = True
        context.load_default_certs()
        context.load_verify_locations(
            cafile=os.path.relpath(certifi.where()),
            capath=None,
            cadata=None,
        )
        return context

    @classmethod
    def _get_host_cert(cls, hostname: str, ssl_context: ssl.SSLContext) -> bytes:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            with ssl_context.wrap_socket(sock, server_hostname=hostname) as sock:
                sock.connect((hostname, 443))
                cert = sock.getpeercert(binary_form=True)
        return cert

    @classmethod
    def _save_cacert(cls, certs: typing.List[CertContainer]):
        LOG.info('Save {} certificates to {}'.format(len(certs), _cacert_filepath))
        cert_texts = []
        with open(certifi.where()) as f:
            cert_texts.append(f.read())
        for cert in certs:
            cert_texts.append(cert.export().decode('ascii'))
        cacert = '\n'.join(cert_texts)
        with open(_cacert_filepath, 'w') as f:
            f.write(cacert)
        os.chmod(_cacert_filepath, stat.S_IREAD | stat.S_IWRITE)

    def where(self) -> str:
        return _cacert_filepath
