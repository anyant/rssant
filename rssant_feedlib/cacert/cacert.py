import typing
import logging
import os
import stat
import ssl
from pathlib import Path

import certifi

from .resolver import ChainResolver, CertContainer


LOG = logging.getLogger(__name__)

_here = Path(__file__).parent
_cacert_filepath = str(_here / 'cacert.pem')


HOSTS = """
solidot.org
hotrss.top
"""


class CacertHelper:

    @classmethod
    def update(cls):
        import logging
        logging.basicConfig(level='DEBUG')
        resolver = ChainResolver()
        for host in cls._get_hosts():
            LOG.info('Resolving {}'.format(host))
            cert = cls._get_host_cert(host)
            resolver.resolve(cert)
        cls._save_cacert(resolver.list())

    @classmethod
    def _get_hosts(cls) -> typing.List[str]:
        return [line.strip() for line in HOSTS.strip().splitlines()]

    @classmethod
    def _get_host_cert(cls, hostname: str) -> bytes:
        cert = ssl.get_server_certificate((hostname, 443))
        return cert.encode('ascii')

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
