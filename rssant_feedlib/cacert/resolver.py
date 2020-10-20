"""
Certificate chain resolver:
    https://github.com/rkoopmans/python-certificate-chain-resolver

See also:
    https://github.com/zakjan/cert-chain-resolver
    https://github.com/muchlearning/cert-chain-resolver-py
"""
from cryptography import x509
from cryptography.hazmat.backends.openssl.backend import backend as OpenSSLBackend
from cryptography.x509.oid import ExtensionOID, AuthorityInformationAccessOID, NameOID
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.serialization import Encoding
from OpenSSL import crypto
from contextlib import closing
import logging
import binascii
import six
from urllib.request import urlopen, Request


log = logging.getLogger(__name__)


class UnsuportedCertificateType(Exception):
    pass


class CertContainer(object):
    x509 = None
    details = None

    def __init__(self, x509, details):
        self.x509 = x509
        self.details = details

    def export(self, encoding=Encoding.PEM):
        return self.x509.public_bytes(encoding)


def pkcs7_get_certs(self):
    """
    https://github.com/pyca/pyopenssl/pull/367/files#r67300900
    Returns all certificates for the PKCS7 structure, if present. Only
    objects of type ``signedData`` or ``signedAndEnvelopedData`` can embed
    certificates.
    :return: The certificates in the PKCS7, or :const:`None` if
        there are none.
    :rtype: :class:`tuple` of :class:`X509` or :const:`None`
    """
    from OpenSSL.crypto import _lib, _ffi, X509
    certs = _ffi.NULL
    if self.type_is_signed():
        certs = self._pkcs7.d.sign.cert
    elif self.type_is_signedAndEnveloped():
        certs = self._pkcs7.d.signed_and_enveloped.cert

    pycerts = []
    for i in range(_lib.sk_X509_num(certs)):
        x509 = _ffi.gc(_lib.X509_dup(_lib.sk_X509_value(certs, i)),
                       _lib.X509_free)
        pycert = X509._from_raw_x509_ptr(x509)
        pycerts.append(pycert)
    if pycerts:
        return [x.to_cryptography() for x in pycerts]


class Resolver:
    def __init__(self, cert, content_type=None):
        try:
            if cert.startswith(b"-----BEGIN CERTIFICATE-----"):
                log.debug("Loading file with content_type pem")
                self.cert = x509.load_pem_x509_certificate(cert, OpenSSLBackend)
            elif content_type == 'pkcs7-mime':
                pkcs7 = crypto.load_pkcs7_data(crypto.FILETYPE_ASN1, cert)
                certs = pkcs7_get_certs(pkcs7)
                if not len(certs):
                    raise ValueError('No certs in pkcs7')
                elif len(certs) > 1:
                    log.warning('Multiple certs found but only processing the first one')
                self.cert = certs[0]
            else:
                log.debug("Loading file with content_type {0}".format(content_type))
                self.cert = x509.load_der_x509_certificate(cert, OpenSSLBackend)
        except ValueError:
            raise UnsuportedCertificateType(
                "Failed to load cert with content_type={0}".format(content_type))

    def get_details(self):
        return {
            "issuer": self.cert.issuer.rfc4514_string(),
            "subject": self.cert.subject.rfc4514_string(),
            "fingerprint_sha256": self.fingerprint(),
            "signature_algorithm": self.cert.signature_hash_algorithm.name,
            "serial": self.cert.serial_number,
            "not_before": self.cert.not_valid_before,
            "not_after": self.cert.not_valid_after,
            "common_name": self._get_common_name(),
            "san": self._get_san(),
            "ca": self._is_ca(),
        }

    def get_parent_cert(self):
        try:
            aias = self.cert.extensions.get_extension_for_oid(
                ExtensionOID.AUTHORITY_INFORMATION_ACCESS)
            for aia in aias.value:
                if AuthorityInformationAccessOID.CA_ISSUERS == aia.access_method:
                    return self._download(aia.access_location.value)
        except x509.extensions.ExtensionNotFound:
            pass
        return (None, None)

    def fingerprint(self, _hash=hashes.SHA256):
        binary = self.cert.fingerprint(_hash())
        txt = binascii.hexlify(binary)
        if six.PY3:
            txt = txt.decode('ascii')
        return txt

    def _is_ca(self):
        try:
            ext = self.cert.extensions.get_extension_for_oid(ExtensionOID.BASIC_CONSTRAINTS)
        except x509.extensions.ExtensionNotFound:
            return False
        return ext.value.ca

    def _get_common_name(self):
        cn = [x.value for x in self.cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)]
        if cn:
            return cn[0]

    def _get_san(self):
        try:
            ext = self.cert.extensions.get_extension_for_oid(ExtensionOID.SUBJECT_ALTERNATIVE_NAME)
            return ext.value.get_values_for_type(x509.DNSName)
        except x509.extensions.ExtensionNotFound:
            return None

    def _download(self, url):
        req = Request(url, headers={"User-Agent": "Cert/fixer"})
        log.debug("Downloading: {0}".format(url))
        with closing(urlopen(req)) as resp:
            content_type = resp.headers.get_content_subtype()
            return content_type, resp.read()


class ChainResolver:

    _chain = None
    depth = None

    def __init__(self, depth=None):
        self._chain = []
        self.depth = depth

    def resolve(self, cert, content_type=None):
        r = Resolver(cert, content_type)
        self._chain.append(CertContainer(x509=r.cert, details=r.get_details()))

        if (self.depth is None or len(self._chain) <= self.depth):
            content_type, parent_cert = r.get_parent_cert()
            if parent_cert:
                return self.resolve(parent_cert, content_type=content_type)

    def list(self):
        return self._chain
