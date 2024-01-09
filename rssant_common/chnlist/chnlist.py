import gzip
from pathlib import Path

import tldextract
from marisa_trie import Trie

_CHNLIST_FILEPATH = Path(__file__).parent / 'chnlist.txt.gz'


def _get_main_domain(fqdn: str):
    extract_domain = tldextract.TLDExtract(suffix_list_urls=[])
    extracted = extract_domain(fqdn)
    main_domain = f"{extracted.domain}.{extracted.suffix}"
    return main_domain


class ChinaWebsiteList:
    def __init__(self) -> None:
        self._chnlist_trie: Trie = None

    def _read_chnlist_text(self):
        content = _CHNLIST_FILEPATH.read_bytes()
        content = gzip.decompress(content)
        text = content.decode('utf-8')
        return text

    def _build_chnlist_trie(self):
        text = self._read_chnlist_text()
        domain_s = []
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            domain_s.append(line)
        trie = Trie(domain_s)
        return trie

    def _get_trie(self):
        if self._chnlist_trie is None:
            self._chnlist_trie = self._build_chnlist_trie()
        return self._chnlist_trie

    def is_china_website(self, hostname: str) -> bool:
        trie = self._get_trie()
        domain = _get_main_domain(hostname)
        return domain in trie


CHINA_WEBSITE_LIST = ChinaWebsiteList()
