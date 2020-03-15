

class LXMLError(Exception):
    """Wrapper for lxml error"""


def lxml_call(f, text: str, *args, **kwargs):
    '''
    Fix ValueError: Unicode strings with encoding declaration are not supported.
        Please use bytes input or XML fragments without declaration.
    See also: https://stackoverflow.com/questions/15830421/xml-unicode-strings-with-encoding-declaration-are-not-supported
              https://lxml.de/parsing.html
    '''  # noqa: E501
    try:
        text = text.strip()
        try:
            r = f(text, *args, **kwargs)
        except ValueError as ex:
            is_unicode_error = ex.args and 'encoding declaration' in ex.args[0]
            if not is_unicode_error:
                raise
            r = f(text.encode('utf-8'), *args, **kwargs)
            if isinstance(r, bytes):
                r = r.decode('utf-8')
    except Exception as ex:  # lxml will raise too many errors
        raise LXMLError(ex) from ex
    return r
