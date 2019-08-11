import os.path
import tempfile


def shorten(text, width, placeholder='...'):
    """
    >>> shorten('123456789', width=8)
    '12345...'
    >>> shorten('123456789', width=9)
    '123456789'
    """
    if not text:
        return text
    if len(text) <= width:
        return text
    return text[: max(0, width - len(placeholder))] + placeholder


class BackdoorRequest:
    def __init__(self, command, params=None):
        self.command = command
        if params is None:
            params = {}
        self.params = params

    def __repr__(self):
        return '<{} {} {}>'.format(
            type(self).__name__, self.command, shorten(repr(self.params), 60))

    def to_dict(self):
        return dict(
            command=self.command,
            params=self.params,
        )


class BackdoorResponse:
    def __init__(self, ok, content):
        self.ok = ok
        self.content = content

    def __repr__(self):
        ok = 'OK' if self.ok else 'ERROR'
        return '<{} {} {}>'.format(
            type(self).__name__, ok, shorten(repr(self.content), 60))

    def to_dict(self):
        return dict(
            ok=self.ok,
            content=self.content,
        )


def get_socket_dir():
    socket_dir = os.path.join(tempfile.gettempdir(), 'backdoor')
    os.makedirs(socket_dir, exist_ok=True)
    return socket_dir


def get_socket_path(pid):
    socket_path = os.path.join(get_socket_dir(), '{}.sock'.format(pid))
    return socket_path


def detect_server_pid():
    files = list(sorted(os.listdir(get_socket_dir())))
    for filename in files:
        name, ext = os.path.splitext(os.path.basename(filename))
        if ext == '.sock':
            return int(name)
    return None
