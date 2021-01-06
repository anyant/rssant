"""
https://github.com/s3rvac/blog/blob/master/en-2018-04-22-on-incomplete-http-reads-and-the-requests-library-in-python/servers/content-encoding-gzip.py
"""
import sys
import socketserver
from threading import Thread


INCOMPLETE_TEXT_RESPONSE = (
    b'HTTP/1.1 200 OK\r\n'
    b'Content-Length: 10\r\n'
    b'\r\n'
    b'123456'
)

INCOMPLETE_GZIP_RESPONSE = (
    b'HTTP/1.1 200 OK\r\n'
    b'Content-Encoding: gzip\r\n'
    b'Content-Length: 30\r\n'
    b'\r\n'
    # gzipped "hello" (25 bytes):
    b'\x1f\x8b\x08\x00\xc9)\xdcZ\x00\x03\xcbH\xcd\xc9\xc9\x07\x00\x86\xa6\x106\x05\x00\x00\x00'
)


def create_handler(response: bytes):
    class HttpTCPHandler(socketserver.BaseRequestHandler):
        def handle(self):
            self.request.recv(1024)
            self.request.sendall(response)
    return HttpTCPHandler


class HttpTCPSever(socketserver.TCPServer):
    allow_reuse_address = True


class SocketHttpServer:
    def __init__(self, response: bytes):
        self._response = response
        self._thread = None
        self._server = None
        self._port = 0

    @classmethod
    def incomplete_text(cls):
        return cls(response=INCOMPLETE_TEXT_RESPONSE)

    @classmethod
    def incomplete_gzip(cls):
        return cls(response=INCOMPLETE_GZIP_RESPONSE)

    def __enter__(self):
        return self._start()

    def __exit__(self, *exc_info):
        self._close()

    @property
    def url(self) -> str:
        return f'http://127.0.0.1:{self._port}'

    @property
    def join(self):
        self._thread.join()

    def _start(self):
        handler = create_handler(self._response)
        self._server = HttpTCPSever(('127.0.0.1', 0), handler)
        self._port = self._server.socket.getsockname()[1]
        self._thread = Thread(target=self._run)
        self._thread.start()
        return self

    def _close(self):
        self._server.shutdown()
        self._thread.join(timeout=3)

    def _run(self):
        with self._server:
            self._server.serve_forever()


if __name__ == "__main__":
    key = sys.argv[1]
    with getattr(SocketHttpServer, key)() as server:
        print(f'* Listen at {server.url}')
        server.join()
