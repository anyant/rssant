import socket
import logging

from msgpack import Packer, Unpacker

from .helper import BackdoorRequest, BackdoorResponse, get_socket_path


LOG = logging.getLogger(__name__)


class BackdoorClient:
    def __init__(self, pid, sock=None):
        self.pid = pid
        socket_path = get_socket_path(pid)
        self.socket_path = socket_path
        LOG.info(f'backdoor client connect to {socket_path}')
        if sock is None:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect(socket_path)
        self.sock = sock

    def close(self):
        self.sock.close()

    def request(self, command, **params):
        packer = Packer(use_bin_type=True)
        unpacker = Unpacker(raw=False, max_buffer_size=10 * 1024 * 1024)
        request = BackdoorRequest(command, params)
        LOG.debug(f'backdoor client sending request {request}')
        self.sock.sendall(packer.pack(request.to_dict()))
        while True:
            buf = self.sock.recv(1024)
            if not buf:
                break
            unpacker.feed(buf)
            for response in unpacker:
                response = BackdoorResponse(response['ok'], response['content'])
                LOG.debug(f'backdoor client received response {response}')
                return response
