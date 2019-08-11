import ast
import os
import sys
import socket
import logging
import traceback
import atexit
import resource
from io import StringIO
import threading
from threading import Thread

from msgpack import Packer, Unpacker

from .helper import BackdoorRequest, BackdoorResponse, get_socket_path


LOG = logging.getLogger(__name__)


_BACKDOOR_HANDLER = threading.local()


class BackdoorOutput:
    def __init__(self, fileobj):
        self.__dict__['_wrapped_fileobj'] = fileobj

    def write(self, s):
        fileobj = self.__dict__['_wrapped_fileobj']
        handler = getattr(_BACKDOOR_HANDLER, 'handler', None)
        if handler is not None:
            handler.print_buffer.write(s)
        return fileobj.write(s)

    def __getattr__(self, *args, **kwargs):
        fileobj = self.__dict__['_wrapped_fileobj']
        return getattr(fileobj, *args, **kwargs)

    def __setattr__(self, *args, **kwargs):
        fileobj = self.__dict__['_wrapped_fileobj']
        return setattr(fileobj, *args, **kwargs)

    def __delattr__(self, *args, **kwargs):
        fileobj = self.__dict__['_wrapped_fileobj']
        return delattr(fileobj, *args, **kwargs)


class BackdoorServer:
    def __init__(self):
        socket_path = get_socket_path(os.getpid())
        LOG.info(f'backdoor server listen at {socket_path}!')
        self.socket_path = socket_path
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.bind(socket_path)
        sock.listen(5)
        self.sock = sock
        atexit.register(self.close)
        thread = Thread(target=self.run)
        thread.daemon = True
        self.thread = thread
        self._sys_stdout = sys.stdout
        self._sys_stderr = sys.stderr

    def run(self):
        while True:
            cli_sock, cli_addr = self.sock.accept()
            t = Thread(target=self.handler, args=(cli_sock, cli_addr))
            t.start()

    def start(self):
        sys.stdout = BackdoorOutput(self._sys_stdout)
        sys.stderr = BackdoorOutput(self._sys_stderr)
        self.thread.start()

    def handler(self, cli_sock: socket.socket, cli_addr):
        client_name = 'fd={}'.format(cli_sock.fileno())
        LOG.info(f'client {client_name} enter backdoor!')
        handler = BackdoorHandler(self, cli_sock, cli_addr)
        _BACKDOOR_HANDLER.handler = handler
        try:
            handler.handle()
        finally:
            LOG.info(f'client {client_name} exit backdoor!')
            handler.close()
            _BACKDOOR_HANDLER.handler = None

    def close(self):
        sys.stdout = self._sys_stdout
        sys.stderr = self._sys_stderr
        self.sock.close()
        os.unlink(self.socket_path)


class BackdoorHandler:
    def __init__(self, server, cli_sock: socket.socket, cli_addr):
        self.server = server
        self.cli_sock = cli_sock
        self.cli_addr = cli_addr
        self.globals = {'print': self.backdoor_print}
        self.locals = {}
        self.print_buffer = StringIO()
        self.filename = '<backdoor>'

    def backdoor_print(self, *args, file=None, **kwargs):
        print(*args, **kwargs, file=self.print_buffer)

    def take_print_buffer(self):
        ret = self.print_buffer.getvalue()
        self.print_buffer = StringIO()
        return ret

    def close(self):
        self.cli_sock.close()
        self.locals = None
        self.globals = None

    def handle(self):
        packer = Packer(use_bin_type=True)
        unpacker = Unpacker(raw=False, max_buffer_size=10 * 1024 * 1024)
        while True:
            buf = self.cli_sock.recv(1024)
            if not buf:
                break
            unpacker.feed(buf)
            for request in unpacker:
                response = self.process(request)
                self.cli_sock.sendall(packer.pack(response))

    def _format_exception(self, ex):
        msg = traceback.format_exception(type(ex), ex, ex.__traceback__)
        return ''.join(msg).rstrip() + '\n'

    def command_info(self):
        total_memory_usage = (
            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            + resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss)
        num_active_threads = threading.active_count()
        r = dict(
            version=sys.version,
            platform=sys.platform,
            total_memory_usage=total_memory_usage,
            num_active_threads=num_active_threads,
        )
        return BackdoorResponse(True, r)

    def command_eval(self, source):
        try:
            source_ast = ast.parse(source, filename=self.filename)
        except (SyntaxError, ValueError, OverflowError) as ex:
            return BackdoorResponse(False, self._format_exception(ex))
        last_ast = None
        if source_ast.body and isinstance(source_ast.body[-1], ast.Expr):
            last_expr = source_ast.body.pop()
            last_ast = ast.Expression(body=last_expr.value)
        try:
            code = compile(source_ast, self.filename, 'exec')
            last_code = None
            if last_ast is not None:
                last_code = compile(last_ast, self.filename, 'eval')
        except (SyntaxError, ValueError, OverflowError) as ex:
            return BackdoorResponse(False, self._format_exception(ex))
        try:
            exec(code, self.globals, self.locals)
            msg = ''
            if last_code is not None:
                r = eval(last_code, self.globals, self.locals)
                if r is None:
                    msg = ''
                else:
                    msg = repr(r) + '\n'
        except Exception as ex:
            return BackdoorResponse(False, self._format_exception(ex))
        print_buffer = self.take_print_buffer()
        return BackdoorResponse(True, print_buffer + msg)

    def process(self, request: dict):
        request = BackdoorRequest(request['command'], request['params'])
        LOG.info(f'backdoor server process request {request}')
        command = 'command_{}'.format(request.command)
        if hasattr(self, command):
            response = getattr(self, command)(**request.params)
        else:
            response = BackdoorResponse(True, str(request))
        LOG.info(f'backdoor server sending response {response}')
        return response.to_dict()


def setup():
    server = BackdoorServer()
    server.start()
    return server


if __name__ == "__main__":
    from rssant_common.logger import configure_logging
    configure_logging()
    setup().thread.join()
