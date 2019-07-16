import logging

from aiohttp.web import Application, run_app, Response

from .message import ActorMessage, ActorMessageDecodeError, UnsupportContentEncodingError


LOG = logging.getLogger(__name__)


class MessageReceiver:
    def __init__(self, host, port, executor, registery, subpath=''):
        self.host = host
        self.port = port
        self.subpath = subpath
        self.executor = executor
        self.registery = registery

    async def request_handler(self, request):
        content_encoding = request.headers.get('Actor-Content-Encoding')
        data = await request.read()
        try:
            messages = ActorMessage.batch_decode(data, content_encoding)
        except UnsupportContentEncodingError as ex:
            LOG.exception(ex)
            return Response(str(ex), status=400)
        except ActorMessageDecodeError as ex:
            LOG.exception(ex)
            return Response(str(ex), status=400)
        for msg in messages:
            await self.handle_message(msg)
        return Response(status=204)

    async def handle_message(self, message):
        await self.executor.async_on_message(message)

    def create_app(self):
        app = Application(client_max_size=100 * 1024 * 1024)
        app.router.add_post(self.subpath, self.request_handler)
        return app

    def run(self):
        app = self.create_app()
        run_app(app, host=self.host, port=self.port)
