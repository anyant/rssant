import logging
from aiohttp.web import Application, run_app, Response

from actorlib.message import ActorMessage

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
        messages = ActorMessage.batch_decode(data, content_encoding)
        for msg in messages:
            await self.handle_message(msg)
        return Response(status=204)

    async def handle_message(self, message):
        if message.dst == 'registery.update':
            self.registery.update(message.content)
        else:
            await self.executor.async_submit(message)

    def create_app(self):
        api = Application()
        api.router.add_post('/', self.request_handler)
        if self.subpath:
            app = Application()
            app.add_subapp(self.subpath, api)
        else:
            app = api
        return app

    def run(self):
        app = self.create_app()
        run_app(app, host=self.host, port=self.port)
