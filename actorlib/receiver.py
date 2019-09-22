import logging
import secrets
import asyncio
from concurrent.futures import Future as ThreadFuture

from aiohttp.web import Application, run_app, Response

from .message import ActorMessage, ContentEncoding, ActorMessageDecodeError, UnsupportContentEncodingError
from .queue import ActorMessageQueue
from .registery import ActorRegistery
from .state import ActorStateError


LOG = logging.getLogger(__name__)


class MessageReceiver:
    def __init__(self, host, port, queue: ActorMessageQueue, registery: ActorRegistery, subpath='', token=None):
        self.host = host
        self.port = port
        self.subpath = subpath
        self.queue = queue
        self.registery = registery
        self.token = token

    def _verify_token(self, request):
        if not self.token:
            return True
        actor_token = request.headers.get('actor-token')
        if not actor_token:
            return False
        return secrets.compare_digest(actor_token, self.token)

    async def request_handler(self, request):
        if not self._verify_token(request):
            return Response(status=401)
        actor_ask_dst = request.headers.get('actor-ask-dst')
        content_encoding = request.headers.get('actor-content-encoding')
        data = await request.read()
        try:
            content_encoding = ContentEncoding.of(content_encoding)
            if actor_ask_dst:
                if data:
                    data = ActorMessage.raw_decode(data, content_encoding=content_encoding)
                else:
                    data = None
            else:
                data = ActorMessage.batch_decode(data, content_encoding=content_encoding)
        except UnsupportContentEncodingError as ex:
            LOG.exception(ex)
            return Response(body=str(ex), status=400)
        except ActorMessageDecodeError as ex:
            LOG.exception(ex)
            return Response(body=str(ex), status=400)
        if actor_ask_dst:
            return await self.handle_ask(request, data, actor_ask_dst, content_encoding)
        else:
            for msg in data:
                msg = self.registery.complete_message(msg)
                try:
                    self.queue.op_inbox(msg)
                except ActorStateError as ex:
                    LOG.warning(ex)
            return Response(status=204)

    async def handle_ask(self, request, data, dst, content_encoding):
        message_id = request.headers.get('actor-ask-id')
        src = request.headers.get('actor-ask-src')
        src_node = request.headers.get('actor-ask-src-node')
        dst_node = request.headers.get('actor-ask-dst-node')
        if not dst_node:
            dst_node = self.registery.current_node_name
        if not message_id:
            message_id = self.registery.generate_message_id()
        thread_future = ThreadFuture()
        msg = self.registery.create_message(
            id=message_id, content=data, is_ask=True,
            src=src, src_node=src_node, require_ack=False,
            dst=dst, dst_node=dst_node, future=thread_future,
        )
        try:
            self.queue.op_inbox(msg)
        except ActorStateError as ex:
            LOG.warning(ex)
            return Response(body=str(ex), status=400)
        result = await asyncio.wrap_future(thread_future)
        if result is None:
            return Response(status=204)
        result = ActorMessage.raw_encode(result, content_encoding=content_encoding)
        headers = {}
        if content_encoding == ContentEncoding.JSON:
            headers['content-type'] = 'application/json; charset=utf-8'
        else:
            headers['actor-content-encoding'] = content_encoding.value
        return Response(body=result, headers=headers)

    def create_app(self):
        app = Application(client_max_size=100 * 1024 * 1024)
        app.router.add_post(self.subpath, self.request_handler)
        return app

    def run(self):
        self.app = self.create_app()
        run_app(self.app, host=self.host, port=self.port)
