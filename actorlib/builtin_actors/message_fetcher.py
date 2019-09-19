import typing
import asyncio
import logging
from validr import T

from actorlib.actor import actor
from actorlib.context import ActorContext
from actorlib.message import ActorMessage
from actorlib.state import ActorStateError

from .name import ACTOR_MESSAGE_FETCHER, ACTOR_MESSAGE_ACKER, ACTOR_MESSAGE_EXPORTER
from .base import BuiltinActorBase


LOG = logging.getLogger(__name__)


@actor(ACTOR_MESSAGE_FETCHER)
class MessageFetcher(BuiltinActorBase):

    async def safe_fetch(self, ctx: ActorContext, content, src_node) -> typing.List[ActorMessage]:
        try:
            r = await ctx.ask(ACTOR_MESSAGE_EXPORTER, content, dst_node=src_node)
        except Exception as ex:
            LOG.exception(ex)
            return []
        else:
            if not r:
                return []
            return [ActorMessage.from_dict(d) for d in r['messages']]

    async def local_fetch(self, dst, maxsize):
        messages = self.app.queue.op_export(dst=dst, dst_node=self.app.name, maxsize=maxsize)
        messages = [ActorMessage.from_dict(d.to_dict()) for d in messages]
        return messages

    async def __call__(
        self,
        ctx: ActorContext,
        actor_name: T.str,
        upstream_list: T.list(T.str).minlen(1),
        maxsize: T.int.min(1),
    ):
        LOG.info(f'fetch dst={actor_name} maxsize={maxsize} from {upstream_list}')
        tasks = []
        size = min(100, max(1, maxsize // len(upstream_list)))
        content = dict(dst=actor_name, maxsize=size)
        for src_node in upstream_list:
            if src_node == self.app.name:
                tasks.append(self.local_fetch(dst=actor_name, maxsize=size))
            else:
                tasks.append(self.safe_fetch(ctx, content, src_node))
        queue = self.app.queue
        messages_list = await asyncio.gather(*tasks)
        for src_node, messages in zip(upstream_list, messages_list):
            if len(messages) < size:
                queue.op_notify(src_node=src_node, dst=actor_name, available=False)
            if actor_name == ACTOR_MESSAGE_ACKER:
                for msg in messages:
                    status = msg.content['status']
                    try:
                        queue.op_acked(outbox_message_id=msg.id, status=status)
                    except ActorStateError as ex:
                        LOG.warning(ex)
            else:
                for msg in messages:
                    msg = self.app.registery.complete_message(msg)
                    try:
                        queue.op_inbox(msg)
                    except ActorStateError as ex:
                        LOG.warning(ex)
