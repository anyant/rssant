import typing
import asyncio
import logging
from collections import defaultdict

from validr import T

from actorlib.actor import actor
from actorlib.context import ActorContext
from actorlib.message import ActorMessage

from .name import ACTOR_MESSAGE_NOTIFY_SENDER, ACTOR_MESSAGE_NOTIFY_RECEIVER
from .base import BuiltinActorBase


LOG = logging.getLogger(__name__)


@actor(ACTOR_MESSAGE_NOTIFY_SENDER)
class MessageNotifySender(BuiltinActorBase):

    async def safe_notify(self, ctx: ActorContext, content, dst_node) -> typing.List[ActorMessage]:
        try:
            await ctx.ask(ACTOR_MESSAGE_NOTIFY_RECEIVER, content, dst_node=dst_node)
        except Exception as ex:
            LOG.exception(ex)

    async def __call__(
        self,
        ctx: ActorContext,
        dst_info: T.list(T.dict(dst = T.str)),
        dst_node_info: T.list(T.dict(dst = T.str, dst_node = T.str)),
    ):
        dst_nodes = defaultdict(set)
        dst_info = [x['dst'] for x in dst_info]
        for dst in dst_info:
            for dst_node in ctx.registery.find_dst_nodes(dst):
                dst_nodes[dst_node].add(dst)
        dst_node_info = [(x['dst'], x['dst_node']) for x in dst_node_info]
        for dst, dst_node in dst_node_info:
            dst_nodes[dst_node].add(dst)
        local_dst_list = dst_nodes.pop(self.app.name, None)
        if local_dst_list:
            for dst in local_dst_list:
                self.app.queue.op_notify(src_node=self.app.name, dst=dst, available=True)
        tasks = []
        for dst_node, dst_list in dst_nodes.items():
            content = dict(dst_list=list(dst_list))
            tasks.append(self.safe_notify(ctx, content, dst_node=dst_node))
        await asyncio.gather(*tasks)
