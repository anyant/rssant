import logging
from validr import T

from actorlib.actor import actor
from actorlib.context import ActorContext

from .name import ACTOR_MESSAGE_EXPORTER
from .base import BuiltinActorBase


LOG = logging.getLogger(__name__)


@actor(ACTOR_MESSAGE_EXPORTER)
class MessageExporter(BuiltinActorBase):

    async def __call__(
        self,
        ctx: ActorContext,
        dst: T.str,
        maxsize: T.int.min(1),
    ):
        messages = self.app.queue.op_export(
            dst=dst,
            dst_node=ctx.message.src_node,
            maxsize=maxsize,
        )
        messages = [x.to_dict() for x in messages]
        return dict(messages=messages)
