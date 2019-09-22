import logging
from validr import T

from actorlib.actor import actor
from actorlib.context import ActorContext
from actorlib.state import ActorStateError

from .name import ACTOR_MESSAGE_NOTIFY_RECEIVER
from .base import BuiltinActorBase


LOG = logging.getLogger(__name__)


@actor(ACTOR_MESSAGE_NOTIFY_RECEIVER)
class MessageNotifyReceiver(BuiltinActorBase):

    async def __call__(
        self,
        ctx: ActorContext,
        dst_list: T.list(T.str),
    ):
        queue = self.app.queue
        src_node = ctx.message.src_node
        for dst in dst_list:
            try:
                queue.op_notify(src_node=src_node, dst=dst, available=True)
            except ActorStateError as ex:
                LOG.warning(ex)
        return dict(message='OK')
