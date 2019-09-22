from validr import T

from actorlib.actor import actor
from actorlib.context import ActorContext

from .name import ACTOR_MESSAGE_ACKER
from .base import BuiltinActorBase


@actor(ACTOR_MESSAGE_ACKER)
class MessageAcker(BuiltinActorBase):

    async def __call__(self, ctx: ActorContext, status: T.str):
        self.app.queue.op_acked(message_id=ctx.message.id, status=status)
