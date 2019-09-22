import time
import logging

from actorlib.actor import actor
from actorlib.context import ActorContext

from .base import BuiltinActorBase
from .name import ACTOR_MESSAGE_MONITOR


LOG = logging.getLogger(__name__)


@actor(ACTOR_MESSAGE_MONITOR, timer='1s')
class MessageMonitor(BuiltinActorBase):

    async def __call__(self, ctx: ActorContext):
        self.app.queue.op_tick(time.time())
