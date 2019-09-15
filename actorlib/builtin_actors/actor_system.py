from actorlib.actor import actor
from actorlib.context import ActorContext

from .name import ACTOR_SYSTEM, ACTOR_INIT, ACTOR_TIMER
from .base import BuiltinActorBase


@actor(ACTOR_SYSTEM)
class ActorHealth(BuiltinActorBase):
    def __call__(self, ctx: ActorContext):
        if ACTOR_INIT in self.app.actors:
            self.op_inbox(ACTOR_INIT, src=ACTOR_SYSTEM)
        self.op_inbox(ACTOR_TIMER, src=ACTOR_SYSTEM)
