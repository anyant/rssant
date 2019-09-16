from actorlib.actor import actor
from actorlib.context import ActorContext

from .name import ACTOR_STORAGE_COMPACTOR
from .base import BuiltinActorBase


@actor(ACTOR_STORAGE_COMPACTOR)
class ActorStorageCompactor(BuiltinActorBase):
    def __call__(self, ctx: ActorContext):
        prepare_info = self.app.queue.prepare_compact()
        self.app.storage.compact(prepare_info)
