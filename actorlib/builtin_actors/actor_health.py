from rssant_common.helper import pretty_format_json

from actorlib.actor import actor
from actorlib.context import ActorContext

from .name import ACTOR_HEALTH
from .base import BuiltinActorBase


@actor(ACTOR_HEALTH)
class ActorHealth(BuiltinActorBase):

    async def __call__(self, ctx: ActorContext):
        """Report health metrics"""
        return self.health()

    def health(self):
        app = self.app
        # registery
        registery_info = {}
        registery_info['current_node'] = app.registery.current_node.to_spec()
        if app.registery.registery_node:
            registery_info['registery_node'] = app.registery.registery_node.to_spec()
        else:
            registery_info['registery_node'] = None
        registery_info['nodes'] = app.registery.to_spec()
        # storage
        storage_info = dict()
        if app.storage:
            storage_info.update(
                dirpath=app.storage.dirpath,
                current_filepath=app.storage.current_filepath,
                filepaths=app.storage.filepaths,
                wal_size=app.storage.wal_size,
                non_current_wal_size=app.storage.non_current_wal_size,
                compact_wal_delta=app.storage.compact_wal_delta,
                is_compacting=app.storage.is_compacting,
            )
        return dict(
            name=app.name,
            host=app.host,
            port=app.port,
            subpath=app.subpath,
            concurrency=app.concurrency,
            registery=registery_info,
            storage=storage_info,
            receiver=dict(),  # TODO: receiver/aiohttp metrics
            queue=app.queue.stats(),
            executor=dict(
                concurrency=app.executor.concurrency,
                num_async_workers=app.executor.num_async_workers,
                num_thread_workers=app.executor.num_thread_workers,
                async_concurrency=app.executor.async_concurrency,
                async_pending_limit=app.executor.async_pending_limit,
            ),
        )

    def print_health(self):
        print(pretty_format_json(self.health()))
