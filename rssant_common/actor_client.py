from actorlib import ActorClient, ActorRegistery

from rssant_config import CONFIG


class SchedulerActorClient:
    def __init__(self, registery=None, token=None):
        if registery is None:
            registery = self.get_registery()
        if not token:
            token = CONFIG.actor_token
        self.registery = registery
        self.client = ActorClient(registery=self.registery, token=token)

    @classmethod
    def get_registery(cls):
        registery = ActorRegistery(
            current_node_spec=CONFIG.current_node_spec,
            registery_node_spec=CONFIG.registery_node_spec,
        )
        return registery

    def tell(self, dst, content=None, expire_at=None):
        self.batch_tell([dict(dst=dst, content=content, expire_at=expire_at)])

    def batch_tell(self, tasks):
        tasks = [
            dict(
                dst=t['dst'],
                content=t.get('content'),
                expire_at=t.get('expire_at'),
            )
            for t in tasks
        ]
        self.client.send(self.registery.create_message(
            dst='scheduler.proxy_tell',
            content=dict(tasks=tasks),
        ))

    def ask(self, dst, content, expire_at=None):
        return self.client.ask(self.registery.create_message(
            dst='scheduler.proxy_ask',
            content=dict(dst=dst, content=content),
            expire_at=expire_at,
        ))


scheduler = SchedulerActorClient()
