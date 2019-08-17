from actorlib import ActorClient, ActorMessage, ActorRegistery

from rssant.settings import ENV_CONFIG


class SchedulerActorClient:
    def __init__(self, registery=None):
        if registery is None:
            registery = self.get_registery()
        self.registery = registery
        self.client = ActorClient(registery=self.registery)

    @classmethod
    def get_registery(cls):
        registery = ActorRegistery(
            current_node_spec=ENV_CONFIG.current_node_spec,
            registery_node_spec=ENV_CONFIG.registery_node_spec,
        )
        return registery

    def tell(self, dst, content=None):
        self.batch_tell([dict(dst=dst, content=content)])

    def batch_tell(self, tasks):
        tasks = [dict(dst=t['dst'], content=t.get('content')) for t in tasks]
        self.client.send(ActorMessage(
            dst='scheduler.proxy_tell', content=dict(tasks=tasks),
        ))

    def ask(self, dst, content):
        return self.client.ask(ActorMessage(
            dst='scheduler.proxy_ask', content=dict(dst=dst, content=content),
        ))


scheduler = SchedulerActorClient()
