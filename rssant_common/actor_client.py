from actorlib import ActorClient, ActorMessage, ActorRegistery
from rssant.settings import ENV_CONFIG


class SchedulerActorClient:
    def __init__(self, registery_node_spec):
        self.registery = ActorRegistery(
            registery_node_spec=registery_node_spec)
        self.client = ActorClient(registery=self.registery)

    def tell(self, dst, content):
        self.client.send(ActorMessage(
            dst='scheduler.proxy_tell',
            content=dict(
                dst=dst,
                content=content,
            )
        ))

    def batch_tell(self, tasks):
        messages = []
        for t in tasks:
            messages.append(ActorMessage(
                dst='scheduler.proxy_tell',
                content=dict(
                    dst=t['dst'],
                    content=t['content'],
                )
            ))
        self.client.send(*messages)

    def ask(self, dst, content):
        return self.client.ask('scheduler.proxy_ask', dict(dst=dst, content=content))


scheduler = SchedulerActorClient(ENV_CONFIG.registery_node_spec)
