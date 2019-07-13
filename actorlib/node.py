import socket

from .actor import Actor
from .executor import ActorExecutor
from .registery import ActorRegistery
from .receiver import MessageReceiver
from .sender import MessageSender


class ActorNode:
    def __init__(
        self,
        actors,
        host='0.0.0.0',
        port=8000,
        concurrency=100,
        name=None,
        subpath=None,
        networks=None,
        registery_node_spec=None,
    ):
        actors = [Actor(x) for x in actors]
        self.actors = {x.name: x for x in actors}
        actor_modules = {x.module for x in actors}
        if not name:
            name = socket.getfqdn()
        self.name = name
        if not networks:
            networks = []
        self.registery = ActorRegistery(dict(
            name=self.name,
            modules=actor_modules,
            networks=networks,
        ), registery_node_spec=registery_node_spec)
        self.sender = MessageSender(
            concurrency=concurrency, registery=self.registery)
        self.executor = ActorExecutor(
            self.actors, sender=self.sender,
            registery=self.registery, concurrency=concurrency)
        self.receiver = MessageReceiver(
            host=host, port=port, subpath=subpath,
            executor=self.executor, registery=self.registery)

    def run(self):
        self.sender.start()
        self.executor.start()
        register_message = self.registery.get_register_message()
        if register_message:
            self.sender.submit(register_message)
        try:
            self.receiver.run()
        finally:
            self.sender.shutdown()
            self.executor.shutdown()
