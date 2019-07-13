import socket
import logging

from validr import Compiler

from .actor import Actor
from .executor import ActorExecutor
from .registery import ActorRegistery
from .receiver import MessageReceiver
from .sender import MessageSender


LOG_FORMAT = "%(levelname)1.1s %(asctime)s %(name)s:%(lineno)-4d %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

LOG = logging.getLogger(__name__)


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
        schema_compiler=None,
    ):
        if schema_compiler is None:
            schema_compiler = Compiler()
        self.schema_compiler = schema_compiler
        actors = [Actor(x, schema_compiler=schema_compiler) for x in actors]
        self.actors = {x.name: x for x in actors}
        actor_modules = {x.module for x in actors}
        if not name:
            name = '{}:{}'.format(socket.getfqdn(), port)
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
        LOG.info(f'Actor Node {self.name} started')
        try:
            self.receiver.run()
        finally:
            self.executor.shutdown()
            self.sender.shutdown()
