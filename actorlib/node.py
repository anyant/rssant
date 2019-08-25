import logging
from urllib.parse import urlparse

import click
from validr import Compiler

from rssant_common.helper import pretty_format_json

from .actor import Actor
from .executor import ActorExecutor
from .registery import ActorRegistery
from .receiver import MessageReceiver
from .sender import MessageSender
from .message import ActorMessage
from .network_helper import get_local_networks, LOCAL_NODE_NAME
from .storage import ActorLocalStorage, ActorMemoryStorage
from .storage_compactor import ActorStorageCompactor
from .message_monitor import ActorMessageMonitor


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
        storage_dir_path=None,
        storage_max_pending_size=10**2,
        storage_max_done_size=10**3,
        storage_compact_interval=60,
        ack_timeout=180,
        max_retry_count=3,
        schema_compiler=None,
        on_startup=None,
        on_shutdown=None,
    ):
        if schema_compiler is None:
            schema_compiler = Compiler()
        self.schema_compiler = schema_compiler
        actors = [Actor(x, schema_compiler=schema_compiler) for x in actors]
        self.actors = {x.name: x for x in actors}
        actor_modules = {x.module for x in actors}
        if not name:
            name = '{}-{}'.format(LOCAL_NODE_NAME, port)
        self.name = name
        if not networks:
            networks = []
        networks.extend(get_local_networks(port=port, subpath=subpath))
        current_node_spec = dict(
            name=self.name,
            modules=actor_modules,
            networks=networks,
        )
        self.registery = ActorRegistery(
            current_node_spec=current_node_spec,
            registery_node_spec=registery_node_spec)
        self.storage_dir_path = storage_dir_path
        if storage_dir_path:
            self.storage = ActorLocalStorage(
                dir_path=storage_dir_path,
                max_pending_size=storage_max_pending_size,
                max_done_size=storage_max_done_size,
            )
            self.storage_compactor = ActorStorageCompactor(
                self.storage, interval=storage_compact_interval)
        else:
            LOG.info('storage_dir_path not set, will use memory storage')
            self.storage = ActorMemoryStorage(
                max_pending_size=storage_max_pending_size,
                max_done_size=storage_max_done_size,
            )
            self.storage_compactor = None
        self.concurrency = concurrency
        self.sender = MessageSender(
            concurrency=concurrency, storage=self.storage, registery=self.registery)
        self.executor = ActorExecutor(
            self.actors, sender=self.sender, storage=self.storage,
            registery=self.registery, concurrency=concurrency)
        self.message_monitor = ActorMessageMonitor(
            self.storage, self.executor, self.sender,
            ack_timeout=ack_timeout, max_retry_count=max_retry_count,
        )
        self.host = host
        self.port = port
        self.subpath = subpath or ''
        self.receiver = MessageReceiver(
            host=self.host, port=self.port, subpath=self.subpath,
            executor=self.executor, registery=self.registery)
        self._on_startup_handlers = []
        self._on_shutdown_handlers = []
        if on_startup:
            self._on_startup_handlers.extend(on_startup)
        if on_shutdown:
            self._on_shutdown_handlers.extend(on_shutdown)

    def on_startup(self, handler):
        self._on_startup_handlers.append(handler)
        return handler

    def on_shutdown(self, handler):
        self._on_shutdown_handlers.append(handler)
        return handler

    def _send_init_message(self):
        if 'actor.init' not in self.actors:
            return
        msg = ActorMessage(
            src='actor.init', dst='actor.init', is_ask=False,
            dst_node=self.registery.current_node.name
        )
        self.executor.submit(msg)

    def ask(self, dst, content=None, dst_node=None):
        if content is None:
            content = {}
        msg = ActorMessage(
            src='actor.init', content=content, dst=dst, dst_node=dst_node)
        client = self.executor.main_thread_client
        return client.ask(msg)

    def tell(self, dst, content=None, dst_node=None):
        if content is None:
            content = {}
        msg = ActorMessage(
            src='actor.init', content=content, dst=dst, dst_node=dst_node)
        client = self.executor.main_thread_client
        client.send(msg)

    def run(self):
        self.sender.start()
        self.executor.start()
        if self.storage_compactor:
            self.storage_compactor.start()
        self.message_monitor.start()
        LOG.info(f'Actor Node {self.name} at http://{self.host}:{self.port}{self.subpath} started')
        LOG.info(f'current registery:\n{pretty_format_json(self.registery.to_spec())}')
        try:
            for handler in self._on_startup_handlers:
                handler(self)
            self._send_init_message()
            self.receiver.run()
        finally:
            try:
                for handler in self._on_shutdown_handlers:
                    handler(self)
            finally:
                self.message_monitor.shutdown()
                if self.storage_compactor:
                    self.storage_compactor.shutdown()
                self.executor.shutdown()
                self.sender.shutdown()

    @classmethod
    def cli(cls, *args, **kwargs):
        @click.command()
        @click.option('--name', help='actor node name')
        @click.option('--listen', help='http://host:port/subpath')
        @click.option('--network', multiple=True, help='network@http://host:port/subpath')
        @click.option('--concurrency', type=int, help='concurrency')
        def command(name, listen, network, concurrency):
            if name:
                kwargs['name'] = name
            listen = urlparse(listen)
            host = listen.hostname
            if host:
                kwargs['host'] = host
            port = listen.port
            if port:
                kwargs['port'] = int(port)
            subpath = listen.path
            if subpath:
                kwargs['subpath'] = subpath
            networks = []
            for network_spec in network:
                name, url = network_spec.split('@', maxsplit=1)
                networks.append(dict(name=name, url=url))
            if kwargs.get('networks'):
                networks.extend(kwargs.get('networks'))
            kwargs['networks'] = networks
            if concurrency:
                kwargs['concurrency'] = concurrency
            app = cls(*args, **kwargs)
            app.run()
        command()
