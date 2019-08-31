import logging
import os.path
from urllib.parse import urlparse

import click
from validr import Compiler

from rssant_common.helper import pretty_format_json

from .actor import Actor, actor
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
        name_prefix=None,
        subpath=None,
        networks=None,
        registery_node_spec=None,
        storage_dir_path=None,
        storage_max_pending_size=10**2,
        storage_max_done_size=10**3,
        storage_compact_interval=60,
        ack_timeout=180,
        max_retry_count=3,
        token=None,
        schema_compiler=None,
        on_startup=None,
        on_shutdown=None,
    ):
        if schema_compiler is None:
            schema_compiler = Compiler()
        self.schema_compiler = schema_compiler
        self.actors = {}
        for handler in [self.do_actor_health] + list(actors):
            x = Actor(handler, schema_compiler=schema_compiler)
            self.actors[x.name] = x
        actor_modules = {x.module for x in self.actors.values()}
        if not name:
            prefix = '{}-'.format(name_prefix) if name_prefix else ''
            name = '{}{}-{}'.format(prefix, LOCAL_NODE_NAME, port)
        self.name = name
        if not networks:
            networks = []
        networks.extend(get_local_networks(port=port, subpath=subpath))
        current_node_spec = dict(
            name=self.name,
            modules=actor_modules,
            networks=networks,
        )
        self.token = token
        self.registery = ActorRegistery(
            current_node_spec=current_node_spec,
            registery_node_spec=registery_node_spec)
        if storage_dir_path:
            storage_dir_path = os.path.abspath(os.path.expanduser(storage_dir_path))
            self.storage_dir_path = storage_dir_path
            storage_path = os.path.join(storage_dir_path, self.name)
            os.makedirs(storage_path, exist_ok=True)
            self.storage = ActorLocalStorage(
                dir_path=storage_path,
                max_pending_size=storage_max_pending_size,
                max_done_size=storage_max_done_size,
            )
            self.storage_compactor = ActorStorageCompactor(
                self.storage, interval=storage_compact_interval)
        else:
            LOG.info('storage_dir_path not set, will use memory storage')
            self.storage_dir_path = None
            self.storage = ActorMemoryStorage(
                max_pending_size=storage_max_pending_size,
                max_done_size=storage_max_done_size,
            )
            self.storage_compactor = None
        self.concurrency = concurrency
        self.sender = MessageSender(
            concurrency=concurrency, token=self.token,
            storage=self.storage, registery=self.registery)
        self.executor = ActorExecutor(
            self.actors, sender=self.sender, storage=self.storage,
            registery=self.registery, concurrency=concurrency, token=self.token)
        self.message_monitor = ActorMessageMonitor(
            self.storage, self.executor, self.sender,
            ack_timeout=ack_timeout, max_retry_count=max_retry_count,
        )
        self.host = host
        self.port = port
        self.subpath = subpath or ''
        self.receiver = MessageReceiver(
            host=self.host, port=self.port, subpath=self.subpath,
            executor=self.executor, registery=self.registery, token=self.token)
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

    @actor('actor.health')
    async def do_actor_health(self, ctx):
        """Report health metrics"""
        return self.health()

    def health(self):
        # registery
        registery_info = {}
        registery_info['current_node'] = self.registery.current_node.to_spec()
        if self.registery.registery_node:
            registery_info['registery_node'] = self.registery.registery_node.to_spec()
        else:
            registery_info['registery_node'] = None
        registery_info['nodes'] = self.registery.to_spec()
        # storage
        storage_info = dict(
            max_pending_size=self.storage.max_pending_size,
            max_done_size=self.storage.max_done_size,
            current_wal_size=self.storage.current_wal_size,
            num_begin_messages=self.storage.num_begin_messages,
            num_send_messages=self.storage.num_send_messages,
            num_pending_messages=self.storage.num_pending_messages,
            num_done_messages=self.storage.num_done_messages,
            num_messages=self.storage.num_messages,
        )
        if isinstance(self.storage, ActorLocalStorage):
            storage_info.update(
                dir_path=self.storage.dir_path,
                current_filepath=self.storage.current_filepath,
            )
        # storage_compactor
        storage_compactor_info = {}
        if self.storage_compactor:
            storage_compactor_info.update(
                interval=self.storage_compactor.interval,
            )
        return dict(
            name=self.name,
            host=self.host,
            port=self.port,
            subpath=self.subpath,
            concurrency=self.concurrency,
            registery=registery_info,
            storage=storage_info,
            storage_compactor=storage_compactor_info,
            receiver=dict(),  # TODO: receiver/aiohttp metrics
            sender=dict(
                outbox_size=self.sender.outbox.qsize(),
                message_state_size=self.sender.message_state_size,
            ),
            executor=dict(
                concurrency=self.executor.concurrency,
                num_async_workers=self.executor.num_async_workers,
                num_pool_workers=self.executor.num_pool_workers,
                num_thread_workers=self.executor.num_thread_workers,
                thread_inbox_size=self.executor.thread_inbox.qsize(),
                async_inbox_size=self.executor.async_inbox.qsize(),
            ),
            message_monitor=dict(
                ack_timeout=self.message_monitor.ack_timeout,
                max_retry_count=self.message_monitor.max_retry_count,
            )
        )

    def print_health(self):
        pretty_format_json(self.health())

    def _send_init_message(self):
        if 'actor.init' not in self.actors:
            return
        self.hope('actor.init', dst_node=self.registery.current_node.name)

    def _create_message(self, dst, content=None, dst_node=None, **kwargs):
        if content is None:
            content = {}
        msg = ActorMessage(
            content=content, src='actor.init',
            dst=dst, dst_node=dst_node,
            **kwargs,
        )
        msg = self.registery.complete_message(msg)
        return msg

    def ask(self, dst, content=None, dst_node=None):
        msg = self._create_message(
            dst, content=content, dst_node=dst_node, is_ask=True)
        if self.registery.is_local_message(msg):
            return self.executor.handle_ask(msg)
        else:
            return self.executor.main_thread_client.ask(msg)

    def tell(self, dst, content=None, dst_node=None):
        msg = self._create_message(
            dst, content=content, dst_node=dst_node, require_ack=True)
        if self.registery.is_local_message(msg):
            return self.executor.submit(msg)
        else:
            return self.sender.submit(msg)

    def hope(self, dst, content=None, dst_node=None):
        msg = self._create_message(
            dst, content=content, dst_node=dst_node, require_ack=False)
        if self.registery.is_local_message(msg):
            return self.executor.submit(msg)
        else:
            return self.sender.submit(msg)

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
