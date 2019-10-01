import logging
import inspect
import os.path
from itertools import chain

from validr import Compiler
from prometheus_client import start_http_server, Info

from rssant_common.helper import pretty_format_json

from .actor import Actor, collect_actors
from .executor import ActorExecutor
from .registery import ActorRegistery
from .receiver import MessageReceiver
from .network_helper import get_localhost_network
from .queue import ActorMessageQueue
from .storage import ActorLocalStorage
from .client import ActorClient
from .builtin_actors.name import ACTOR_SYSTEM


LOG = logging.getLogger(__name__)


BUILTIN_ACTORS = collect_actors('actorlib.builtin_actors')


class ActorNode:
    def __init__(
        self,
        actors,
        host='0.0.0.0',
        port=8000,
        prometheus_port=None,
        concurrency=100,
        name=None,
        subpath=None,
        networks=None,
        registery_node_spec=None,
        storage_dir_path=None,
        storage_compact_wal_delta=1000,
        queue_max_complete_size=1000,
        max_retry_time=10 * 60,
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
        self.timers = {}
        for handler in chain(actors, BUILTIN_ACTORS):
            if inspect.isclass(handler):
                handler = handler(self)
            x = Actor(handler, schema_compiler=schema_compiler)
            if x.timer is not None:
                self.timers[x.name] = x
            self.actors[x.name] = x
        actor_modules = {x.module for x in self.actors.values()}
        if not name:
            name = f'actor-{port}'
        self.name = name
        if not networks:
            networks = []
        networks.append(get_localhost_network(port=port, subpath=subpath))
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
            storage_path = os.path.join(storage_dir_path, self.name)
            self.storage = ActorLocalStorage(
                dirpath=storage_path, compact_wal_delta=storage_compact_wal_delta)
        else:
            self.storage = None
        self.queue = ActorMessageQueue(
            registery=self.registery,
            actors=self.actors,
            storage=self.storage,
            max_complete_size=queue_max_complete_size,
            concurrency=concurrency,
            max_retry_count=max_retry_count,
            max_retry_time=max_retry_time,
        )
        self.concurrency = concurrency
        self.executor = ActorExecutor(
            self.actors,
            queue=self.queue,
            registery=self.registery,
            concurrency=concurrency,
            token=self.token,
        )
        self.host = host
        self.port = port
        self.prometheus_port = prometheus_port or (port + 1)
        Info("actor_node", "actor node info").info({'name': self.name})
        self.subpath = subpath or ''
        self.receiver = MessageReceiver(
            host=self.host, port=self.port, subpath=self.subpath,
            queue=self.queue, registery=self.registery, token=self.token)
        self._client = None
        self._on_startup_handlers = []
        self._on_shutdown_handlers = []
        if on_startup:
            self._on_startup_handlers.extend(on_startup)
        if on_shutdown:
            self._on_shutdown_handlers.extend(on_shutdown)

    @property
    def client(self):
        if self._client is None:
            self._client = ActorClient(registery=self.registery, token=self.token)
        return self._client

    def on_startup(self, handler):
        self._on_startup_handlers.append(handler)
        return handler

    def on_shutdown(self, handler):
        self._on_shutdown_handlers.append(handler)
        return handler

    def _send_system_init_message(self):
        msg = self.registery.create_message(
            src=ACTOR_SYSTEM,
            dst=ACTOR_SYSTEM,
            dst_node=self.name,
            priority=0,
        )
        self.queue.op_inbox(msg)

    def _close(self):
        self.executor.shutdown()
        if self._client:
            self._client.close()

    def run(self):
        self.executor.start()
        LOG.info(f'Actor Node {self.name} at http://{self.host}:{self.port}{self.subpath} started')
        LOG.info(f'current registery:\n{pretty_format_json(self.registery.to_spec())}')
        self.queue.op_restart()
        start_http_server(port=self.prometheus_port)
        LOG.info(f'prometheus exporter at http://{self.host}:{self.prometheus_port} started')
        try:
            for handler in self._on_startup_handlers:
                handler(self)
            self._send_system_init_message()
            self.receiver.run()
        finally:
            try:
                for handler in self._on_shutdown_handlers:
                    handler(self)
            finally:
                self._close()

    def ask(self, dst, content=None, dst_node=None):
        """Send request and wait response"""
        if not dst_node:
            dst_node = self.registery.choice_dst_node(dst)
        msg = self.registery.create_message(
            is_ask=True,
            content=content,
            src=ACTOR_SYSTEM,
            dst=dst,
            dst_node=dst_node,
        )
        return self.client.ask(msg)
