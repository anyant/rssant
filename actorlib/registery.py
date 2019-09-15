from typing import List
import random
import logging
from collections import defaultdict
from threading import RLock

from validr import T
from cached_property import cached_property

from .actor import Actor
from .network_helper import LOCAL_NODE_NAME
from .helper import generate_message_id


LOG = logging.getLogger(__name__)


NodeSpecSchema = T.dict(
    name=T.str,
    modules=T.list(T.str),
    networks=T.list(T.dict(
        name=T.str,
        url=T.str.optional,
    ))
)


class NodeInfo:
    def __init__(self, name: str, modules: set, networks: list):
        self.name = name
        self.modules = modules
        self._networks = networks

    def __repr__(self):
        return '<{} #{} {}>'.format(type(self).__name__, self.id, self.name)

    @cached_property
    def networks(self) -> dict:
        networks = defaultdict(set)
        for network in self._networks:
            networks[network['name']].add(network['url'])
        networks = {name: set(x for x in urls if x) for name, urls in networks.items()}
        return networks

    @classmethod
    def from_spec(cls, node):
        networks = []
        for network in node['networks']:
            if network['name'] == 'localhost':
                networks.append(dict(name=LOCAL_NODE_NAME, url=network['url']))
            else:
                networks.append(network)
        return cls(
            name=node['name'],
            modules=set(node['modules']),
            networks=networks,
        )

    def to_spec(self):
        return dict(
            name=self.name,
            modules=list(sorted(self.modules)),
            networks=list(sorted(self._networks, key=lambda x: (x['name'], x['url']))),
        )


class ActorRegistery:

    def __init__(self, *, current_node_spec, registery_node_spec=None, node_specs=None):
        self.current_node = NodeInfo.from_spec(current_node_spec)
        self.current_node_name = self.current_node.name
        self.current_networks = set(self.current_node.networks.keys())
        self.registery_node = None
        if registery_node_spec:
            self.registery_node = NodeInfo.from_spec(registery_node_spec)
        self._nodes = {}
        self._node_index = {}  # node -> urls
        self._module_index = {}  # module -> (node, urls)
        self._lock = RLock()
        self.update(node_specs or [])

    def _update(self, nodes):
        nodes = {x.name: x for x in nodes}
        if self.current_node:
            nodes[self.current_node_name] = self.current_node
        if self.registery_node:
            nodes[self.registery_node.name] = self.registery_node
        node_index = {}
        module_index = defaultdict(set)
        for node in nodes.values():
            urls = set()
            for name in node.networks.keys() & self.current_networks:
                urls.update(node.networks[name])
            node_index[node.name] = list(urls)
            for mod in node.modules:
                module_index[mod].add(node.name)
        self._node_index = node_index
        self._module_index = module_index
        self._nodes = nodes

    def update(self, node_specs):
        nodes = [NodeInfo.from_spec(spec) for spec in node_specs]
        with self._lock:
            self._update(nodes)

    def add(self, node_spec):
        node = NodeInfo.from_spec(node_spec)
        with self._lock:
            nodes = list(self._nodes.values())
            nodes.append(node)
            self._update(nodes)

    def remove(self, node_name):
        with self._lock:
            self._nodes.pop(node_name, None)
            self._update(list(self._nodes.values()))

    def to_spec(self):
        with self._lock:
            ret = [x.to_spec() for x in self._nodes.values()]
            return list(sorted(ret, key=lambda x: x['name']))

    def get(self, name):
        with self._lock:
            return self._nodes.get(name)

    @property
    def nodes(self):
        with self._lock:
            return list(self._nodes.values())

    def find_dst_nodes(self, dst: str) -> List[str]:
        module = Actor.get_module(dst)
        with self._lock:
            return list(self._module_index[module])

    def choice_dst_node(self, dst: str) -> str:
        nodes = self.find_dst_nodes(dst)
        if not nodes:
            return None
        return random.choice(nodes)

    def find_dst_urls(self, dst_node: str) -> List[str]:
        with self._lock:
            return list(self._node_index[dst_node])

    def choice_dst_url(self, dst_node: str) -> str:
        if not dst_node:
            return None
        urls = self.find_dst_urls(dst_node)
        if not urls:
            return None
        return random.choice(urls)

    def complete_message(self, message):
        if self.current_node and not message.src_node:
            message.src_node = self.current_node_name
        if not message.id:
            message.id = self.generate_message_id()
        return message

    def is_local_message(self, message):
        return self.is_local_node(message.dst_node)

    def is_local_node(self, node_name):
        if not self.current_node or not node_name:
            return False
        return node_name == self.current_node_name

    def generate_message_id(self):
        return generate_message_id(self.current_node_name)
