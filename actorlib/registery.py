from typing import List
import random
from collections import defaultdict
from threading import RLock

from validr import T

from .actor import Actor


NodeSpecSchema = T.dict(
    name=T.str,
    modules=T.list(T.str),
    networks=T.list(T.dict(
        name=T.str,
        url=T.url,
    ))
)


class NodeInfo:
    def __init__(self, name, modules, networks):
        self.name = name
        self.modules = modules
        self.networks = networks

    @classmethod
    def from_spec(cls, node):
        networks = defaultdict(set)
        for network in node['networks']:
            networks[network['name']].add(network['url'])
        return cls(
            node['name'],
            set(node['modules']),
            networks=networks,
        )

    def to_spec(self):
        networks = []
        for name, urls in self.networks.items():
            for url in urls:
                networks.append(dict(name=name, url=url))
        return dict(name=self.name, modules=list(self.modules), networks=networks)


class ActorRegistery:

    def __init__(self, current_node_spec, registery_node_spec=None, node_specs=None):
        if registery_node_spec:
            self.registery_node = NodeInfo.from_spec(registery_node_spec)
        else:
            self.registery_node = None
        self.current_node = NodeInfo.from_spec(current_node_spec)
        self._nodes = {}
        self._node_index = {}  # node -> urls
        self._module_index = {}  # module -> (node, urls)
        self._lock = RLock()
        self.update(node_specs or [])

    def _update(self, nodes):
        with self._lock:
            nodes = list(nodes) + [self.current_node]
            if self.registery_node:
                nodes.append(self.registery_node)
            node_index = {}
            module_index = defaultdict(set)
            for node in nodes:
                urls = set()
                for name in node.networks.keys() & self.current_node.networks.keys():
                    urls.update(node.networks[name])
                node_index[node.name] = list(urls)
                for mod in node.modules:
                    module_index[mod].add(node.name)
            self._node_index = node_index
            self._module_index = module_index
            self._nodes = {x.name: x for x in nodes}

    def update(self, node_specs):
        nodes = [NodeInfo.from_spec(spec) for spec in node_specs]
        self._update(nodes)

    def add(self, node_spec):
        nodes = list(self._nodes.values())
        nodes.append(NodeInfo.from_spec(node_spec))
        self._update(nodes)

    def to_spec(self):
        with self._lock:
            return [x.to_spec() for x in self._nodes.values()]

    def find_dst_nodes(self, dst: str) -> List[str]:
        module = Actor.get_module(dst)
        with self._lock:
            return list(self._module_index[module])

    def choice_dst_node(self, dst: str) -> str:
        return random.choice(self.find_dst_nodes(dst))

    def find_dst_urls(self, dst_node: str) -> List[str]:
        with self._lock:
            return list(self._node_index[dst_node])

    def choice_dst_url(self, dst_node: str) -> str:
        return random.choice(self.find_dst_urls(dst_node))

    def complete_message(self, message):
        if not message.src_node:
            message.src_node = self.current_node.name
        if not message.dst_node:
            message.dst_node = self.choice_dst_node(message.dst)
        if message.dst_node != self.current_node.name:
            if not message.dst_url:
                message.dst_url = self.choice_dst_url(message.dst_node)
        return message

    def is_local_message(self, message):
        return message.dst_node == self.current_node.name
