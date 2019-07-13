import re
import os.path
import importlib
import inspect

from .message import ActorMessage


class Actor:
    def __init__(self, handler):
        self.name = handler.__actor_name__
        self.module = self.get_module(self.name)
        self.handler = handler
        self.is_async = inspect.iscoroutinefunction(handler)

    @staticmethod
    def get_module(name):
        return name.split('.', maxsplit=1)[0]

    def __repr__(self):
        is_async = 'async ' if self.is_async else ''
        return '<{} {}{}>'.format(type(self).__name__, is_async, self.name)

    def __call__(self, ctx):
        return self.handler(ctx, ctx.message)


class ActorContext:
    def __init__(self, executor, actor, state, message):
        self.executor = executor
        self.actor = actor
        self.state = state
        self.message = message

    def send(self, dst, content, dst_node=None):
        msg = ActorMessage(
            src=self.actor.name,
            dst=dst, dst_node=dst_node,
            content=content,
        )
        if self.actor.is_async:
            return self.executor.async_submit(msg)
        else:
            return self.executor.submit(msg)


def actor(name):
    def decorator(f):
        f.__actor_name__ = name
        return f
    return decorator


def import_all_modules(import_name):
    root = importlib.import_module(import_name)
    yield root
    if import_name == "__main__":
        return
    for root_path in set(getattr(root, "__path__", [])):
        root_path = root_path.rstrip("/")
        for root, dirs, files in os.walk(root_path):
            root = root.rstrip("/")
            if "__init__.py" in files:
                module = root[len(root_path):].replace("/", ".")
                if module:
                    module = f"{import_name}{module}"
                else:
                    module = import_name
                yield importlib.import_module(module)
            for filename in files:
                if filename != "__init__.py" and filename.endswith(".py"):
                    module = os.path.splitext(os.path.join(root, filename))[0]
                    module = module[len(root_path):].replace("/", ".")
                    yield importlib.import_module(f"{import_name}{module}")


def import_all_actors(import_name, pattern=".*"):
    visited = set()
    pattern = re.compile(pattern)
    for module in import_all_modules(import_name):
        for obj in vars(module).values():
            if not hasattr(obj, '__actor_name__'):
                continue
            if not (inspect.iscoroutinefunction(obj) or inspect.isfunction(obj)):
                continue
            if obj in visited:
                continue
            if pattern.fullmatch(obj.__name__):
                visited.add(obj)
                yield obj


def collect_actors(*modules):
    actors = set()
    for import_name in modules:
        for handler in import_all_actors(import_name):
            if handler in actors:
                continue
            actors.add(handler)
    return actors
