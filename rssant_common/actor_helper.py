import logging
import time
import functools
import os.path

import django
from django import db
from validr import T
import backdoor
from actorlib import actor, collect_actors, ActorNode, NodeSpecSchema
from actorlib.sentry import sentry_init

from rssant_common.helper import pretty_format_json
from rssant_common.validator import compiler as schema_compiler
from rssant.settings import ENV_CONFIG
from rssant_common.logger import configure_logging


LOG = logging.getLogger(__name__)


def django_context(f):

    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        db.reset_queries()
        db.close_old_connections()
        try:
            return f(*args, **kwargs)
        finally:
            db.close_old_connections()

    return wrapper


@actor('actor.health')
def do_health(ctx):
    nodes = pretty_format_json(ctx.registery.to_spec())
    LOG.info(f'receive healthcheck message {ctx.message}:\n{nodes}')


@actor('actor.update_registery')
def do_update_registery(ctx, nodes: T.list(NodeSpecSchema)):
    LOG.info(f'update registery {ctx.message}')
    ctx.registery.update(nodes)
    nodes = pretty_format_json(ctx.registery.to_spec())
    LOG.info(f'current registery:\n' + nodes)


def on_startup(app):
    while True:
        try:
            r = app.ask('scheduler.register', dict(node=app.registery.current_node.to_spec()))
        except Exception as ex:
            LOG.warning(f'ask scheduler.register failed: {ex}')
            time.sleep(3)
        else:
            app.registery.update(r['nodes'])
            break
    nodes = pretty_format_json(app.registery.to_spec())
    LOG.info(f'current registery:\n' + nodes)


def on_shutdown(app):
    try:
        app.ask('scheduler.unregister', dict(node_name=app.name))
    except Exception as ex:
        LOG.warning(f'ask scheduler.unregister failed: {ex}')


def start_actor(*modules, name, is_scheduler=False, **kwargs):
    configure_logging()
    if ENV_CONFIG.sentry_enable:
        sentry_init(ENV_CONFIG.sentry_dsn)
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'rssant.settings')
    django.setup()
    backdoor.setup()
    actors = collect_actors('rssant_common.actor_helper', *modules)
    if not is_scheduler:
        kwargs.update(
            on_startup=[on_startup],
            on_shutdown=[on_shutdown],
        )
    if ENV_CONFIG.storage_path:
        storage_path = os.path.expanduser(ENV_CONFIG.storage_path)
        storage_dir_path = os.path.abspath(os.path.join(storage_path, name))
        os.makedirs(storage_dir_path, exist_ok=True)
    else:
        storage_dir_path = None
    ActorNode.cli(
        actors=actors,
        name=name,
        subpath=f'/api/v1/{name}',
        registery_node_spec=ENV_CONFIG.registery_node_spec,
        schema_compiler=schema_compiler,
        storage_dir_path=storage_dir_path,
        **kwargs
    )
