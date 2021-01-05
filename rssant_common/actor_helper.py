import logging
import time
import functools
from contextlib import contextmanager
from urllib.parse import urlparse

import click
from django import db
from django.db import connection
from validr import T
import backdoor
from pyinstrument import Profiler
from actorlib import actor, collect_actors, ActorNode, NodeSpecSchema
from actorlib.sentry import sentry_init

import rssant_common.django_setup  # noqa:F401
from rssant_config import CONFIG
from rssant_common.helper import pretty_format_json
from rssant_common.validator import compiler as schema_compiler
from rssant_common.logger import configure_logging
from rssant_common.kong_client import KongClient
from rssant_common.dns_service import DNS_SERVICE


LOG = logging.getLogger(__name__)


def django_context(f):

    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        with log_django_context_metric(f.__name__):
            db.reset_queries()
            db.close_old_connections()
            try:
                return f(*args, **kwargs)
            finally:
                db.close_old_connections()

    return wrapper


def profile_django_context(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        profiler = None
        if CONFIG.profiler_enable:
            profiler = Profiler()
            profiler.start()
        try:
            return f(*args, **kwargs)
        finally:
            if profiler is not None:
                profiler.stop()
                print(profiler.output_text(unicode=True, color=True))
    return wrapper


@contextmanager
def log_django_context_metric(name, profiler_enable=False):
    begin_time = time.time()
    try:
        yield
    finally:
        num_sqls = len(connection.queries)
        sql_cost = sum(float(x['time']) for x in connection.queries) * 1000
        sql_time = f'{num_sqls},{sql_cost:.0f}ms'
        cost = int((time.time() - begin_time) * 1000)
        LOG.info(f'{name} sql=%s cost=%dms', sql_time, cost)


@actor('actor.update_registery')
def do_update_registery(ctx, nodes: T.list(NodeSpecSchema)):
    LOG.info(f'update registery {ctx.message}')
    ctx.registery.update(nodes)
    nodes = pretty_format_json(ctx.registery.to_spec())
    LOG.info('current registery:\n' + nodes)


@actor('actor.dns_service_update')
def do_dns_service_update(ctx, records: T.dict.key(T.str).value(T.list(T.str))):
    LOG.info('dns_service_update %r', records)
    DNS_SERVICE.update(records)


@actor('actor.keepalive', timer='60s')
async def do_keepalive(ctx):
    try:
        r = await ctx.ask('scheduler.register', dict(node=ctx.registery.current_node.to_spec()))
    except Exception as ex:
        LOG.warning(f'ask scheduler.register failed: {ex}')
    else:
        ctx.registery.update(r['nodes'])


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
    LOG.info('current registery:\n' + nodes)
    if app.kong_client:
        LOG.info(f'kong register {app.name} url={app.kong_actor_url}')
        while True:
            try:
                app.kong_client.register(app.name, app.kong_actor_url)
            except Exception as ex:
                LOG.warning(f'kong register failed: {ex}')
                time.sleep(3)
            else:
                break


def on_shutdown(app):
    try:
        app.ask('scheduler.unregister', dict(node_name=app.name))
    except Exception as ex:
        LOG.warning(f'ask scheduler.unregister failed: {ex}')
    if app.kong_client:
        LOG.info(f'kong unregister {app.name}')
        try:
            app.kong_client.unregister(app.name)
        except Exception as ex:
            LOG.warning(f'kong unregister failed: {ex}')


def start_actor_cli(*args, actor_type, **kwargs):

    default_port = kwargs.get('port', 6790)
    default_concurrency = kwargs.get('concurrency', 100)

    @click.command()
    @click.option('--node', default='localhost', help='actor node name')
    @click.option('--host', default='0.0.0.0', help='listen host')
    @click.option('--port', type=int, default=default_port, help='listen port')
    @click.option('--network', multiple=True, help='network@http://host:port')
    @click.option('--concurrency', type=int, default=default_concurrency, help='concurrency')
    @click.option('--kong-admin-url', type=str, default='http://localhost:8001', help='kong admin url')
    @click.option('--kong-actor-host', type=str, help='actor host for kong to connect')
    def command(node, host, port, network, concurrency, kong_admin_url, kong_actor_host=None):
        is_scheduler = actor_type == 'scheduler'
        kwargs['host'] = host
        kwargs['port'] = port
        if is_scheduler:
            name = 'scheduler'
            subpath = '/api/v1/scheduler'
        else:
            name = '{}-{}-{}'.format(actor_type, node, port)
            subpath = '/api/v1/{}/{}-{}'.format(actor_type, node, port)
        kwargs.update(name=name, subpath=subpath)
        network_specs = []
        for network_spec in network:
            name, url = network_spec.split('@', maxsplit=1)
            network_specs.append(dict(name=name, url=url))
        if kwargs.get('networks'):
            network_specs.extend(kwargs.get('networks'))
        networks = []
        for spec in network_specs:
            url = urlparse(spec['url'])
            if (not url.scheme) or (not url.netloc):
                raise ValueError('invalid network url: {url}')
            networks.append(dict(
                name=spec['name'],
                url=f'{url.scheme}://{url.netloc}{subpath}'
            ))
        kwargs['networks'] = networks
        kwargs['concurrency'] = concurrency
        app = ActorNode(*args, **kwargs)
        if kong_actor_host:
            kong_actor_url = f'http://{kong_actor_host}:{port}{subpath}'
            client = KongClient(kong_admin_url)
            app.kong_client = client
            app.kong_actor_url = kong_actor_url
        else:
            app.kong_client = None
            app.kong_actor_url = None
        app.run()
    return command()


def start_actor(actor_type, **kwargs):
    configure_logging(level=CONFIG.log_level)
    if CONFIG.sentry_enable:
        sentry_init(CONFIG.sentry_dsn)
    backdoor.setup()
    is_scheduler = actor_type == 'scheduler'
    actors = list(collect_actors(f'rssant_{actor_type}'))
    if not is_scheduler:
        actors.extend([
            do_update_registery,
            do_keepalive,
            do_dns_service_update,
        ])
        kwargs.update(
            on_startup=[on_startup],
            on_shutdown=[on_shutdown],
        )
    start_actor_cli(
        actor_type=actor_type,
        actors=actors,
        registery_node_spec=CONFIG.registery_node_spec,
        schema_compiler=schema_compiler,
        storage_dir_path=CONFIG.actor_storage_path,
        storage_compact_wal_delta=CONFIG.actor_storage_compact_wal_delta,
        queue_max_complete_size=CONFIG.actor_queue_max_complete_size,
        max_retry_time=CONFIG.actor_max_retry_time,
        max_retry_count=CONFIG.actor_max_retry_count,
        token=CONFIG.actor_token,
        **kwargs
    )
