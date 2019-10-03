from aiohttp.web import Response, Request
from prometheus_client import Histogram, Gauge, Counter
from prometheus_client.exposition import choose_encoder, REGISTRY

from .message import ActorMessage


async def aiohttp_metrics_handler(request: Request):
    registry = REGISTRY
    accept = request.headers.get('Accept')
    encoder, content_type = choose_encoder(accept)
    if 'name[]' in request.query:
        name = request.query['name[]']
        registry = registry.restricted_registry(name)
    output = encoder(registry)
    return Response(body=output, headers={'Content-Type': content_type})


ACTOR_QUEUE_OP = Counter(
    'actor_queue_op', 'actor queue op', [
        'op',
        'src',
        'src_node',
        'dst',
        'dst_node',
        'is_ask',
        'require_ack',
        'is_local',
    ]
)


def metric_queue_op(op, message: ActorMessage):
    ACTOR_QUEUE_OP.labels(
        op=op,
        src=message.src,
        src_node=message.src_node,
        dst=message.dst,
        dst_node=message.dst_node,
        is_ask=message.is_ask,
        require_ack=message.require_ack,
        is_local=message.is_local,
    ).inc()


ACTOR_QUEUE_INBOX_SIZE = Gauge(
    'actor_queue_inbox_size', 'inbox size', [
        'dst',
    ]
)


ACTOR_QUEUE_OUTBOX_SIZE = Gauge(
    'actor_queue_outbox_size', 'outbox size', [
        'dst',
    ]
)


ACTOR_EXECUTOR_TIME = Histogram(
    'actor_executor_time', 'actor execute time', [
        'dst',
    ],
    buckets=(
        .005, .01, .025, .05, .075, .1, .25, .5, .75, 1.0, 2.5, 5.0, 7.5,
        10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 120.0,
    )
)
