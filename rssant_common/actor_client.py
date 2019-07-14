from actorlib import ActorClient, ActorMessage, ActorRegistery


registery = ActorRegistery(current_node_spec={
    'name': 'actor_client',
    'modules': [],
    'networks': [{
        'name': 'local',
        'url': 'http://127.0.0.1:6788',
    }]
}, registery_node_spec={
    'name': 'scheduler',
    'modules': ['scheduler'],
    'networks': [{
        'name': 'local',
        'url': 'http://127.0.0.1:6790/api/v1/scheduler',
    }]
})


actor_client = ActorClient(registery=registery)


def schedule_task(dst, content):
    actor_client.send(ActorMessage(
        dst='scheduler.schedule',
        content=dict(
            dst=dst,
            content=content,
        )
    ))


def batch_schedule_task(tasks):
    messages = []
    for t in tasks:
        messages.append(ActorMessage(
            dst='scheduler.schedule',
            content=dict(
                dst=t['dst'],
                content=t['content'],
            )
        ))
    actor_client.send(*messages)
