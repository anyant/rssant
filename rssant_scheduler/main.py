from actorlib import actor, ActorContext

from rssant_common.actor_helper import start_actor


@actor('actor.init')
def do_init(ctx: ActorContext):
    ctx.tell('scheduler.load_registery')
    ctx.tell('scheduler.schedule_check_feed')
    ctx.tell('scheduler.schedule_clean_feed_creation')
    # ctx.tell('scheduler.healthcheck')


if __name__ == "__main__":
    start_actor(
        'rssant_scheduler',
        name='scheduler',
        concurrency=100,
        port=6790,
        is_scheduler=True,
    )
