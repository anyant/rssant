from actorlib import actor, ActorContext

from rssant_common.actor_helper import start_actor


@actor('actor.init')
def do_init(ctx: ActorContext):
    ctx.hope('scheduler.load_registery')
    ctx.hope('scheduler.schedule_check_feed')
    ctx.hope('scheduler.schedule_clean_feed_creation')
    # ctx.hope('scheduler.healthcheck')


if __name__ == "__main__":
    start_actor('scheduler', port=6790)
