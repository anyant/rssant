from rssant_common.actor_helper import start_actor


if __name__ == "__main__":
    start_actor(
        'rssant_harbor',
        name='harbor',
        concurrency=200,
        port=6791,
    )
