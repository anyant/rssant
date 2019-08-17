from rssant_common.actor_helper import start_actor


if __name__ == "__main__":
    start_actor(
        'rssant_worker',
        name='worker',
        concurrency=500,
        port=6792,
    )
