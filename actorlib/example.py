import time
import asyncio

from validr import T
import requests
from actorlib import actor, collect_actors, ActorNode, ActorMessage


@actor('rss.fetch_story')
def do_fetch_story(ctx, url: T.url):
    resp = requests.get(url)
    image_urls = resp.images
    ctx.send('rss_sink.save_story', resp.text)
    ctx.send('rss.detect_images', image_urls)


@actor('hello.echo')
def do_echo(ctx, message):
    print(message)
    time.sleep(1)
    ctx.send('hello.async_echo', b'hello')


@actor('hello.async_echo')
async def do_async_echo(ctx, message):
    print(message)
    await asyncio.sleep(1)
    await ctx.send('hello.echo', b'hello')


ACTORS = collect_actors(__name__)


def main():
    app = ActorNode(actors=ACTORS)
    app.executor.submit(ActorMessage(
        content=b'hello', src='app.app', dst='hello.echo'
    ))
    app.run()


if __name__ == "__main__":
    main()
