import logging
import typing
from concurrent.futures import Future, ThreadPoolExecutor
from queue import Empty as QueueEmpty
from queue import Queue

import click
from django.contrib.auth import get_user_model
from tqdm import tqdm

import rssant_common.django_setup  # noqa:F401
from rssant_api.models import UserProfile

LOG = logging.getLogger(__name__)


@click.group()
def main():
    """User Commands"""


def _run_sync_vip_info(queue: Queue, progress: tqdm):
    while True:
        try:
            user = queue.get(block=False)
        except QueueEmpty:
            return
        UserProfile.sync_vip_info(user=user)
        progress.update(1)


@click.option('--user-id', type=int, required=False, help='User ID')
@main.command()
def sync_vip_info(user_id: typing.Optional[int] = None):
    User = get_user_model()
    if user_id is not None:
        user_s = User.objects.filter(id=user_id).all()
    else:
        user_s = User.objects.all()
    LOG.info(f'sync_vip_info user count={len(user_s)}')
    queue = Queue()
    for user in user_s:
        queue.put(user)
    progress = tqdm(total=queue.qsize(), ascii=True, ncols=80)
    pool = ThreadPoolExecutor(max_workers=20)
    try:
        fut_s: typing.List[Future] = []
        for _ in range(20):
            fut = pool.submit(_run_sync_vip_info, queue, progress)
            fut_s.append(fut)
        for fut in fut_s:
            fut.result()
    finally:
        progress.close()
        pool.shutdown(wait=True)


if __name__ == "__main__":
    main()
