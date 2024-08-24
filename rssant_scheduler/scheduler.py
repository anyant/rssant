import asyncio
import logging
import os
import random
import signal
import time
from threading import Thread
from typing import List

from rssant_common.service_client import SERVICE_CLIENT
from rssant_config import CONFIG

from .timer_task import SCHEDULER_TASK_S

LOG = logging.getLogger(__name__)


class BaseTask:
    async def start(self):
        raise NotImplementedError

    async def join(self):
        raise NotImplementedError


class TimerTask(BaseTask):
    def __init__(self, task: dict) -> None:
        self.task = task
        self._task_obj = None

    @property
    def _delay(self):
        return int(self.task['timer'].total_seconds())

    @property
    def _name(self):
        return 'timer:' + self.task['api']

    async def start(self):
        loop = asyncio.get_event_loop()
        self._task_obj = loop.create_task(self._execute(), name=self._name)

    async def join(self):
        if self._task_obj is not None:
            await self._task_obj

    async def _execute(self):
        LOG.info("schedule {} every {} seconds".format(self._name, self._delay))
        init_delay = max(3, self._delay / 10) + random.random() * 3
        await asyncio.sleep(init_delay)
        while True:
            await self._execute_one_safe()
            await asyncio.sleep(self._delay)

    async def _execute_one_safe(self):
        try:
            await SERVICE_CLIENT.acall(self.task['api'])
        except Exception as ex:
            LOG.exception('%s failed: %r', self._name, ex, exc_info=ex)
        else:
            LOG.info('%s execute success', self._name)


class WorkerGetTaskService:
    """
    限流调用get_task，没有任务时，每隔3秒调用一次
    """

    def __init__(self) -> None:
        self._has_task = True
        self._no_task_wait = 3
        self._last_call_time = None

    def _check_call(self):
        if self._has_task:
            return True
        now = time.monotonic()
        if self._last_call_time is None:
            self._last_call_time = now
            return True
        if now - self._last_call_time >= self._no_task_wait:
            self._last_call_time = now
            return True
        return False

    async def get_task(self):
        if not self._check_call():
            return None
        result = await SERVICE_CLIENT.acall('harbor_rss.get_task')
        task = result['task']
        self._has_task = bool(task)
        return task


WORKER_GET_TASK_SERVICE = WorkerGetTaskService()


class WorkerTask(BaseTask):
    def __init__(self, index: int) -> None:
        self.index = index
        self._task_obj = None

    @property
    def _name(self):
        return 'worker-' + str(self.index)

    async def start(self):
        loop = asyncio.get_event_loop()
        self._task_obj = loop.create_task(self._execute(), name=f'worker-{self.index}')

    async def join(self):
        if self._task_obj is not None:
            await self._task_obj

    async def _execute(self):
        LOG.info("schedule {} started".format(self._name))
        init_delay = 1 + random.random() * 3
        await asyncio.sleep(init_delay)
        while True:
            await self._execute_one_safe()

    async def _execute_task(self):
        task = await WORKER_GET_TASK_SERVICE.get_task()
        if not task:
            return False
        LOG.info('%s executing task %s', self._name, task['key'])
        await SERVICE_CLIENT.acall(task['api'], data=task['data'], timeout=120)
        return True

    async def _execute_one_safe(self):
        try:
            is_ok = await self._execute_task()
        except Exception as ex:
            LOG.exception('%s failed: %r', self._name, ex, exc_info=ex)
            is_ok = False
        if not is_ok:
            delay = 10 + random.random() * 20
            await asyncio.sleep(delay)


class RssantScheduler:
    def __init__(self, num_worker: int) -> None:
        task_s: List[BaseTask] = []
        task_s.extend(self._get_timer_task_s())
        task_s.extend(self._get_worker_task_s(num_worker))
        self.task_s = task_s
        self.num_worker = num_worker
        self._thread = None

    def _get_timer_task_s(self):
        task_s = [TimerTask(task=task) for task in SCHEDULER_TASK_S]
        return task_s

    def _get_worker_task_s(self, num: int):
        task_s = [WorkerTask(index) for index in range(num)]
        return task_s

    def main(self, *, is_thread=False):
        LOG.info('harbor url: %s', CONFIG.harbor_url)
        LOG.info('worker url: %s', CONFIG.worker_url)
        try:
            asyncio.run(self._main_async())
        except KeyboardInterrupt:
            raise
        except Exception as ex:
            if is_thread:
                LOG.exception('scheduler failed: %r', ex, exc_info=ex)
                self._exit_process()
            raise

    def _exit_process(self):
        """
        退出主进程
        https://stackoverflow.com/questions/905189/why-does-sys-exit-not-exit-when-called-inside-a-thread-in-python
        """
        os.kill(os.getpid(), signal.SIGINT)

    async def _main_async(self):
        for task in self.task_s:
            await task.start()
        fut_s = [task.join() for task in self.task_s]
        for fut in asyncio.as_completed(fut_s):
            await fut

    def start(self):
        thread = Thread(
            target=self.main,
            kwargs=dict(is_thread=True),
            name='rssant-scheduler',
            daemon=True,
        )
        self._thread = thread
        thread.start()

    def join(self):
        if self._thread:
            self._thread.join()
