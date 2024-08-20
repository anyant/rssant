import asyncio
import logging
from threading import Thread
from typing import List

from rssant_common.logger import configure_logging
from rssant_common.service_client import SERVICE_CLIENT
from rssant_config import CONFIG

from .task import SCHEDULER_TASK_S

LOG = logging.getLogger(__name__)


class TimerTask:
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
        init_delay = max(3, self._delay / 10)
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


class TimerThread:
    def __init__(self) -> None:
        task_s: List[TimerTask] = []
        for task in SCHEDULER_TASK_S:
            task_s.append(TimerTask(task=task))
        self.task_s = task_s
        self._thread: Thread = None

    def _main(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._main_async())

    async def _main_async(self):
        for task in self.task_s:
            await task.start()
        for task in self.task_s:
            await task.join()

    def start(self):
        name = 'rssant-scheduler-timer'
        thread = Thread(target=self._main, name=name, daemon=True)
        self._thread = thread
        thread.start()

    def join(self, timeout=None):
        if self._thread is not None:
            self._thread.join(timeout=timeout)


def main():
    configure_logging(level=CONFIG.log_level)
    timer = TimerThread()
    timer.start()
    timer.join()


if __name__ == '__main__':
    main()
