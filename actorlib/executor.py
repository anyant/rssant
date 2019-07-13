import queue
from threading import Thread

import asyncio
import aiojobs

from .actor import ActorContext
from .helper import kill_thread


class ActorExecutor:
    def __init__(self, actors, sender, registery, concurrency=100):
        self.actors = actors
        self.sender = sender
        self.registery = registery
        if concurrency <= 3:
            num_async_workers = 1
        elif concurrency <= 10:
            num_async_workers = 2
        else:
            num_async_workers = 3
        num_thread_workers = min(1, concurrency - num_async_workers)
        self.num_async_workers = num_async_workers
        self.num_thread_workers = num_thread_workers
        self.concurrency = num_async_workers + num_thread_workers
        self.thread_inbox = queue.Queue(self.concurrency)
        self.async_inbox = queue.Queue(self.concurrency)
        self.threads = []

    async def _async_get_message(self, box):
        while True:
            try:
                return box.get_nowait()
            except queue.Empty:
                await asyncio.sleep(0.1)

    async def _async_put_message(self, box, message):
        while True:
            try:
                return box.put_nowait(message)
            except queue.Full:
                await asyncio.sleep(0.1)

    async def _async_main(self):
        scheduler = await aiojobs.create_scheduler(
            limit=self.concurrency, pending_limit=self.concurrency)
        state = {}
        while True:
            message = await self._async_get_message(self.async_inbox)
            actor = self.actors[message.dst]
            ctx = ActorContext(
                executor=self, actor=actor, state=state, message=message)
            await scheduler.spawn(actor(ctx))

    def _send_message(self, message):
        message = self.registery.complete_message(message)
        if self.registery.is_local_message(message):
            self.submit(message)
        else:
            self.sender.submit(message)

    async def _async_send_message(self, message):

        if self.registery.is_local_message(message):
            self.submit(message)
        else:
            self.sender.submit(message)

    def async_main(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._async_main())

    def thread_main(self):
        state = {}
        while True:
            message = self.thread_inbox.get()
            actor = self.actors[message.dst]
            ctx = ActorContext(
                executor=self, actor=actor, state=state, message=message)
            actor(ctx)

    async def async_submit(self, message):
        message = self.registery.complete_message(message)
        if not self.registery.is_local_message(message):
            await self.sender.async_submit(message)
            return
        actor = self.actors[message.dst]
        if actor.is_async:
            await self._async_put_message(self.async_inbox, message)
        else:
            await self._async_put_message(self.thread_inbox, message)

    def submit(self, message):
        message = self.registery.complete_message(message)
        if not self.registery.is_local_message(message):
            self.sender.submit(message)
            return
        actor = self.actors[message.dst]
        if actor.is_async:
            self.async_inbox.put(message)
        else:
            self.thread_inbox.put(message)

    def start(self):
        for i in range(self.num_async_workers):
            t = Thread(target=self.async_main)
            self.threads.append(t)
        for i in range(self.num_thread_workers):
            t = Thread(target=self.thread_main)
            self.threads.append(t)
        for t in self.threads:
            t.daemon = True
            t.start()

    def shutdown(self):
        for t in self.threads:
            kill_thread(t.ident)
