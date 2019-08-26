import time
import logging
from threading import Thread

from .storage import ActorStorageBase
from .helper import unsafe_kill_thread


LOG = logging.getLogger(__name__)


class ActorStorageCompactor:
    def __init__(self, storage: ActorStorageBase, interval=180):
        self.storage = storage
        self.interval = interval
        self._thread = None

    def main(self):
        while True:
            try:
                time.sleep(self.interval)
                if self.storage.should_compact():
                    self.storage.compact()
            except Exception as ex:
                LOG.error(f'compact failed: {ex}', exc_info=ex)

    def start(self):
        self._thread = Thread(target=self.main, name='actor_storage_compactor')
        self._thread.daemon = True
        self._thread.start()

    def shutdown(self):
        self._stop = True
        if self._thread and self._thread.is_alive():
            unsafe_kill_thread(self._thread.ident)

    def join(self):
        if self._thread:
            self._thread.join()
