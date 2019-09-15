from .state2 import ActorState


class ActorStorage:
    def __init__(self):
        self._wal = []

    def load(self, state: ActorState):
        for item in self._wal:
            state.apply(**item)

    def save(self, type, **kwargs):
        self._wal.append({'type': type, **kwargs})
