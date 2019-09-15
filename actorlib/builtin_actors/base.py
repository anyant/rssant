import actorlib.node


class BuiltinActorBase:
    def __init__(self, app: "actorlib.node.ActorNode"):
        self.app = app

    def op_inbox(self, dst, *, src, expire_at=None):
        message = self.app.registery.create_message(
            dst=dst,
            dst_node=self.app.name,
            src=src,
            expire_at=expire_at,
            priority=0,
        )
        self.app.queue.op_inbox(message)
