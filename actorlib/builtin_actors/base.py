import actorlib.node


class BuiltinActorBase:
    def __init__(self, app: "actorlib.node.ActorNode"):
        self.app = app
