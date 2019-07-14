from django.utils import timezone
from jsonfield import JSONField

from .helper import Model, models


class Registery(Model):
    """scheduler/worker registery"""

    class Meta:
        indexes = [
            models.Index(fields=["registery_node"]),
        ]

    class Admin:
        display_fields = ['registery_node', 'registery_node_spec', 'dt_updated']

    registery_node = models.CharField(unique=True, max_length=200, help_text='registery node name')
    registery_node_spec = JSONField(help_text="registery node spec")
    node_specs = JSONField(help_text="node specs")
    dt_updated = models.DateTimeField(help_text="更新时间")

    @staticmethod
    def create_or_update(registery_node_spec, node_specs):
        registery_node = registery_node_spec['name']
        obj, created = Registery.objects.update_or_create(
            registery_node=registery_node,
            defaults=dict(
                registery_node_spec=registery_node_spec,
                node_specs=node_specs,
                dt_updated=timezone.now(),
            )
        )
        return obj

    @staticmethod
    def get(registery_node):
        try:
            return Registery.objects.get(registery_node=registery_node)
        except Registery.DoesNotExist:
            return None
