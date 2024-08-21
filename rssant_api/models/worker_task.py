from typing import List, Optional

from django.db import connection
from django.utils import timezone

from .helper import JSONField, Model, models


class WorkerTask(Model):
    """任务缓存队列"""

    class Meta:
        indexes = [
            models.Index(fields=['key']),
            models.Index(fields=['dt_expired']),
            models.Index(fields=['priority', 'dt_created']),
        ]
        constraints = [
            models.UniqueConstraint(fields=['key'], name='unique_key'),
        ]

    class Admin:
        display_fields = ['key', 'api', 'priority', 'dt_created']

    key = models.CharField(max_length=255, verbose_name='唯一标识')
    priority = models.IntegerField(verbose_name='优先级', default=0)
    api = models.CharField(max_length=255, verbose_name='任务API')
    data = JSONField(max_length=1024 * 1024, verbose_name='任务数据')
    dt_created = models.DateTimeField(auto_now_add=True, help_text="创建时间")
    dt_expired = models.DateTimeField(help_text="过期时间")

    def to_dict(self):
        return dict(
            key=self.key,
            priority=self.priority,
            api=self.api,
            data=self.data,
            dt_created=self.dt_created,
            dt_expired=self.dt_expired,
        )

    @classmethod
    def from_dict(
        self,
        *,
        key: str,
        api: str,
        data: dict,
        priority: Optional[int] = None,
        dt_created: Optional[timezone.datetime] = None,
        dt_expired: Optional[timezone.datetime] = None,
        expired_seconds: Optional[int] = None,
    ):
        if priority is None:
            priority = 0
        if dt_created is None:
            dt_created = timezone.now()
        if dt_expired is None:
            if expired_seconds is None:
                expired_seconds = 24 * 60 * 60
            expired_delta = timezone.timedelta(seconds=expired_seconds)
            dt_expired = dt_created + expired_delta
        return WorkerTask(
            key=key,
            api=api,
            data=data,
            priority=priority,
            dt_created=dt_created,
            dt_expired=dt_expired,
        )

    @classmethod
    def delete_all_expired(cls, *, now: Optional[timezone.datetime] = None):
        if now is None:
            now = timezone.now()
        q = WorkerTask.objects.filter(dt_expired__lt=now)
        num_deleted, __ = q.delete()
        return num_deleted

    @classmethod
    def bulk_save(cls, task_obj_s: List["WorkerTask"]):
        # TODO: django 4.2 support bulk_create with update_conflicts
        for task_obj in task_obj_s:
            value = task_obj.to_dict()
            WorkerTask.objects.update_or_create(value, key=task_obj.key)

    @classmethod
    def poll(cls):
        """
        从队列中取出一个任务
        """
        table_name = cls._meta.db_table
        column_s = [x.name for x in cls._meta.fields]
        sql = f'''
DELETE FROM {table_name} WHERE "id" IN (
    SELECT "id" FROM {table_name}
    ORDER BY "priority" DESC, "dt_created"
    LIMIT 1
) RETURNING *
'''
        with connection.cursor() as cursor:
            cursor.execute(sql)
            row = cursor.fetchone()
        if row is None:
            return None
        ret = WorkerTask(**dict(zip(column_s, row)))
        return ret
