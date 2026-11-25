import logging

from django.utils import timezone

from .helper import Model, models, optional

LOG = logging.getLogger(__name__)


class RsshubInstance(Model):
    """RSSHub 实例健康状态，冷却到期前跳过该实例

    只在传输层失败（连接/超时/DNS/SSL）和 429 时写入冷却：
    任何 HTTP 响应（403/404/5xx/200 非 Feed）都可能是路由级问题，
    冷却整个实例会误伤它在其他路由上的能力。
    """

    class Meta:
        indexes = [
            models.Index(fields=['cooldown_until']),
        ]

    class Admin:
        display_fields = ['url', 'cooldown_until']

    url = models.TextField(unique=True, help_text="实例地址")
    cooldown_until = models.DateTimeField(**optional, help_text="冷却到期时间")

    @classmethod
    def cooling_urls(cls, now=None) -> set:
        """返回当前在冷却中的实例地址集合"""
        if now is None:
            now = timezone.now()
        q = cls.objects.filter(cooldown_until__gt=now).values_list('url', flat=True)
        return set(q)

    @classmethod
    def set_cooldown(cls, url: str, seconds: int):
        """写入冷却；seconds 会被限制在 1 秒到 30 分钟之间"""
        from rssant_feedlib.rsshub import MAX_COOLDOWN_SECONDS

        seconds = max(1, min(int(seconds), MAX_COOLDOWN_SECONDS))
        cooldown_until = timezone.now() + timezone.timedelta(seconds=seconds)
        cls.objects.update_or_create(
            url=url, defaults=dict(cooldown_until=cooldown_until)
        )
        return cooldown_until

    @classmethod
    def clear_cooldown(cls, url: str):
        cls.objects.filter(url=url).update(cooldown_until=None)
