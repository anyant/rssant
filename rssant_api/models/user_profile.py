import time
from typing import Optional

from django.contrib.auth.models import AbstractUser

from rssant_common.ezrevenue import EZREVENUE_CLIENT

from .helper import JSONField, Model, User, models, optional


class UserProfile(Model):
    """用户关联信息"""

    class Meta:
        indexes = [
            models.Index(fields=['user']),
        ]
        constraints = [
            models.UniqueConstraint(fields=['user'], name='userprofile_unique_user'),
        ]

    class Admin:
        display_fields = ['user', 'vip_balance']

    user = models.ForeignKey(User, on_delete=models.CASCADE)
    vip_balance: int = models.BigIntegerField(**optional, verbose_name='会员余额')
    vip_info: dict = JSONField(**optional, verbose_name='会员信息')

    def is_vip(self, now=None):
        if now is None:
            now = int(time.time())
        # 会员余额为None可能是未同步会员信息，当作会员处理
        if self.vip_balance is None:
            return True
        if self.vip_balance >= now:
            return True
        return False

    @classmethod
    def _get_impl(
        cls,
        *,
        user_id: int = None,
    ) -> Optional["UserProfile"]:
        q = UserProfile.objects.filter(user_id=user_id)
        result = q.seal().first()
        return result

    @classmethod
    def get(cls, *, user_id: int):
        return cls._get_impl(user_id=user_id)

    @classmethod
    def is_vip_user(self, user: AbstractUser):
        profile = self.get(user_id=user.id)
        # 未同步会员信息，当作会员处理
        if profile is None:
            return True
        return profile.is_vip()

    @classmethod
    def refresh_vip_info(cls, user: AbstractUser):
        params = dict(
            paywall_alias='paywall_vip',
            customer=dict(
                external_id=user.id,
                nickname=user.username,
                external_dt_created=user.date_joined.isoformat(),
            ),
            include_balance=True,
        )
        vip_info = EZREVENUE_CLIENT.call('customer.info', params)
        vip_balance = cls._get_vip_balance(vip_info)
        profile, _ = UserProfile.objects.update_or_create(
            dict(
                user_id=user.id,
                vip_balance=vip_balance,
                vip_info=vip_info,
            ),
            user_id=user.id,
        )
        return profile

    @classmethod
    def _get_vip_balance(cls, vip_info: dict):
        vip_equity_alias = 'equity_vip'
        for item in vip_info['balance_s']:
            equity_alias = item['equity']['alias']
            if equity_alias == vip_equity_alias:
                return item['balance']
        return None
