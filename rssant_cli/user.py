import logging
import typing
import io

import click
from shopant_sdk import ShopAntServerError
import rssant_common.django_setup  # noqa:F401
from django.utils import timezone
from django.contrib.auth.models import User
from django.db.models import Q
from rssant_common.shopant import SHOPANT_SERVER


LOG = logging.getLogger(__name__)


@click.group()
def main():
    """User Commands"""


def _search_user(user) -> typing.List[User]:
    try:
        user_id = int(user)
    except ValueError:
        user_id = None
    where = Q(username=user) | Q(email=user)
    if user_id is not None:
        where = where | Q(id=user_id)
    return list(User.objects.filter(where).all())


@main.command()
@click.option('--user', required=True, help="user_id, username or email")
def search(user):
    user_s = _search_user(user)
    for user in user_s:
        customer = dict(
            external_id=user.id,
            nickname=user.username,
        )
        result = SHOPANT_SERVER.call('customer.get', params=dict(customer=customer))
        balance = result['balance']
        date_format = '%Y-%m-%dT%H:%M:%SZ'
        dt_balance = timezone.datetime.fromtimestamp(balance, tz=timezone.utc).strftime(date_format)
        date_joined = user.date_joined.strftime(date_format)
        last_login = user.last_login.strftime(date_format)
        message = (f'id={user.id} username={user.username} email={user.email} '
                   f'balance={dt_balance} date_joined={date_joined} last_login={last_login}')
        click.echo(message)


@main.command()
@click.option('--user', help="user_id, username or email")
@click.option('--userfile', type=click.File(), help="user_id, username or email")
@click.option('--code', required=True, help="redeem code value")
def redeem_code_exchange(user: str, userfile: io.FileIO, code: str):
    if not user and not userfile:
        raise click.MissingParameter('user or userfile required')
    user_s = []
    if user:
        user_s.append(user)
    if userfile:
        for line in userfile.read().splitlines():
            line = line.strip().split()[0]
            if line:
                user_s.append(line)
    for user in user_s:
        _redeem_code_exchange(user, code)


def _redeem_code_exchange(user: str, code: str):
    key = user
    user_s = _search_user(user)
    if not user_s:
        click.echo(f'{key:>30s}: not found')
        return
    if len(user_s) != 1:
        click.echo(f'{key:>30s}: found multiple')
        return
    user = user_s[0]
    customer = dict(
        external_id=user.id,
        nickname=user.username,
    )
    try:
        result = SHOPANT_SERVER.call('redeem_code.exchange', params=dict(
            customer=customer,
            value=code,
        ))
    except ShopAntServerError as ex:
        click.echo(f'{key:>30s}: id={user.id} {ex}')
        return
    amount = result['amount']
    message = f'{key:>30s}: id={user.id} amount={amount}'
    click.echo(message)


if __name__ == "__main__":
    main()
