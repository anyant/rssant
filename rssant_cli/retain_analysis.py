import typing
import os.path
from itertools import groupby
from collections import namedtuple

import click
from django.utils import timezone
from django.db import connection
import rssant_common.django_setup  # noqa:F401
from rssant_common.helper import to_timezone_cst
from rssant.email_template import EmailTemplate
from rssant_config import CONFIG


User = namedtuple('User', 'id,username,email,dt_joined,feed_count,dt_last_visit')


def query_users(dt_gte: timezone.datetime = None, dt_lt: timezone.datetime = None) -> typing.List[User]:
    sql_template = '''
    SELECT id, username, email, date_joined,
        COALESCE(feed_count, 0) AS feed_count,
        COALESCE(dt_last_visit, last_login) AS dt_last_visit
    FROM auth_user LEFT OUTER JOIN (
        SELECT user_id, count(1) AS feed_count, MAX(dt_updated) AS dt_last_visit
        FROM rssant_api_userfeed GROUP BY user_id
    ) t1 ON auth_user.id=t1.user_id
    {where} ORDER BY id;
    '''
    params = []
    where = []
    if dt_gte:
        where.append('date_joined >= %s')
        params.append(dt_gte)
    if dt_lt:
        where.append('date_joined < %s')
        params.append(dt_lt)
    if where:
        where = 'WHERE ' + ' AND '.join(where)
    else:
        where = ''
    sql = sql_template.format(where=where)
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        rows = list(cursor.fetchall())
    items = []
    for row in rows:
        id, username, email, dt_joined, feed_count, dt_last_visit = row
        if not dt_last_visit:
            dt_last_visit = dt_joined
        user = User(
            id=id,
            username=username,
            email=email,
            dt_joined=dt_joined,
            feed_count=feed_count,
            dt_last_visit=dt_last_visit,
        )
        items.append(user)
    return items


def filter_by_feed_count(users: typing.List[User], gte: int = None, lt: int = None) -> typing.List[User]:
    if gte is not None:
        users = [x for x in users if x.feed_count >= gte]
    if lt is not None:
        users = [x for x in users if x.feed_count < lt]
    return users


def compute_retain(users: typing.List[User], periods: typing.List[int]) -> list:
    counts = {k: 0 for k in periods}
    for user in users:
        days = (user.dt_last_visit - user.dt_joined).days
        for n in periods:
            if days >= n:
                counts[n] += 1
    divisor = max(1, len(users))
    ratios = {k: v / divisor for k, v in counts.items()}
    return [v for k, v in sorted(ratios.items())]


def analysis(users: typing.List[User], periods: typing.List[int], by='day') -> list:
    if by == 'day':
        def key(x):
            dt = to_timezone_cst(x.dt_joined)
            return dt.strftime('%Y-%m-%d')
    elif by == 'month':
        def key(x):
            dt = to_timezone_cst(x.dt_joined)
            return dt.strftime('%Y-%m')
    elif by == 'week':
        def key(x):
            dt = to_timezone_cst(x.dt_joined)
            dt_week = dt - timezone.timedelta(days=dt.weekday())
            return dt_week.strftime('%Y-%m-%d')
    else:
        raise ValueError(f'not support analysis by {by}')
    rows = []
    for dt_joined, group_users in groupby(sorted(users, key=key), key=key):
        group_users = list(group_users)
        activated_users = filter_by_feed_count(group_users, gte=2)
        ratios = compute_retain(group_users, periods=periods)
        activated_ratios = compute_retain(activated_users, periods=periods)
        rows.append((
            dt_joined,
            len(group_users), ratios,
            len(activated_users), activated_ratios,
        ))
    return list(reversed(rows))


def render_html(rows: list, periods: typing.List[int]) -> str:
    template = EmailTemplate(filename='retain_analysis.html')
    context = dict(rows=rows, periods=periods)
    return template.render_html(**context)


def render_and_send_email(rows: list, periods: typing.List[int], dt_end: timezone.datetime, receiver: str):
    date = to_timezone_cst(dt_end).strftime('%Y-%m-%d')
    template = EmailTemplate(
        subject=f'蚁阅用户留存 {date}',
        filename='retain_analysis.html',
    )
    context = dict(rows=rows, periods=periods)
    sender = CONFIG.smtp_username
    return template.send(sender=sender, receiver=receiver, context=context)


def render_csv(rows: list, periods: typing.List[int]) -> str:
    periods_header = ','.join(map(str, periods))
    header = f'date,joined,{periods_header},activated,{periods_header}'
    lines = [header + '\n']
    for dt, total, ratios, activated, activated_ratios in rows:
        ratios = ['{:.3f}'.format(x) for x in ratios]
        activated_ratios = ['{:.3f}'.format(x) for x in activated_ratios]
        values = [dt, total] + ratios + [activated] + activated_ratios
        line = ','.join(map(str, values)) + '\n'
        lines.append(line)
    return ''.join(lines)


def format_utc_date(dt):
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')


DAILY_RETAIN_PERIODS = [1, 2, 3, 5, 7, 14]
MONTHLY_RETAIN_PERIODS = [1, 2, 3, 5, 7, 14, 30, 60, 90]


@click.option('--dt-end', type=str, required=False, help='end date joined')
@click.option('--days', type=int, default=30, help='analysis days')
@click.option('--by', type=click.Choice(['day', 'week', 'month']), default='day', help='group user by')
@click.option('--output', type=str, required=True, help='output email or filepath')
@click.option('--output-type', type=click.Choice(['html', 'csv', 'email']), default='html')
@click.command()
def main(output: str, output_type: str, days: int, dt_end=None, by=None):
    if dt_end:
        dt_end = timezone.datetime.strptime(dt_end, '%Y-%m-%d')
    else:
        dt_end = timezone.datetime.now()
    dt_lt = to_timezone_cst(dt_end)\
        .replace(hour=0, minute=0, second=0, microsecond=0)\
        .astimezone(timezone.utc)
    dt_gte = dt_lt - timezone.timedelta(days=days)
    periods = DAILY_RETAIN_PERIODS if days <= 30 else MONTHLY_RETAIN_PERIODS
    users = query_users(dt_gte=dt_gte, dt_lt=dt_lt)
    time_range = f'gte={format_utc_date(dt_gte)} lt={format_utc_date(dt_lt)}'
    click.echo(f'query users {time_range} total={len(users)}')
    rows = analysis(users, periods=periods, by=by)
    if output_type in ('csv', 'html'):
        render = {'csv': render_csv, 'html': render_html}[output_type]
        content = render(rows, periods=periods)
        output = os.path.abspath(os.path.expanduser(output))
        click.echo(f'save to {output}')
        with open(output, 'w') as f:
            f.write(content)
    else:
        assert output_type == 'email', f'unknown output type {output_type}'
        click.echo(f'send to {output}')
        render_and_send_email(rows, periods=periods, dt_end=dt_end, receiver=output)


if __name__ == "__main__":
    main()
