import json
import gzip
import typing
import logging
import os.path
from itertools import groupby
from collections import namedtuple, defaultdict

import click
from django.utils import timezone
from django.db import connection
import rssant_common.django_setup  # noqa:F401
from rssant_common.helper import to_timezone_cst
from rssant.email_template import EmailTemplate
from rssant_config import CONFIG
from rssant_api.models import FeedStatus


LOG = logging.getLogger(__name__)


Feed = namedtuple('Feed', 'id,domain,status,use_proxy,response_status,freeze_level')


def query_feeds() -> typing.List[Feed]:
    sql = r'''
    SELECT
        id, status, use_proxy, response_status, freeze_level,
        SUBSTRING(reverse_url FROM '^([\w\.\-]+)\!') AS domain
    FROM rssant_api_feed
    '''
    with connection.cursor() as cursor:
        cursor.execute(sql)
        rows = list(cursor.fetchall())
    items = []
    for row in rows:
        id, status, use_proxy, response_status, freeze_level, domain = row
        if not domain:
            LOG.error(f'Feed#{id} domain is None, skip it!')
            continue
        domain = '.'.join(reversed(domain.split('.')))
        feed = Feed(
            id=id,
            domain=domain,
            status=status or FeedStatus.READY,
            use_proxy=bool(use_proxy),
            response_status=response_status or 0,
            freeze_level=freeze_level or 0,
        )
        items.append(feed)
    return items


def compute_group_stats(feeds: typing.List[Feed]) -> dict:
    keys = ['status', 'use_proxy', 'response_status', 'freeze_level']
    stats = {k: defaultdict(lambda: 0) for k in keys}
    stats['total'] = len(feeds)
    for feed in feeds:
        for key in keys:
            stats[key][str(getattr(feed, key))] += 1
    stats['use_proxy'] = stats['use_proxy']['True']
    return stats


def compute_snapshot(feeds: typing.List[Feed]) -> list:
    result_items = []
    def key(x): return x.domain
    for domain, group_feeds in groupby(sorted(feeds, key=key), key=key):
        group_stats = compute_group_stats(list(group_feeds))
        group_stats['domain'] = domain
        result_items.append(group_stats)
    return dict(items=result_items)


DomainRecord = namedtuple('DomainRecord', 'domain,total,snapshot1,snapshot2')


def _compute_domain_records(snapshot1, snapshot2):
    snapshot1 = {x['domain']: x for x in snapshot1['items']}
    snapshot2 = {x['domain']: x for x in snapshot2['items']}
    records = []
    empty_domain = {
        'total': 0,
        'use_proxy': 0,
        'status': {},
        'response_status': {},
        'freeze_level': {},
    }
    for domain in set(snapshot1.keys()) | set(snapshot2.keys()):
        d1 = snapshot1.get(domain, empty_domain)
        d2 = snapshot2.get(domain, empty_domain)
        total = max(d1['total'], d2['total'])
        records.append(DomainRecord(domain, total, d1, d2))
    records = list(sorted(records, key=lambda x: x.total, reverse=True))
    return records


def compute_record_values(domain_snapshot):
    result = defaultdict(lambda: 0)
    result['total'] = domain_snapshot['total']
    result['use_proxy'] = domain_snapshot['use_proxy']
    for status, count in domain_snapshot['response_status'].items():
        status = int(status)
        if status in (200, 304):
            key = 'ok'
        elif -299 <= status <= -200:
            key = 'neterr'
        elif status in (401, 403):
            key = 'deny'
        elif 400 <= status <= 499:
            key = '4xx'
        elif 500 <= status <= 599:
            key = '5xx'
        else:
            key = 'other'
        result['response_status:' + key] += count
    for level, count in domain_snapshot['freeze_level'].items():
        level = int(level)
        if level >= 168:
            key = '168+'
        elif level >= 12:
            key = '12-167'
        elif level >= 4:
            key = '4-11'
        else:
            key = '1-3'
        result['freeze_level:' + key] += count
    return result


def _compute_record_deltas(values1, values2):
    keys = set(values1.keys()) | set(values2.keys())
    deltas = {}
    for k in keys:
        deltas[k] = values2.get(k, 0) - values1.get(k, 0)
    return deltas


def _default_domain_report():
    return {
        'total': 0,
        'base': defaultdict(lambda: 0),
        'delta': defaultdict(lambda: 0),
    }


def compute_report(snapshot1, snapshot2):
    report_records = defaultdict(_default_domain_report)
    domain_records = _compute_domain_records(snapshot1, snapshot2)
    for index, dr in enumerate(domain_records):
        if index < 30:
            key = dr.domain
        else:
            if dr.total <= 3:
                key = f'other:{dr.total}'
            elif dr.total <= 9:
                key = f'other:4-9'
            elif dr.total <= 99:
                key = f'other:10-99'
            else:
                key = f'other:100+'
        for key in (key, 'ALL'):
            domain_report = report_records[key]
            domain_report['total'] += dr.total
            values1 = compute_record_values(dr.snapshot1)
            for k, v in values1.items():
                domain_report['base'][k] += v
            values2 = compute_record_values(dr.snapshot2)
            deltas = _compute_record_deltas(values1, values2)
            for k, v in deltas.items():
                domain_report['delta'][k] += v
    for domain, domain_report in report_records.items():
        domain_report['domain'] = domain

    def record_sort_key(x):
        if x['domain'] == 'ALL':
            return (3, 0, x['domain'])
        if x['domain'].startswith('other:'):
            count = int(x['domain'][len('other:'):].strip('+').split('-')[0])
            return (1, -count, x['domain'])
        else:
            return (2, x['total'], x['domain'])
    report_records = list(sorted(report_records.values(), key=record_sort_key, reverse=True))

    response_status_headers = ['ok', 'neterr', 'deny', '4xx', '5xx', 'other']
    freeze_level_headers = ['1-3', '4-11', '12-167', '168+', ]
    result = {
        'records': report_records,
        'headers': {
            'response_status': response_status_headers,
            'freeze_level': freeze_level_headers,
        }
    }
    return result


@click.group()
def main():
    """feed analysis command"""


@click.option('--output', type=str, help='output filepath')
@main.command()
def snapshot(output=None):
    feeds = query_feeds()
    result = compute_snapshot(feeds)
    text = json.dumps(result, ensure_ascii=False, indent=4)
    content = gzip.compress(text.encode('utf-8'))
    if output:
        output = os.path.abspath(os.path.expanduser(output))
        click.echo(f'save to {output}')
        with open(output, 'wb') as f:
            f.write(content)
    else:
        click.echo(content)


def load_snapshot(filepath):
    filepath = os.path.abspath(os.path.expanduser(filepath))
    with open(filepath, 'rb') as f:
        content = f.read()
    return json.loads(gzip.decompress(content).decode('utf-8'))


def _get_template():
    date = to_timezone_cst(timezone.now()).strftime('%Y-%m-%d')
    return EmailTemplate(
        subject=f'蚁阅订阅分析 {date}',
        filename='feed_analysis.html.mako',
    )


def render_html(report: dict) -> str:
    return _get_template().render_html(**report)


def render_and_send_email(report: dict, receiver: str):
    template = _get_template()
    sender = CONFIG.smtp_username
    return template.send(sender=sender, receiver=receiver, context=report)


@click.option('--snapshot1', type=str, help='snapshot1 filepath')
@click.option('--snapshot2', type=str, help='snapshot2 filepath')
@click.option('--output', type=str, required=True, help='output email or filepath')
@click.option('--output-type', type=click.Choice(['html', 'email']), default='html', help='output type')
@main.command()
def report(snapshot1, snapshot2, output: str, output_type: str):
    snapshot1 = load_snapshot(snapshot1)
    snapshot2 = load_snapshot(snapshot2)
    report = compute_report(snapshot1, snapshot2)
    if output_type == 'html':
        content = render_html(report)
        output = os.path.abspath(os.path.expanduser(output))
        click.echo(f'save to {output}')
        with open(output, 'w') as f:
            f.write(content)
    else:
        assert output_type == 'email', f'unknown output type {output_type}'
        click.echo(f'send to {output}')
        render_and_send_email(report, receiver=output)


if __name__ == "__main__":
    main()
