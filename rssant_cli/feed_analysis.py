import json
import typing
import logging
import os.path
import copy
from itertools import groupby, chain
from collections import namedtuple, defaultdict, OrderedDict

import click
from django.utils import timezone
from django.db import connection
import rssant_common.django_setup  # noqa:F401
from rssant_common.helper import to_timezone_cst
from rssant.email_template import EmailTemplate
from rssant_config import CONFIG
from rssant_api.models import FeedStatus
from rssant_feedlib.response import FeedResponseStatus


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
    stats['use_proxy'] = stats['use_proxy']['true']
    return stats


def domain_sort_key(domain: str):
    return (domain.startswith('other:'), domain)


def compute_snapshot(feeds: typing.List[Feed]) -> dict:
    groups = defaultdict(lambda: [])
    def key(x): return x.domain
    for domain, group_feeds in groupby(sorted(feeds, key=key), key=key):
        group_feeds = list(group_feeds)
        if len(group_feeds) <= 5:
            domain = 'other:{}'.format(len(group_feeds))
        groups[domain].extend(group_feeds)
    result = {}
    for domain, group_feeds in groups.items():
        group_stats = compute_group_stats(group_feeds)
        result[domain] = group_stats
    result = OrderedDict(sorted(result.items(), key=lambda x: domain_sort_key(x[0])))
    return result


def compute_report(snapshot1, snapshot2):
    empty_domain = {
        'total': 0,
        'use_proxy': 0,
        'status': {},
        'response_status': {},
        'freeze_level': {},
    }
    number_keys = ['total', 'use_proxy']
    count_keys = ['status', 'response_status', 'freeze_level']
    count_names = defaultdict(lambda: set())
    for stats in chain(snapshot1.values(), snapshot2.values()):
        for key in count_keys:
            count_names[key].update(stats[key].keys())
    result = OrderedDict()
    domains = sorted(set(snapshot1.keys()) | set(snapshot2.keys()), key=domain_sort_key)
    for domain in domains:
        stats_1 = copy.deepcopy(snapshot1.get(domain, empty_domain))
        stats_2 = snapshot2.get(domain, empty_domain)
        for key in number_keys:
            delta = stats_2[key] - stats_1[key]
            stats_1[f'{key}_diff'] = delta
        for key in count_keys:
            delta = {}
            for name in count_names[key]:
                v2 = stats_2[key].get(name, 0)
                v1 = stats_1[key].setdefault(name, 0)
                delta[name] = v2 - v1
            stats_1[f'{key}_diff'] = delta
        result[domain] = stats_1
    return result


@click.group()
def main():
    """feed analysis command"""


@click.option('--output', type=str, help='output filepath')
@main.command()
def snapshot(output=None):
    feeds = query_feeds()
    result = compute_snapshot(feeds)
    content = json.dumps(result, ensure_ascii=False, indent=4)
    if output:
        output = os.path.abspath(os.path.expanduser(output))
        click.echo(f'save to {output}')
        with open(output, 'w') as f:
            f.write(content)
    else:
        click.echo(content)


def load_snapshot(filepath):
    filepath = os.path.abspath(os.path.expanduser(filepath))
    with open(filepath) as f:
        return json.load(f)


def _to_template_context(result: dict) -> dict:
    stats = next(iter(result.values()))
    response_status_names = list(sorted(stats['response_status'].keys(), key=int))
    response_status_labels = [FeedResponseStatus.name_of(int(x)) for x in response_status_names]
    freeze_level_names = list(sorted(stats['freeze_level'].keys(), key=int))
    context = {
        'result': list(result.items()),
        'status_names': list(sorted(stats['status'].keys())),
        'response_status_names': response_status_names,
        'response_status_labels': response_status_labels,
        'freeze_level_names': freeze_level_names,
    }
    return context


def render_html(result: dict) -> str:
    template = EmailTemplate(filename='feed_analysis.html.mako')
    return template.render_html(**_to_template_context(result))


def render_and_send_email(result: dict, receiver: str):
    date = to_timezone_cst(timezone.now()).strftime('%Y-%m-%d')
    template = EmailTemplate(
        subject=f'蚁阅订阅分析 {date}',
        filename='feed_analysis.html.mako',
    )
    context = _to_template_context(result)
    sender = CONFIG.smtp_username
    return template.send(sender=sender, receiver=receiver, context=context)


@click.option('--snapshot1', type=str, help='snapshot1 filepath')
@click.option('--snapshot2', type=str, help='snapshot2 filepath')
@click.option('--output', type=str, required=True, help='output email or filepath')
@click.option('--output-type', type=click.Choice(['html', 'email']), default='html', help='output type')
@main.command()
def report(snapshot1, snapshot2, output: str, output_type: str):
    snapshot1 = load_snapshot(snapshot1)
    snapshot2 = load_snapshot(snapshot2)
    result = compute_report(snapshot1, snapshot2)
    if output_type == 'html':
        content = render_html(result)
        output = os.path.abspath(os.path.expanduser(output))
        click.echo(f'save to {output}')
        with open(output, 'w') as f:
            f.write(content)
    else:
        assert output_type == 'email', f'unknown output type {output_type}'
        click.echo(f'send to {output}')
        render_and_send_email(result, receiver=output)


if __name__ == "__main__":
    main()
