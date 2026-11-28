"""无订阅坏源清理（人工执行，跑完退出）。

把「error/pending + 90 天没成功 + 没有订阅者 + 不在发现页」的源归档为 discard，
并恢复仍有订阅者的 discard 源（历史遗留的 29 条 + 清理期间并发订阅的兜底）。

归档只改 status/warnings，不删行；用户重新订阅、OPML 导入、发现页添加时会自动复活。

用法：用目标环境的 RSSANT_* 环境变量执行（需要 API 角色，Django 才配置了 DATABASES）：

    python scripts/clean_orphan_feeds.py --dry-run
    python scripts/clean_orphan_feeds.py --max-batches 3
    python scripts/clean_orphan_feeds.py
"""
import os
import sys
import time

import click

# 支持直接 python scripts/clean_orphan_feeds.py 运行
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

OLDER_THAN_DAYS = 90
BATCH_SIZE = 500
BATCH_SLEEP_SECONDS = 0.2


@click.command()
@click.option('--dry-run', is_flag=True, help='只统计，不写库')
@click.option(
    '--batch-size',
    default=BATCH_SIZE,
    show_default=True,
    type=click.IntRange(min=1),
    help='每批归档多少条',
)
@click.option(
    '--max-batches',
    default=0,
    show_default=True,
    type=click.IntRange(min=0),
    help='最多跑几批，0 表示不限',
)
def main(dry_run: bool, batch_size: int, max_batches: int):
    import rssant_common.django_setup  # noqa:F401
    from rssant_api.models import Feed

    if dry_run:
        counts = Feed.orphan_feed_counts(OLDER_THAN_DAYS)
        total = sum(counts.values())
        detail = ', '.join(
            '{}={}'.format(status, count)
            for status, count in sorted(counts.items())
        )
        click.echo(
            '待归档 {} 条（{}，条件：无订阅、{} 天没成功、不在发现页）'.format(
                total, detail or '无', OLDER_THAN_DAYS
            )
        )
        click.echo(
            '待恢复 discard 有订阅 {} 条'.format(
                Feed.count_discard_with_subscribers()
            )
        )
        return

    total = 0
    batches = 0
    while True:
        num = Feed.archive_orphan_feeds(
            limit=batch_size, older_than_days=OLDER_THAN_DAYS
        )
        if not num:
            break
        total += num
        batches += 1
        click.echo('归档 {} 条（累计 {}）'.format(num, total))
        if max_batches and batches >= max_batches:
            click.echo('达到 max-batches={}，可能还有剩余'.format(max_batches))
            break
        time.sleep(BATCH_SLEEP_SECONDS)
    click.echo('归档完成，共 {} 条，{} 批'.format(total, batches))
    num_revived = Feed.revive_discard_with_subscribers()
    click.echo('恢复 discard 有订阅 {} 条'.format(num_revived))


if __name__ == '__main__':
    main()
