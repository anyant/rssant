"""RSSHub 存量错误源唤醒（人工执行，跑完退出）

把「error + 有订阅者」的 RSSHub 源分批置 dt_checked = NULL，调度器下一轮就会重新检查。
批大小按可用实例数动态计算（可用实例数 × 20），没有可用实例时等待，避免把实例打到冷却。
每批打印领取数和剩余数，跑完打印总数。

用法：用目标环境的 RSSANT_* 环境变量执行

    python scripts/rsshub_wakeup.py
"""
import os
import sys
import time

import click

# 支持直接 python scripts/rsshub_wakeup.py 运行
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 每个可用实例每批放多少条
BATCH_PER_INSTANCE = 20
# 批间隔，给 worker 处理时间
BATCH_INTERVAL_SECONDS = 30
# 没有可用实例时的轮询间隔
WAIT_SECONDS = 10


def cooling_instances() -> int:
    from rssant_api.models import RsshubInstance

    return len(RsshubInstance.cooling_urls())


def available_instances() -> int:
    from rssant_feedlib.rsshub import get_instances

    return max(0, len(get_instances()) - cooling_instances())


@click.command()
def main():
    import rssant_common.django_setup  # noqa:F401
    from rssant_api.models import Feed

    total = 0
    while True:
        available = available_instances()
        if available <= 0:
            click.echo(
                '没有可用实例（冷却中 {} 个），等待 {} 秒'.format(
                    cooling_instances(), WAIT_SECONDS
                )
            )
            time.sleep(WAIT_SECONDS)
            continue
        limit = available * BATCH_PER_INSTANCE
        feed_ids, remaining = Feed.wakeup_rsshub_feeds(limit=limit)
        if not feed_ids:
            break
        total += len(feed_ids)
        click.echo(
            '唤醒 {} 条（可用实例 {}，剩余 {}，累计 {}）'.format(
                len(feed_ids), available, remaining, total
            )
        )
        time.sleep(BATCH_INTERVAL_SECONDS)
    click.echo('唤醒完成，共 {} 条'.format(total))


if __name__ == '__main__':
    main()
