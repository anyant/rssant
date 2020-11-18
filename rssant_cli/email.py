import time
import os.path

from validr import Compiler, T
import click
import rssant_common.django_setup  # noqa:F401
from rssant.email_template import EmailTemplate
from rssant_config import CONFIG


@click.group()
def main():
    """Email Commands"""


def _parse_receivers(text) -> list:
    return [x.strip() for x in text.splitlines() if x.strip()]


@main.command()
@click.option('--receivers', required=True, help="receiver emails")
def send_recall(receivers):
    subject = '好久不见，甚是想念，欢迎回蚁阅看看~'
    sender = CONFIG.smtp_username
    email = EmailTemplate(subject, filename='recall.html')
    receivers = _parse_receivers(receivers)
    click.confirm('Send recall email to {} receivers?'.format(len(receivers)), abort=True)
    for receiver in receivers:
        click.echo('> {}'.format(receiver))
        email.send(sender, receiver, context=dict(username=receiver))


@main.command()
@click.option('--receiver-file', required=True, help="file contains receivers")
@click.option('--duration', required=True, help="total duration, eg: 5d")
def send_recall_timed(receiver_file, duration):
    receiver_file = os.path.abspath(os.path.expanduser(receiver_file))
    done_file = receiver_file + '.done'
    with open(receiver_file) as f:
        receivers = _parse_receivers(f.read())
    try:
        with open(done_file) as f:
            done_receivers = set(_parse_receivers(f.read()))
    except FileNotFoundError:
        done_receivers = set()
    todo_receivers = [x for x in receivers if x not in done_receivers]

    parse_duration = Compiler().compile(T.timedelta.min(0))
    duration = parse_duration(duration)

    status_msg = 'total={} done={} todo={}'.format(
        len(receivers), len(done_receivers), len(todo_receivers))
    click.confirm('Send recall email to {} receivers?'.format(status_msg), abort=True)
    if not todo_receivers:
        return

    subject = '好久不见，甚是想念，欢迎回蚁阅看看~'
    sender = CONFIG.smtp_username
    email = EmailTemplate(subject, filename='recall.html')

    sleep_time = duration / len(todo_receivers)
    for receiver in todo_receivers:
        click.echo('> {}'.format(receiver))
        email.send(sender, receiver, context=dict(username=receiver))
        with open(done_file, 'a+') as f:
            f.write(receiver + '\n')
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()
