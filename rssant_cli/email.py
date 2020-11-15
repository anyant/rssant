import click
import rssant_common.django_setup  # noqa:F401
from rssant.email_template import EmailTemplate


@click.group()
def main():
    """Email Commands"""


@main.command()
@click.option('--receivers', required=True, help="receiver emails")
def send_recall(receivers):
    subject = '好久不见，甚是想念，欢迎回蚁阅看看~'
    sender = 'guyskk@anyant.com'
    email = EmailTemplate(subject, filename='recall.html')
    receivers = [x.strip() for x in receivers.splitlines() if x.strip()]
    click.confirm('Send recall email to {} receivers?'.format(len(receivers)), abort=True)
    for receiver in receivers:
        click.echo('> {}'.format(receiver))
        email.send(sender, receiver, context=dict(username=receiver))


if __name__ == "__main__":
    main()
