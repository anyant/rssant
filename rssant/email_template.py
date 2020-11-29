import logging
import os.path
from smtplib import SMTPException

from django.template import Template, Context
from django.core.mail import send_mail
from mako.template import Template as MakoTemplate

import pynliner
from html2text import html2text

from rssant.settings import BASE_DIR, ENV_CONFIG

LOG = logging.getLogger(__name__)


class EmailTemplate:
    def __init__(self, filename, subject=None):
        filepath = os.path.join(BASE_DIR, 'rssant/templates/email', filename)
        with open(filepath) as f:
            html = f.read()
        if filename.endswith('.mako'):
            self.is_mako = True
            self.html_template = MakoTemplate(html)
        else:
            self.is_mako = False
            self.html_template = Template(html)
        self.subject = subject or ''

    def render_html(self, **kwargs) -> str:
        if self.is_mako:
            html = self.html_template.render(**kwargs)
        else:
            html = self.html_template.render(Context(kwargs))
        html = pynliner.fromString(html)
        return html

    def send(self, sender, receiver, context):
        LOG.info(f'send email subject={self.subject!r} to {receiver}')
        context.update(
            rssant_url=ENV_CONFIG.root_url,
            rssant_email=ENV_CONFIG.smtp_username,
        )
        html = self.render_html(**context)
        text = html2text(html)
        try:
            send_mail(self.subject, text, sender, [receiver],
                      fail_silently=False, html_message=html)
        except SMTPException as ex:
            LOG.exception(f"failed to send email  subject={self.subject!r} to {receiver}: {ex}")


EMAIL_CONFIRM_TEMPLATE = EmailTemplate(
    subject='[蚁阅] 请验证您的邮箱',
    filename='confirm.html',
)

RESET_PASSWORD_TEMPLATE = EmailTemplate(
    subject='[蚁阅] 重置密码',
    filename='reset_password.html',
)
