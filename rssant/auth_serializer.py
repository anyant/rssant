from urllib.parse import urljoin

from django.contrib.auth.forms import PasswordResetForm
from rest_auth.serializers import PasswordResetSerializer

from rssant_common.standby_domain import get_request_root_url

from .settings import DEFAULT_FROM_EMAIL
from .email_template import RESET_PASSWORD_TEMPLATE


class RssantPasswordResetForm(PasswordResetForm):

    def __init__(self, *args, **kwargs):
        self._request = None
        super().__init__(*args, **kwargs)

    def get_from_email(self):
        """
        This is a hook that can be overridden to programatically
        set the 'from' email address for sending emails
        """
        return DEFAULT_FROM_EMAIL

    def send_mail(self, subject_template_name, email_template_name,
                  context, from_email, to_email, html_email_template_name=None):
        link = 'reset-password/{}?token={}'.format(context['uid'], context['token'])
        root_url = get_request_root_url(self._request)
        link = urljoin(root_url, link)
        my_context = dict(rssant_url=root_url, email=to_email, link=link)
        RESET_PASSWORD_TEMPLATE.send(self.get_from_email(), to_email, my_context)

    def save(self, *args, **kwargs):
        self._request = kwargs.get('request')
        super().save(*args, **kwargs)


class RssantPasswordResetSerializer(PasswordResetSerializer):
    password_reset_form_class = RssantPasswordResetForm
