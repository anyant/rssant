from urllib.parse import urljoin

from django.contrib.auth.forms import PasswordResetForm
from rest_auth.serializers import PasswordResetSerializer

from .email_template import RESET_PASSWORD_TEMPLATE
from rssant.settings import DEFAULT_FROM_EMAIL, ENV_CONFIG


class RssantPasswordResetForm(PasswordResetForm):

    def get_from_email(self):
        """
        This is a hook that can be overridden to programatically
        set the 'from' email address for sending emails
        """
        return DEFAULT_FROM_EMAIL

    def send_mail(self, subject_template_name, email_template_name,
                  context, from_email, to_email, html_email_template_name=None):
        link = 'reset-password/{}?token={}'.format(context['uid'], context['token'])
        link = urljoin(ENV_CONFIG.root_url, link)
        my_context = dict(email=to_email, link=link)
        RESET_PASSWORD_TEMPLATE.send(self.get_from_email(), to_email, my_context)


class RssantPasswordResetSerializer(PasswordResetSerializer):
    password_reset_form_class = RssantPasswordResetForm
