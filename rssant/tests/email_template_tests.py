from rssant.email_template import EMAIL_CONFIRM_TEMPLATE
from rssant_config import CONFIG


def test_render_email_confirm():
    ctx = {
        "link": 'https://rss.anyant.com/',
        "username": 'guyskk@localhost.com',
        'rssant_url': CONFIG.root_url,
        'rssant_email': CONFIG.smtp_username,
    }
    html = EMAIL_CONFIRM_TEMPLATE.render_html(**ctx)
    for key, value in ctx.items():
        assert value in html, f'{key} {value!r} not rendered'
