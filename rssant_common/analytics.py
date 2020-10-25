from mako.template import Template
from rssant_config import CONFIG


MATOMO_SCRIPT = Template(r'''
var _paq = window._paq = window._paq || [];
_paq.push(['trackPageView']);
_paq.push(['enableLinkTracking']);
(function () {
    var u = "${url}";
    _paq.push(['setTrackerUrl', u + 'matomo.php']);
    _paq.push(['setSiteId', '${site_id}']);
    var d = document, g = d.createElement('script'), s = d.getElementsByTagName('script')[0];
    g.type = 'text/javascript'; g.async = true; g.src = u + 'matomo.js'; s.parentNode.insertBefore(g, s);
})();
'''.strip())


GOOGLE_SCRIPT = Template(r'''
(function(i,s,o,g,r,a,m){i['GoogleAnalyticsObject']=r;i[r]=i[r]||function(){
(i[r].q=i[r].q||[]).push(arguments)},i[r].l=1*new Date();a=s.createElement(o),
m=s.getElementsByTagName(o)[0];a.async=1;a.src=g;m.parentNode.insertBefore(a,m)
})(window,document,'script','https://www.google-analytics.com/analytics.js','ga');
ga('create', '${tracking_id}', 'auto');
ga('send', 'pageview');
'''.strip())


PLAUSIBLE_SCRIPT = Template(r'''
(function () {
    var d = document, g = d.createElement('script'), s = d.getElementsByTagName('script')[0];
    g.type = 'text/javascript'; g.async = true; g.defer = true;
    g.setAttribute('data-domain', '${domain}');
    g.src = '${url}' + 'js/plausible.js'; s.parentNode.insertBefore(g, s);
})();
''')


class AnalyticsScript:

    def generate(self):
        if CONFIG.analytics_matomo_enable:
            return self.generate_matomo(
                url=CONFIG.analytics_matomo_url,
                site_id=CONFIG.analytics_matomo_site_id,
            )
        if CONFIG.analytics_google_enable:
            return self.generate_google(
                tracking_id=CONFIG.analytics_google_tracking_id,
            )
        if CONFIG.analytics_plausible_enable:
            return self.generate_plausible(
                url=CONFIG.analytics_plausible_url,
                domain=CONFIG.analytics_plausible_domain,
            )
        return None

    @staticmethod
    def generate_matomo(url, site_id):
        """
        >>> assert AnalyticsScript.generate_matomo('//g.matomo.com/', '1')
        """
        url = url.rstrip('/') + '/'
        return MATOMO_SCRIPT.render(url=url, site_id=site_id)

    @staticmethod
    def generate_google(tracking_id):
        """
        >>> assert AnalyticsScript.generate_google('UA-1234')
        """
        return GOOGLE_SCRIPT.render(tracking_id=tracking_id)

    @staticmethod
    def generate_plausible(url, domain):
        """
        >>> assert AnalyticsScript.generate_plausible('https://p.anyant.com', 'rss.anyant.com')
        """
        url = url.rstrip('/') + '/'
        return PLAUSIBLE_SCRIPT.render(url=url, domain=domain)
