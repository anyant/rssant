from mako.template import Template

from rssant_config import CONFIG

GOOGLE_SCRIPT = Template(
    r'''
(function () {
    var d = document, g = d.createElement('script'), s = d.getElementsByTagName('script')[0];
    g.type = 'text/javascript'; g.async = true; g.defer = true;
    g.src = 'https://www.googletagmanager.com/gtag/js?id=${tracking_id}';
    s.parentNode.insertBefore(g, s);
    window.dataLayer = window.dataLayer || [];
    function gtag(){dataLayer.push(arguments);}
    gtag('js', new Date());
    gtag('config', '${tracking_id}');
})();
'''.strip()
)


PLAUSIBLE_SCRIPT = Template(
    r'''
(function () {
    window.plausible = window.plausible || function() {
        (window.plausible.q = window.plausible.q || []).push(arguments) }
    var d = document, g = d.createElement('script'), s = d.getElementsByTagName('script')[0];
    g.type = 'text/javascript'; g.async = true; g.defer = true;
    g.setAttribute('data-domain', '${domain}');
    g.src = '${url}' + 'js/plausible.js'; s.parentNode.insertBefore(g, s);
})();
'''.strip()
)


BAIDU_TONGJI_SCRIPT = Template(
    r'''
(function() {
  window._hmt = window._hmt || [];
  var hm = document.createElement("script");
  hm.src = "https://hm.baidu.com/hm.js?${tracking_id}";
  hm.type = 'text/javascript'; hm.async = true; hm.defer = true;
  var s = document.getElementsByTagName("script")[0];
  s.parentNode.insertBefore(hm, s);
})();
'''.strip()
)

CLARITY_SCRIPT = Template(
    r'''
;(function(c,l,a,r,i,t,y){
    c[a]=c[a]||function(){(c[a].q=c[a].q||[]).push(arguments)};
    t=l.createElement(r);t.async=1;t.src="https://www.clarity.ms/tag/"+i;
    y=l.getElementsByTagName(r)[0];y.parentNode.insertBefore(t,y);
})(window,document,"clarity","script","${code}");
'''
)


class AnalyticsScript:

    def generate(self, request_domain=None):
        if CONFIG.analytics_google_enable:
            return self.generate_google(
                tracking_id=CONFIG.analytics_google_tracking_id,
            )
        if CONFIG.analytics_plausible_enable:
            return self.generate_plausible(
                url=CONFIG.analytics_plausible_url,
                domain=CONFIG.analytics_plausible_domain,
                request_domain=request_domain,
            )
        if CONFIG.analytics_baidu_tongji_enable:
            return self.generate_baidu_tongji(
                tracking_id=CONFIG.analytics_baidu_tongji_id,
            )
        if CONFIG.analytics_clarity_enable:
            return self.generate_clarity(
                code=CONFIG.analytics_clarity_code,
            )
        return None

    @staticmethod
    def generate_google(tracking_id):
        """
        >>> assert AnalyticsScript.generate_google('G-1234')
        """
        return GOOGLE_SCRIPT.render(tracking_id=tracking_id)

    @staticmethod
    def generate_plausible(url, domain, request_domain=None):
        """
        >>> assert AnalyticsScript.generate_plausible('https://p.anyant.com', 'rss.anyant.com')
        """
        url = url.rstrip('/') + '/'
        if request_domain:
            domain = request_domain
        return PLAUSIBLE_SCRIPT.render(url=url, domain=domain)

    @staticmethod
    def generate_baidu_tongji(tracking_id):
        """
        >>> assert AnalyticsScript.generate_baidu_tongji('123456')
        """
        return BAIDU_TONGJI_SCRIPT.render(tracking_id=tracking_id)

    @staticmethod
    def generate_clarity(code):
        """
        >>> assert AnalyticsScript.generate_clarity('123456')
        """
        return CLARITY_SCRIPT.render(code=code)
