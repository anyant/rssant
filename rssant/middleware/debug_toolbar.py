import logging
from debug_toolbar.toolbar import DebugToolbar
from debug_toolbar.middleware import DebugToolbarMiddleware


LOG = logging.getLogger(__name__)


def ms(t):
    return '%dms' % int(t) if t is not None else '#ms'


def s_ms(t):
    return ms(t * 1000) if t is not None else '#ms'


class RssantDebugToolbarMiddleware(DebugToolbarMiddleware):
    """
    Middleware to set up Debug Toolbar on incoming request and render toolbar
    on outgoing response.

    See also:
    https://github.com/jazzband/django-debug-toolbar/blob/master/debug_toolbar/middleware.py
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        toolbar = DebugToolbar(request, self.get_response)

        # Activate instrumentation ie. monkey-patch.
        for panel in toolbar.enabled_panels:
            panel.enable_instrumentation()
        try:
            # Run panels like Django middleware.
            response = toolbar.process_request(request)
        finally:
            # Deactivate instrumentation ie. monkey-unpatch. This must run
            # regardless of the response. Keep 'return' clauses below.
            for panel in reversed(toolbar.enabled_panels):
                panel.disable_instrumentation()

        # generate stats and timing
        for panel in reversed(toolbar.enabled_panels):
            panel.generate_stats(request, response)
            panel.generate_server_timing(request, response)
        stats = self._extract_panel_stats(toolbar.enabled_panels)
        message = self._stats_message(stats)
        LOG.info(f'X-Time-Debug: {message}')
        response['X-Time-Debug'] = message
        response['X-Time'] = ms(stats['timer']['total_time'])
        return response

    def _stats_message(self, stats):
        timer_msg = '0ms'
        total_time = int(stats['timer']['total_time'] or 0)
        if total_time > 0:
            timer_msg = '{},utime={},stime={}'.format(
                ms(total_time),
                ms(stats['timer']['utime']),
                ms(stats['timer']['stime']),
            )

        sql_msg = 'sql=0'
        if stats['sql']:
            sql_msg = 'sql={},{}'.format(
                stats['sql']['num_queries'] or 0,
                ms(stats['sql']['time_spent']),
            )
            similar_count = stats['sql']['similar_count']
            if similar_count and similar_count > 0:
                sql_msg += f',similar={similar_count}'
            duplicate_count = stats['sql']['duplicate_count']
            if duplicate_count and duplicate_count > 0:
                sql_msg += f',duplicate={duplicate_count}'

        seaweed_msg = 'seaweed=0'
        if stats['seaweed']:
            seaweed_items = []
            for op in ['get', 'put', 'delete']:
                count = stats['seaweed'].get(op)
                if count and count > 0:
                    seaweed_items.append('{}:{}:{}'.format(
                        op, count, s_ms(stats['seaweed'].get(f'{op}_time'))))
            if seaweed_items:
                seaweed_msg = 'seaweed=' + ','.join(seaweed_items)

        return ';'.join([timer_msg, sql_msg, seaweed_msg])

    def _extract_panel_stats(self, panels):
        stats_map = {}
        for panel in panels:
            stats = panel.get_stats()
            if not stats:
                continue
            stats_map[panel.__class__.__name__] = stats
        result = {'sql': {}, 'timer': {}, 'seaweed': {}}
        sql_panel_stats = stats_map.get('SQLPanel')
        if sql_panel_stats and sql_panel_stats['databases']:
            _, sql_stats = sql_panel_stats['databases'][0]
            keys = ['time_spent', 'num_queries', 'similar_count', 'duplicate_count']
            for key in keys:
                result['sql'][key] = sql_stats.get(key)
        timer_stats = stats_map.get('TimerPanel')
        if timer_stats:
            keys = ['total_time', 'utime', 'stime', 'total']
            for key in keys:
                result['timer'][key] = timer_stats.get(key)
        seaweed_stats = stats_map.get('SeaweedPanel')
        if seaweed_stats:
            result['seaweed'].update(seaweed_stats)
        return result
