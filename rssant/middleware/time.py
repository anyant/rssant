import time
from django.db import connection


class TimeMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        t_begin = time.time()
        response = self.get_response(request)
        cost = (time.time() - t_begin) * 1000
        response['X-Time'] = f'{cost:.0f}ms'
        num_sqls = len(connection.queries)
        sql_cost = sum(float(x['time']) for x in connection.queries) * 1000
        sql_time = f'{num_sqls};{sql_cost:.0f}ms'
        response['X-SQL-Time'] = sql_time
        return response
