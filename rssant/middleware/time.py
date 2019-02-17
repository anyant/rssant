import time


class TimeMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        t_begin = time.time()
        response = self.get_response(request)
        cost = time.time() - t_begin
        response['X-Time'] = f'{cost * 1000:.0f}ms'
        return response
