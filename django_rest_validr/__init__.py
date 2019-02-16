import inspect
import itertools
from validr import T, Compiler
from django.urls import path
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework.views import APIView


class RestRouter:
    def __init__(self, name=None):
        self.name = name
        self._schema_compiler = Compiler()
        self._routes = []

    @property
    def urls(self):
        def key_func(r):
            f, url, methods, params, returns = r
            return url
        urls = []
        routes = sorted(self._routes, key=key_func)
        groups = itertools.groupby(routes, key=key_func)
        for url, group in groups:
            view = self._make_view(list(group))
            urls.append(path(url, view))
        return urls

    @staticmethod
    def _make_method(method, f, params, returns):
        def rest_method(self, request, format=None, **kwargs):
            if params is not None:
                if request.method in ['GET', 'DELETE']:
                    kwargs.update(request.query_params)
                else:
                    kwargs.update(request.data)
                kwargs = params(kwargs)
            ret = f(request, **kwargs)
            if returns is not None:
                if not isinstance(ret, Response):
                    ret = Response(returns(ret))
            elif ret is None:
                ret = Response()
            return ret
        rest_method.__name__ = method.lower()
        rest_method.__qualname__ = method.lower()
        rest_method.__doc__ = f.__doc__
        return rest_method

    def _make_view(self, group):
        method_maps = {}
        for f, url, methods, params, returns in group:
            for method in methods:
                if method in method_maps:
                    raise ValueError(f'duplicated method {method} of {url}')
                m = self._make_method(method, f, params, returns)
                method_maps[method] = m

        class RestApiView(APIView):
            if 'GET' in method_maps:
                get = method_maps['GET']
            if 'POST' in method_maps:
                post = method_maps['POST']
            if 'PUT' in method_maps:
                put = method_maps['PUT']
            if 'DELETE' in method_maps:
                delete = method_maps['DELETE']
            if 'PATCH' in method_maps:
                patch = method_maps['PATCH']

        return RestApiView.as_view()

    def _route(self, url, methods):
        if isinstance(methods, str):
            methods = set(methods.strip().replace(',', ' ').split())
        else:
            methods = set(methods)
        methods = set(x.upper() for x in methods)

        def wrapper(f):
            params = _get_params(f)
            if params is not None:
                params = self._schema_compiler.compile(params)
            returns = _get_returns(f)
            if returns is not None:
                returns = self._schema_compiler.compile(returns)
            self._routes.append((f, url, methods, params, returns))
            return f

        return wrapper

    def get(self, url=''):
        return self._route(url, methods='GET')

    def post(self, url=''):
        return self._route(url, methods='POST')

    def put(self, url=''):
        return self._route(url, methods='PUT')

    def delete(self, url=''):
        return self._route(url, methods='DELETE')

    def patch(self, url=''):
        return self._route(url, methods='PATCH')

    def route(self, url='', methods='GET'):
        return self._route(url, methods=methods)

    __call__ = route


def _get_params(f):
    sig = inspect.signature(f)
    params_schema = {}
    for name, p in list(sig.parameters.items())[1:]:
        if p.default is not inspect.Parameter.empty:
            raise ValueError('You should set default in schema annotation!')
        if p.annotation is inspect.Parameter.empty:
            raise ValueError(f'Missing annotation in parameter {name}!')
        params_schema[name] = p.annotation
    if params_schema:
        return T.dict(params_schema).__schema__
    return None


def _get_returns(f):
    sig = inspect.signature(f)
    if sig.return_annotation is not inspect.Signature.empty:
        schema = sig.return_annotation
        return T(schema).__schema__
    return None
