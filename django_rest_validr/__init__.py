import inspect
import itertools
from collections import ChainMap

import coreapi
import coreschema
from validr import T, Compiler, Invalid
from django.urls import path
from django.http import HttpResponse
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.schemas import AutoSchema

from .validator import Cursor, VALIDATORS, pagination


__all__ = (
    'pagination',
    'Cursor',
    'RestRouter',
)


def coreschema_from_validr(item):
    mapping = {
        'int': coreschema.Integer,
        'str': coreschema.String,
        'float': coreschema.Number,
        'bool': coreschema.Boolean,
        'date': coreschema.String,
        'time': coreschema.String,
        'datetime': coreschema.String,
        'email': coreschema.String,
        'ipv4': coreschema.String,
        'ipv6': coreschema.String,
        'url': coreschema.String,
        'uuid': coreschema.String,
        'phone': coreschema.String,
        'idcard': coreschema.String,
        'list': coreschema.Array,
        'dict': coreschema.Object,
    }
    default = item.params.get('default')
    description = item.params.get('desc')
    schema_cls = mapping.get(item.validator, coreschema.String)
    return schema_cls(default=default, description=description)


class RestViewSchema(AutoSchema):
    """
    Overrides `get_link()` to provide Custom Behavior X
    """

    def __init__(self, method_meta):
        super(AutoSchema, self).__init__()
        self._method_meta = method_meta

    def get_manual_fields(self, path, method):
        f, url, params, returns = self._method_meta[method]
        if params is None:
            return []
        field_schemas = T(params).__schema__.items
        path_fields = self.get_path_fields(path, method)
        path_field_names = set(x.name for x in path_fields)
        fields = []
        for name, item in field_schemas.items():
            if name in path_field_names or name in ['id', 'pk']:
                continue
            required = not item.params.get('optional', False)
            default = item.params.get('default')
            if not (default is None or default == ''):
                required = False
            if method in ['GET', 'DELETE']:
                location = 'query'
            else:
                location = 'form'
            field = coreapi.Field(
                name=name,
                required=required,
                location=location,
                schema=coreschema_from_validr(item)
            )
            fields.append(field)
        return fields


class RestRouter:
    def __init__(self, name=None):
        self.name = name
        self._schema_compiler = Compiler(validators=VALIDATORS)
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
                maps = [kwargs]
                if request.method in ['GET', 'DELETE']:
                    maps.append(request.query_params)
                else:
                    maps.append(request.data)
                try:
                    kwargs = params(ChainMap(*maps))
                except Invalid as ex:
                    return Response({'message': str(ex)}, status=400)
            ret = f(request, **kwargs)
            if returns is not None:
                if not isinstance(ret, (Response, HttpResponse)):
                    ret = Response(returns(ret))
            elif ret is None:
                ret = Response(status=204)
            return ret
        rest_method.__name__ = method.lower()
        rest_method.__qualname__ = method.lower()
        rest_method.__doc__ = f.__doc__
        return rest_method

    def _make_view(self, group):
        method_maps = {}
        method_meta = {}
        for f, url, methods, params, returns in group:
            for method in methods:
                if method in method_maps:
                    raise ValueError(f'duplicated method {method} of {url}')
                m = self._make_method(method, f, params, returns)
                method_maps[method] = m
                method_meta[method] = f, url, params, returns

        class RestApiView(APIView):
            schema = RestViewSchema(method_meta)
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
