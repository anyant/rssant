import json
import inspect
from collections import ChainMap

from validr import Compiler, Invalid
from aiohttp.web import RouteTableDef, StreamResponse, Response, json_response

from rssant_common.validator import VALIDATORS
from rssant_common.signature import get_params, get_returns


class ValidrRouteTableDef(RouteTableDef):

    def __init__(self):
        super().__init__()
        self._schema_compiler = Compiler(validators=VALIDATORS)

    @staticmethod
    def _response_from_invalid(ex):
        return json_response({
            'description': str(ex),
            'position': ex.position,
            'message': ex.message,
            'field': ex.field,
            'value': ex.value,
        }, status=400)

    def decorate(self, f):
        assert inspect.iscoroutinefunction(f), f'{f} is not coroutine function'
        params = get_params(f)
        if params is not None:
            params = self._schema_compiler.compile(params)
        returns = get_returns(f)
        if returns is not None:
            returns = self._schema_compiler.compile(returns)

        async def wrapped(request, **kwargs):
            ret = None
            if params is not None:
                maps = [kwargs, request.match_info]
                if request.method in ['GET', 'DELETE']:
                    maps.append(request.query)
                else:
                    try:
                        maps.append(await request.json())
                    except json.JSONDecodeError:
                        return json_response({"message": 'Invalid JSON'}, status=400)
                try:
                    kwargs = params(ChainMap(*maps))
                except Invalid as ex:
                    ret = self._response_from_invalid(ex)
            if ret is None:
                ret = await f(request, **kwargs)
            if returns is not None:
                if not isinstance(ret, StreamResponse):
                    ret = returns(ret)
                    ret = json_response(ret)
            elif ret is None:
                ret = Response(status=204)
            return ret
        wrapped.__name__ = f.__name__
        wrapped.__qualname__ = f.__qualname__
        wrapped.__doc__ = f.__doc__
        return wrapped

    def route(self, *args, **kwargs):
        routes_decorate = super().route(*args, **kwargs)

        def wrapper(f):
            return routes_decorate(self.decorate(f))

        return wrapper
