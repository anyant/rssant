import inspect
from validr import T


def get_params(f):
    sig = inspect.signature(f)
    params_schema = {}
    for name, p in list(sig.parameters.items())[1:]:
        if p.default is not inspect.Parameter.empty:
            raise ValueError('You should not set default in schema annotation!')
        if p.annotation is inspect.Parameter.empty:
            raise ValueError(f'Missing annotation in parameter {name}!')
        params_schema[name] = p.annotation
    if params_schema:
        return T.dict(params_schema).__schema__
    return None


def get_returns(f):
    sig = inspect.signature(f)
    if sig.return_annotation is not inspect.Signature.empty:
        schema = sig.return_annotation
        return T(schema).__schema__
    return None
