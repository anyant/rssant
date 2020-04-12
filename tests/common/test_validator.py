from timeit import Timer
from validr import T, Compiler, Invalid
from validators import url as _validators_url
from django.core.validators import URLValidator
from django.core.exceptions import ValidationError


def validators_url(x):
    return _validators_url(x) is True


_validr_url = Compiler().compile(T.url.scheme('http https'))


def validr_url(x):
    try:
        _validr_url(x)
    except Invalid:
        return False
    else:
        return True


_django_url = URLValidator({'http', 'https'})


def django_url(x):
    try:
        _django_url(x)
    except ValidationError:
        return False
    else:
        return True


url_cases = {
    'valid': [
        'http://127.0.0.1:8080/hello?key=中文',
        'http://tool.lu/regex/',
        'https://github.com/guyskk/validator',
        'https://avatars3.githubusercontent.com/u/6367792?v=3&s=40',
        'https://github.com',
        'https://www.google.com/' + 'x' * 128,
    ],
    'invalid': [
        'mail@qq.com',
        'google',
        'readme.md',
        'github.com',
        'www.google.com',
        'http：//www.google.com',
        '//cdn.bootcss.com/bootstrap/4.0.0-alpha.3/css/bootstrap.min.css',
    ]
}


def _benchmark_url_validator(fn):
    for url in url_cases['valid']:
        assert fn(url), f'valid url={url}'
    for url in url_cases['invalid']:
        if fn(url):
            print(f'invalid url={url}')


def test_benchmark_url_validator():
    funcs = [
        ('validr', validr_url),
        ('validators', validators_url),
        ('django', django_url),
    ]
    bench = _benchmark_url_validator
    for name, fn in funcs:
        print(name.center(79, '-'))
        bench(fn)
        print('OK')
    for name, fn in funcs:
        print(name.center(79, '-'))
        n, t = Timer(lambda: bench(fn)).autorange()
        print('{:>8} loops cost {:.3f}s'.format(n, t))


if __name__ == "__main__":
    test_benchmark_url_validator()
