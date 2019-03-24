from rest_framework.response import Response
from rest_framework.decorators import api_view


@api_view()
def index(request):
    return Response({'message': "你好, RSSAnt!"})


@api_view()
def error(request):
    raise ValueError(request.GET.get('error') or 'A value error!')
