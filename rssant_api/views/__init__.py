from rest_framework.response import Response
from rest_framework.decorators import api_view


@api_view()
def index(request):
    return Response({'message': "你好, RSSAnt!"})
