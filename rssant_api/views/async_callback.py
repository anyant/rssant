from validr import T
from rest_framework.permissions import AllowAny
from django_rest_validr import RestRouter
from rssant_api.tasks import rss


AsyncCallbackView = RestRouter(permission_classes=[AllowAny])


@AsyncCallbackView.post('async_callback/story')
def async_callback_story(
    request,
    id: T.str
) -> T.dict(message=T.str):
    rss.process_story_webpage.delay(id)
    return {'message': 'OK'}


@AsyncCallbackView.post('async_callback/story_images')
def async_callback_story_images(
    request,
    story: T.dict(
        id = T.str,
        url = T.url.optional,
    ),
    images: T.list(T.dict(
        status = T.int,
        url = T.url,
    ))
) -> T.dict(message=T.str):
    rss.process_story_images.delay(story_id=story['id'], story_url=story['url'], images=images)
    return {'message': 'OK'}
