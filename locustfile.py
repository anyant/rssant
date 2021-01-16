import random
from locust import HttpUser, between, task


class WebsiteUser(HttpUser):

    wait_time = between(3, 30)

    def on_start(self):
        response = self.client.post("/api/v1/user/login/", json={
            "account": "locust@localhost",
            "password": "locust123456"
        })
        csrftoken = response.cookies['csrftoken']
        self.client.headers['x-csrftoken'] = csrftoken
        response = self.client.post("/api/v1/feed/query")
        feeds = response.json()['feeds']
        self.feeds = [x for x in feeds if x['total_storys'] > 0]
        assert self.feeds, 'locust not has feeds!'

    @task
    def view_home(self):
        hints = []
        for feed in self.feeds[:int(len(self.feeds) * 0.8)]:
            hints.append(dict(
                id=feed['id'],
                dt_updated=feed['dt_updated'],
            ))
        self.client.post("/api/v1/feed/query", json=dict(hints=hints))
        mushrooms = []
        for feed in random.sample(self.feeds, min(20, len(self.feeds))):
            total = feed['total_storys']
            limit = random.randint(1, min(3, total))
            item = dict(feed_id=feed['id'], offset=total - limit, limit=limit)
            mushrooms.append(item)
        self.client.post("/api/v1/story/query-batch", json=dict(storys=mushrooms))

    @task(weight=6)
    def view_feed(self):
        feed = random.choice(self.feeds)
        self.client.get('/api/v1/story/query', params=dict(
            feed_id=feed['id'],
            detail=True,
            offset=max(0, feed['total_storys'] - 15),
            size=15,
        ), name='/api/v1/story/query?[feed_id,offset,size=15]')

    @task(weight=8)
    def view_story(self):
        feed = random.choice(self.feeds)
        min_offset = max(0, feed['total_storys'] - 100)
        max_offset = max(0, feed['total_storys'] - 1)
        offset = random.randint(min_offset, max_offset)
        url = '/api/v1/story/{}-{}?detail=true'.format(feed['id'], offset)
        self.client.get(url, name='/api/v1/story/[feed_id]:[offset]')
        url = '/api/v1/feed/{}/offset'.format(feed['id'])
        self.client.put(url, json=dict(offset=offset), name='/api/v1/feed/[feed_id]/offset')
