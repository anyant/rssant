from aiohttp import web

from .views import routes


app = web.Application()
app.router.add_routes(routes)


if __name__ == "__main__":
    web.run_app(app)
