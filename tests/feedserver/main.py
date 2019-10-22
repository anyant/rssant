from flask import Flask, redirect


app = Flask(__name__)


@app.route('/feed/302')
def do_redirect():
    target = 'http://blog.guyskk.com/feed.xml'
    return redirect(target, 302)


if __name__ == "__main__":
    app.run()
