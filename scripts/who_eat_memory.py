import sys
import module_graph


IS_GEN = len(sys.argv) >= 2 and sys.argv[1] == 'gen'
INPUT_FILEPATH = 'data/rssant_module_graph.json'

if IS_GEN and __name__ == "__main__":
    module_graph.setup_hooker(save_to=INPUT_FILEPATH, verbose=True)


import rssant_common.django_setup  # noqa: F401,F402
from rssant_common.logger import configure_logging  # noqa: F402
from module_graph.traveler import ModuleTraveler  # noqa: F402
from module_graph.render import render_graph  # noqa: F402


IGNORE = """
*test*
encodings.cp65001
ctypes.wintypes
django.contrib.flatpages.*
django.contrib.redirects.*
django.contrib.gis.*
django.db.*oracle*
django.db.*mysql*
tornado.platform.windows
coreschema.encodings.corejson
readability.compat.two
nltk.tokenize.nist
nltk.twitter*
nltk.book*
nltk.app*
numpy.ma.version*
prompt_toolkit.*win*
gunicorn.workers.*gevent*
whitenoise.django*
"""


if __name__ == "__main__":
    if IS_GEN:
        configure_logging(level='DEBUG')
        traveler = ModuleTraveler(ignore=IGNORE)
        traveler.run()
    else:
        render_graph(
            input_filepath=INPUT_FILEPATH,
            output_filepath='data/rssant_module_graph.pdf',
            modules_filepath='data/rssant_modules.txt',
            threshold=0,
        )
