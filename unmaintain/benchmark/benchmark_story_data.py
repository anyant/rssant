import os
import random
import timeit
import pyinstrument
import rssant_common.django_setup

from rssant_api.models.story_storage import StoryData


def random_content():
    return os.urandom(random.randint(1, 10) * 1024)


def main():
    p = pyinstrument.Profiler()
    p.start()
    t_lz4 = timeit.timeit(lambda: StoryData.decode(StoryData(
        random_content(), version=StoryData.VERSION_LZ4).encode()), number=10000)
    print(t_lz4)
    p.stop()
    html = p.output_html()
    with open('benchmark_story_data_lz4.html', 'w') as f:
        f.write(html)

    p = pyinstrument.Profiler()
    p.start()
    t_gzip = timeit.timeit(lambda: StoryData.decode(StoryData(
        random_content(), version=StoryData.VERSION_GZIP).encode()), number=10000)
    print(t_gzip)
    p.stop()
    html = p.output_html()
    with open('benchmark_story_data_gzip.html', 'w') as f:
        f.write(html)


if __name__ == "__main__":
    main()
