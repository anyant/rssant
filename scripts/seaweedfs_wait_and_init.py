import time
import sys
import requests


DEFAULT_URL = "http://127.0.0.1:9333/dir/assign"


def wait_and_init(url=None):
    if not url and len(sys.argv) >= 2:
        url = sys.argv[1]
    if not url:
        url = DEFAULT_URL
    timeout = time.time() + 180
    while time.time() < timeout:
        try:
            response = requests.get(url)
            if response.ok:
                print(response.json())
                break
            print("seaweedfs response:", response.status_code)
        except Exception as ex:
            print(ex)
        time.sleep(1)


if __name__ == "__main__":
    wait_and_init()
