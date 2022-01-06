import json
import sys
from pathlib import Path

import fc2


def _load_config() -> dict:
    config_path = Path('~/.config/aliyun_fc_deploy.json')
    text = config_path.expanduser().read_text(encoding='utf-8')
    return json.loads(text)


def main():
    config = _load_config()
    ACCOUNT_ID = config.get('ALIBABA_CLOUD_ACCOUNT_ID')
    ACCESS_KEY_ID = config.get('ALIBABA_CLOUD_ACCESS_KEY_ID')
    ACCESS_KEY_SECRET = config.get('ALIBABA_CLOUD_ACCESS_KEY_SECRET')
    if not (ACCOUNT_ID and ACCESS_KEY_ID and ACCESS_KEY_SECRET):
        raise RuntimeError('access key envs required')
    BUILD_ID = sys.argv[1]
    client = fc2.Client(
        endpoint=f'{ACCOUNT_ID}.cn-zhangjiakou.fc.aliyuncs.com',
        accessKeyID=ACCESS_KEY_ID,
        accessKeySecret=ACCESS_KEY_SECRET,
    )
    name = 'rssant-img1'
    image = f'registry.cn-zhangjiakou.aliyuncs.com/rssant/async-api:{BUILD_ID}'
    response = client.update_function(
        serviceName=name,
        functionName=name,
        customContainerConfig=dict(image=image)
    )
    print(response)
    print(response.headers)
    print(response.data)


if __name__ == '__main__':
    main()
