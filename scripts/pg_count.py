import json
import sys

import click

from rssant_common.helper import pretty_format_json
from rssant_harbor.pg_count import pg_count, pg_verify


@click.command()
@click.option('--verify', type=str, help='target filepath, or - to query database')
@click.option('--verify-bias', type=float, default=0.003)
@click.argument('filepath', type=str, default='-')
def main(verify, filepath, verify_bias):
    import rssant_common.django_setup  # noqa:F401

    if verify:
        if verify != '-':
            with open(verify) as f:
                result = json.load(f)
        else:
            result = pg_count()
        if filepath and filepath != '-':
            with open(filepath) as f:
                content = f.read()
        else:
            content = sys.stdin.read()
        expect_result = json.loads(content)
        verify_result = pg_verify(result, expect_result, verify_bias)
        for detail in verify_result['details']:
            print(detail['message'])
        is_ok = verify_result['is_all_ok']
        sys.exit(0 if is_ok else 1)
    else:
        result = pg_count()
        content = pretty_format_json(result)
        if filepath and filepath != '-':
            with open(filepath, 'w') as f:
                f.write(content)
        else:
            print(content)


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
