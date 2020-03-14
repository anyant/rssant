import os
import sys
import json

import click
import django.apps
from django.db import connection

from rssant_common.helper import pretty_format_json


sql_count = '''
SELECT relname as table_name, reltuples AS row_count
FROM pg_class WHERE relname=ANY(%s);
'''


def pg_count():
    models = django.apps.apps.get_models()
    tables = [m._meta.db_table for m in models]
    result = []
    with connection.cursor() as cursor:
        cursor.execute(sql_count, [tables])
        for table, count in cursor.fetchall():
            result.append((table, int(count)))
    result = list(sorted(result))
    return result


def pg_verify(result, expect_result, bias):
    result_map = {name: count for name, count in result}
    is_all_ok = True
    for name, expect_count in expect_result:
        count = result_map.get(name)
        if count is None:
            is_ok = False
        else:
            v_bias = abs(expect_count - count) / (expect_count + 1)
            is_ok = v_bias < bias
        if not is_ok:
            is_all_ok = False
        count_text = str(count) if count is not None else '#'
        status = 'OK' if is_ok else 'ERROR'
        print(f'{status:<5s} {name:<35s} count={count_text:<7s} expect={str(expect_count):<7s}')
    return is_all_ok


@click.command()
@click.option('--verify', is_flag=True, default=False)
@click.option('--verify-bias', type=float, default=0.001)
@click.argument('filepath', type=str, default='-')
def main(verify, filepath, verify_bias):
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'rssant.settings')
    django.setup()
    result = pg_count()
    tables = [dict(name=name, count=count) for name, count in result]
    if verify:
        if filepath and filepath != '-':
            with open(filepath) as f:
                content = f.read()
        else:
            content = sys.stdin.read()
        expect_data = json.loads(content)
        expect_result = [(x['name'], x['count']) for x in expect_data['tables']]
        is_ok = pg_verify(result, expect_result, verify_bias)
        sys.exit(0 if is_ok else 1)
    else:
        content = pretty_format_json(dict(tables=tables))
        if filepath and filepath != '-':
            with open(filepath, 'w') as f:
                f.write(content)
        else:
            print(content)


if __name__ == "__main__":
    main()
