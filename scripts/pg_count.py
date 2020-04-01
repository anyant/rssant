import sys
import json

import click
import django.apps
from django.db import connection

import rssant_common.django_setup  # noqa:F401
from rssant_common.helper import pretty_format_json

sql_count_limit = '''
SELECT count(*) as row_count
FROM (SELECT 1 FROM {table} LIMIT {limit}) t;
'''

sql_count_estimate = '''
SELECT relname as table_name, reltuples AS row_count
FROM pg_class WHERE relname=ANY(%s);
'''


def pg_count_limit(tables, limit):
    result = {}
    with connection.cursor() as cursor:
        for table in tables:
            sql = sql_count_limit.format(table=table, limit=limit)
            cursor.execute(sql)
            row_count, = cursor.fetchone()
            row_count = int(row_count)
            if row_count < limit:
                result[table] = row_count
    return result


def pg_count_estimate(tables):
    result = {}
    with connection.cursor() as cursor:
        cursor.execute(sql_count_estimate, [tables])
        for table, count in cursor.fetchall():
            result[table] = int(count)
    return result


def pg_count():
    """
    Problem:
        1. count(*) is slow for large table.
        2. estimate is not accuracy for small table.
    Solution:
        1. count(*) with limit, avoid slow for large tables
        2. query estimate from system table for large tables
    See also:
        https://stackoverflow.com/questions/7943233/fast-way-to-discover-the-row-count-of-a-table-in-postgresql
        https://wiki.postgresql.org/wiki/Count_estimate
    """
    models = django.apps.apps.get_models()
    tables = [m._meta.db_table for m in models]
    result = pg_count_limit(tables, limit=10000)
    large_tables = list(set(tables) - set(result))
    result.update(pg_count_estimate(large_tables))
    result = list(sorted(result.items()))
    return result


def pg_verify(result, expect_result, bias):
    result_map = {name: count for name, count in result}
    is_all_ok = True
    for name, expect_count in expect_result:
        count = result_map.get(name)
        if count is None:
            v_bias = 1.0
            is_ok = False
        else:
            v_bias = abs(expect_count - count) / (expect_count + 1)
            is_ok = v_bias < bias
        if not is_ok:
            is_all_ok = False
        count_text = str(count) if count is not None else '#'
        status = 'OK' if is_ok else 'ERROR'
        bias_text = '{}%'.format(round(v_bias * 100, 2))
        print(f'{status:<5s} {name:<35s} count={count_text:<7s} expect={str(expect_count):<7s} bias={bias_text}')
    return is_all_ok


@click.command()
@click.option('--verify', type=str, help='target filepath, or - to query database')
@click.option('--verify-bias', type=float, default=0.003)
@click.argument('filepath', type=str, default='-')
def main(verify, filepath, verify_bias):
    if verify:
        if verify != '-':
            with open(verify) as f:
                data = json.load(f)
            result = [(x['name'], x['count']) for x in data['tables']]
        else:
            result = pg_count()
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
        result = pg_count()
        tables = [dict(name=name, count=count) for name, count in result]
        content = pretty_format_json(dict(tables=tables))
        if filepath and filepath != '-':
            with open(filepath, 'w') as f:
                f.write(content)
        else:
            print(content)


if __name__ == "__main__":
    main()
