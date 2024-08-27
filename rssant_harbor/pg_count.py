import django.apps
from django.db import connection

sql_count_limit = '''
SELECT count(*) as row_count
FROM (SELECT 1 FROM {table} LIMIT {limit}) t;
'''

sql_count_estimate = '''
SELECT relname as table_name, reltuples AS row_count
FROM pg_class WHERE relname=ANY(%s);
'''

# https://stackoverflow.com/questions/14730228/postgresql-query-to-list-all-table-names
sql_select_tables = '''
SELECT table_name FROM information_schema.tables
WHERE table_schema='public' AND table_type='BASE TABLE';
'''


def get_all_tables():
    table_s = []
    with connection.cursor() as cursor:
        cursor.execute(sql_select_tables)
        for row in cursor.fetchall():
            table_s.append(row[0])
    return table_s


def get_story_volume_tables():
    all_tables = get_all_tables()
    return [x for x in all_tables if x.startswith('story_volume_')]


def pg_count_limit(tables, limit):
    result = {}
    with connection.cursor() as cursor:
        for table in tables:
            sql = sql_count_limit.format(table=table, limit=limit)
            cursor.execute(sql)
            (row_count,) = cursor.fetchone()
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
    story_volume_tables = get_story_volume_tables()
    tables.extend(story_volume_tables)
    result = pg_count_limit(tables, limit=10000)
    large_tables = list(set(tables) - set(result))
    result.update(pg_count_estimate(large_tables))
    result = list(sorted(result.items()))
    ret_tables = [dict(name=name, count=count) for name, count in result]
    return dict(tables=ret_tables)


def pg_verify(result, expect_result, bias):
    result_map = {}
    for item in result['tables']:
        result_map[item['name']] = item['count']
    is_all_ok = True
    details = []
    for expect_item in expect_result['tables']:
        name = expect_item['name']
        expect_count = expect_item['count']
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
        message = f'{status:<5s} {name:<35s} count={count_text:<7s} expect={str(expect_count):<7s} bias={bias_text}'
        details.append(
            dict(
                name=name,
                is_ok=is_ok,
                message=message,
            )
        )
    return dict(
        is_all_ok=is_all_ok,
        details=details,
    )
