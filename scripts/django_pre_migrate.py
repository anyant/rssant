import logging

from django.db import connection


LOG = logging.getLogger(__name__)


def fix_django_migrations_id_seq():
    """
    Postgres django_migrations_id_seq may out of sync after pg_store, which cause migrate failed:
        django.db.utils.IntegrityError: duplicate key value violates unique constraint "django_migrations_pkey"
    """
    # https://stackoverflow.com/questions/4448340/postgresql-duplicate-key-violates-unique-constraint
    # https://stackoverflow.com/questions/244243/how-to-reset-postgres-primary-key-sequence-when-it-falls-out-of-sync
    # https://www.calazan.com/how-to-reset-the-primary-key-sequence-in-postgresql-with-django/
    table_names = connection.introspection.table_names()
    table = "django_migrations"
    if table not in table_names:
        return
    sql = """
    BEGIN;
    SELECT setval(pg_get_serial_sequence('"{table}"','id'),
        coalesce(max("id"), 1), max("id") IS NOT null) FROM "{table}";
    COMMIT;
    """.format(table=table)
    with connection.cursor() as cursor:
        cursor.execute(sql)


def run():
    fix_django_migrations_id_seq()
