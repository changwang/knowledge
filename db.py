# -*- coding: utf-8 -*-

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

__all__ = [
    'fetch_from_table',
    'fetch_one_row',
    'TableNotFoundException'
]

engine = create_engine(
    'postgresql+psycopg2://postgres@localhost/chang',
    convert_unicode=True,
    pool_recycle=3600, pool_size=10
)

db_session = scoped_session(sessionmaker(
    autocommit=False, autoflush=False, bind=engine
))


def _check_table(table_name):
    rs = db_session.execute(
        "SELECT exists(SELECT * FROM information_schema.tables WHERE table_name='{0}')".format(table_name))
    table_row = rs.fetchone()[0]
    if not table_row:
        raise TableNotFoundException("Given table name {0} cannot be found".format(table_name))


def fetch_from_table(table_name, limit, offset):
    _check_table(table_name)

    rs = db_session.execute("SELECT * FROM {0} OFFSET {1} LIMIT {2}".format(table_name, offset, limit))
    return rs.fetchall()


def fetch_one_row(table_name, id, pk='id'):
    _check_table(table_name)

    rs = db_session.execute("SELECT * FROM {0} WHERE {1} = {2}".format(table_name, pk, id))
    return rs.fetchone()


class TableNotFoundException(Exception):
    pass
