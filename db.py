# -*- coding: utf-8 -*-
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from logger import logger

__all__ = [
    'fetch_from_table',
    'fetch_one_row',
    'TableNotFoundException'
]

DATABASE_PASSWORD = os.getenv('DB_PASSWORD')
DATABASE_URI = 'mysql+pymysql://knowledge:{0}@localhost/knowledge'.format(DATABASE_PASSWORD)

engine = create_engine(
    DATABASE_URI,
    convert_unicode=True,
    pool_recycle=3600, pool_size=10
)

db_session = scoped_session(sessionmaker(
    autocommit=False, autoflush=False, bind=engine
))


def _check_table(table_name):
    """
    checks if the given table is available from knowledge database.
    """
    check_table_sql = "SELECT exists(SELECT * FROM information_schema.tables WHERE table_name='{0}')".format(table_name)
    logger.info(check_table_sql)
    rs = db_session.execute(check_table_sql)
    table_row = rs.fetchone()[0]
    if not table_row:
        raise TableNotFoundException("Given table name {0} cannot be found".format(table_name))


def _get_pk(table_name, default_pk='id'):
    """
    gets the primary key name from given table,
    if nothing found, default primary key is used.
    """
    get_pk_sql = "SELECT k.COLUMN_NAME FROM information_schema.table_constraints t " \
                 "LEFT JOIN information_schema.key_column_usage k USING (constraint_name, table_schema, table_name) " + \
                 "WHERE t.constraint_type = 'PRIMARY KEY' AND t.table_schema=DATABASE() AND t.table_name = '{0}'".format(
                     table_name)
    logger.info(get_pk_sql)
    rs = db_session.execute(get_pk_sql)
    pk_name = rs.fetchone()[0]
    if not pk_name:
        pk_name = default_pk
    return pk_name


def fetch_from_table(table_name, limit, offset):
    _check_table(table_name)

    rs = db_session.execute("SELECT * FROM {0} LIMIT {1} OFFSET {2}".format(table_name, limit, offset))
    return rs.fetchall()


def fetch_one_row(table_name, id, pk='id'):
    _check_table(table_name)
    pk = _get_pk(table_name, pk)

    rs = db_session.execute("SELECT * FROM {0} WHERE {1} = {2}".format(table_name, pk, id))
    return rs.fetchone()


class TableNotFoundException(Exception):
    pass
