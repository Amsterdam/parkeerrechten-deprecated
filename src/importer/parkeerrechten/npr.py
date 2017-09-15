"""
Queries for the NPR database (the data source in this context).
"""
from sqlalchemy import Table, MetaData, Column, Integer
from sqlalchemy import select, literal, distinct, text, asc

from settings import NPR_TABLE


def get_view(connection):
    md = MetaData()
    view = Table(NPR_TABLE, md, autoload=True, autoload_with=connection)

    return view


def get_batch_names(connection, view):
    """select distinct("VER_BATCH_NAAM") from "VW_0363";"""
    s = (
        select([distinct(view.c.VER_BATCH_NAAM)])
        .order_by(asc(view.c.VER_BATCH_NAAM))
    )
    return connection.execute(s).fetchall()


def parse_batch_names(batch_names):
    """ """
    pass
