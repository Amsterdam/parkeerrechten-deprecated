# noqa
from sqlalchemy import Table, MetaData

from parkeerrechten import settings
from parkeerrechten.models import get_backup_table_def


def test_get_backup_table_def():
    md = MetaData()

    table = get_backup_table_def(md)

    assert isinstance(table, Table)


def test_fill_db():
    md = MetaData()
    table = get_backup_table_def(md)

#     __file__ ==

