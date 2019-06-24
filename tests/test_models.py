# noqa
import os
import csv

import pytest
from sqlalchemy import Table, MetaData, create_engine

from parkeerrechten import settings
from parkeerrechten.models import get_backup_table_def

TEST_DATA_FILE = os.path.join(os.path.dirname(__file__), 'test-data.csv')

#URL(
#    '


# Steps
# * Create database for tests (as postgres user in postgres db)
# * Create requisite tables in test db
# * import the CSV data
# *

def test_get_backup_table_def():
    md = MetaData()

    table = get_backup_table_def(md, settings.NPR_TABLE)

    assert isinstance(table, Table)
