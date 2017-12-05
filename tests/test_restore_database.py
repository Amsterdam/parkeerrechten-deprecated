import os
import errno
import csv
import logging
import shutil
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, Table, MetaData, select
from sqlalchemy.sql import text
from sqlalchemy.engine.base import Connection

from parkeerrechten import settings
from parkeerrechten import models
from parkeerrechten import run_import
from parkeerrechten import dump_database
from parkeerrechten import namecheck
from parkeerrechten import restore_database

_CSV_FILENAME = os.path.join(os.path.dirname(__file__), 'test-data.csv')
_BACKUP_DIR = '/tmp/backups/'

LOG_FORMAT = '%(asctime)-15s - %(name)s - %(message)s'
logging.basicConfig(format=LOG_FORMAT, level=logging.DEBUG)
logger = logging.getLogger(__name__)

# -- fixtures --


@pytest.fixture(scope='module')
def npr_conn():
    with create_engine(settings.DATAPUNT_TEST_DB_URL).connect() as conn:
        yield conn


@pytest.fixture(scope='module')
def dp_conn():
    with create_engine(settings.DATAPUNT_DB_URL).connect() as conn:
        yield conn


# -- helper functions --


def _load_test_data(conn):
    """
    Load test data from provided CSV.
    """
    # clean out leftovers, create table for tests:
    conn.execute(text(
        '''DROP TABLE IF EXISTS "{}";'''.format(settings.NPR_TABLE)
    ))
    md = MetaData()
    test_table = models.get_backup_table_def(md, settings.NPR_TABLE)
    md.create_all(conn)

    # assume small test data file
    with open(_CSV_FILENAME, 'r') as f:
        reader = csv.DictReader(f, delimiter=',')
        rows = [r for r in reader]

    conn.execute(test_table.insert(), rows)


def _empty_out_local_db(conn):
    """
    Empty out the local database used during import (call before import test).
    """
    conn.execute(text(
        '''DROP TABLE IF EXISTS "{}";'''.format(settings.LOCAL_TABLE)
    ))


@patch('parkeerrechten.backup.get_batch_names_in_objectstore')
@patch('parkeerrechten.objectstore.upload_file')
@patch('parkeerrechten.objectstore.copy_file_from_objectstore')
def test_full_import_process_plus_restore(
        copy_mock, upload_mock, objectstore_mock, npr_conn, dp_conn):
    """
    Run full import process in test context.

    Both the NPR->LOCAL phase and the LOCAL->PG dump steps are tested.
    """
    logger.debug('Testing import process, using local stand-in for NPR ...')

    # Make sure that the patched get_batch_names_in_objectstore returns
    # an empty list, always.
    def empty_list(*args, **kwargs):
        return []
    objectstore_mock.side_effect = empty_list

    # Validate inputs.
    assert isinstance(dp_conn, Connection)
    assert isinstance(npr_conn, Connection)

    # Overwrite the relevant settings to allow batched import.
    # Furthermore set a basename for the generated pg_dumps.
    settings.DEBUG = False
    settings.BATCH_SIZE = 10
    settings.BACKUP_FILE_BASENAME = 'TEST'

    # Load test data into NPR stand in, check that we have 100 records.
    _load_test_data(npr_conn)
    r = npr_conn.execute(text(
        '''SELECT COUNT(*) FROM "{}";'''.format(settings.NPR_TABLE)
    )).fetchall()
    assert r[0][0] == 100

    # Clean up the local database.
    _empty_out_local_db(dp_conn)
    try:
        os.makedirs(_BACKUP_DIR)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    # Copy records from the NPR stand in to the local database, the
    # repeated imports result in grabbing the full data set.
    run_import._run_import(['--orphans'], npr_conn, dp_conn)
    run_import._run_import([], npr_conn, dp_conn)
    run_import._run_import([], npr_conn, dp_conn)
    run_import._run_import([], npr_conn, dp_conn)
    run_import._run_import([], npr_conn, dp_conn)

    r = dp_conn.execute(text(
        '''SELECT COUNT(*) FROM "{}";'''.format(settings.LOCAL_TABLE)
    )).fetchall()
    assert r[0][0] == 100
    logger.debug('There are now {} records.'.format(r[0][0]))
    logger.debug('... done.')

    # Call into the database dumping code, keep the dumped files for later.
    logger.debug('RUNNING TO DATABASE DUMPING STEP')
    dump_database._dump_database(dp_conn, remove_dumps=False)

    # ------

    logger.debug('RUNNING DATABASE RESTORE STEP')

    # patch object_store mock (again) to list locally available database dumps
    def list_local_database_dumps(*args, **kwargs):
        batch_names = []

        file_names = os.listdir(_BACKUP_DIR)
        for fn in file_names:
            if not namecheck.is_batch_file(fn, True):
                continue
            batch_names.append(namecheck.extract_batch_name(fn))

        return batch_names

    # copy from local backup directory to temp. dir used to restore from
    objectstore_mock.side_effect = list_local_database_dumps

    def copy_local_database_dump(_, file_name, temp_dir):
        shutil.copy(
            os.path.join(_BACKUP_DIR, file_name),
            os.path.join(temp_dir, file_name)
        )
        return os.path.join(temp_dir, file_name)
    copy_mock.side_effect = copy_local_database_dump

    # empyt out local DB, re-use connection to restore to that db
    _empty_out_local_db(dp_conn)

    restore_database._restore_database([], dp_conn)
    objectstore_mock.side_effect = list_local_database_dumps

    r = dp_conn.execute(text(
        '''SELECT COUNT(*) FROM "{}";'''.format(settings.TARGET_TABLE)
    )).fetchall()
    assert r[0][0] == 100
