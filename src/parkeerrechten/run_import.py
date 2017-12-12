#!/usr/bin/env python
"""
Import Data from NPR to local database.
"""
import sys
import time
import logging

from sqlalchemy import create_engine, select, asc, Table, MetaData
from sqlalchemy.sql import literal, text

from . import settings
from . import models
from . import namecheck
from . import backup
from . import commandline

NPR_ENGINE = create_engine(settings.NPR_DB_URL)
DP_ENGINE = create_engine(settings.DATAPUNT_DB_URL)

LOG_FORMAT = '%(asctime)-15s - %(name)s - %(message)s'
logging.basicConfig(format=LOG_FORMAT, level=logging.DEBUG)
logger = logging.getLogger('run_import')


def batched_selection_iterator(connection, selection, batch_size, offset=0):
    """
    Given an SQLAlchemy connection + selection query, provide batched iterator.
    """
    while True:
        s = selection.limit(batch_size).offset(offset)
        rows = connection.execute(s).fetchall()

        # For quick test runs.
        if not rows:
            raise StopIteration
        yield rows

        offset += batch_size


def get_and_store_batch(npr_conn, dp_conn, batch_name):
    """
    Retrieve records from NPR, store them in local database in batches.
    """
    # set-up query for NPR
    md = MetaData()
    view = Table(
        settings.NPR_TABLE, md, autoload=True, autoload_with=npr_conn)

    selection = (
        select([view])
        .where(view.c.VER_BATCH_NAAM == literal(batch_name))
        .order_by(asc(view.c.VERW_RECHT_ID))
    )

    # Set up table in local database if needed
    md = MetaData()
    dp_table = models.get_backup_table_def(md, settings.LOCAL_TABLE)
    md.create_all(dp_conn)

    it = batched_selection_iterator(npr_conn, selection, settings.BATCH_SIZE, 0)
    for i, rows in enumerate(it):
        dp_conn.execute(dp_table.insert(), rows)
        logger.info(
            '{} records were stored for batch {} (iteration no: {}).'.format(
                len(rows), batch_name, i
            )
        )

        if settings.DEBUG:
            break


def _run_import(raw_args, npr_conn, dp_conn):
    # Determine which batchnames we will be querying for:
    logger.info('Checking command line arguments ...')
    args = commandline.parse_args(raw_args, include_orphans_option=True)

    # Check what is available on object store and in the local database
    # waiting to be dumped and copied to objectstore.
    on_objectstore = backup.get_batch_names_in_objectstore(include_leeg=True)
    logger.info('Backed-up batches in data store: %s', on_objectstore)

    in_local_db = backup.get_batch_names_in_database(
        dp_conn, settings.LOCAL_TABLE, include_leeg=True, require_table=False
    )
    logger.info('Backed-up batches in local db: %s', in_local_db)

    backed_up = on_objectstore + in_local_db

    # Check what is requested by the user:
    if args.orphans:
        batch_names = ['Leeg']
    else:
        # What is available in the NPR database in requested date range.
        batch_names = backup.get_batch_names_in_database(
            npr_conn, settings.NPR_TABLE, False)
        batch_names = namecheck.filter_batch_names_by_date(
            batch_names, args.startdate, args.enddate)

    # We want batches that are requested and not yet backed up (these
    # are the set of candidates to back up).
    batch_names = list(set(batch_names) - set(backed_up))
    batch_names.sort()
    logger.info('NPR batches still to be backed up: %s', batch_names)

    # only take some of the batches each run and store them:
    logger.info('At most %d batches will backed up', settings.N_DAYS_PER_RUN)
    logger.info(
        'Backing up following batches: %s', batch_names[:settings.N_DAYS_PER_RUN])

    for batch_name in batch_names[:settings.N_DAYS_PER_RUN]:
        get_and_store_batch(npr_conn, dp_conn, batch_name)

    if batch_names:
        sql = '''select count(*) from "{}"'''.format(settings.LOCAL_TABLE)
        result = dp_conn.execute(text(sql)).fetchall()
        logger.info(
            'There are now %s records in the local postgres db', result[0])
    else:
        logger.info('No new backups were needed.')


def main():
    """
    Script entrypoint, run full import process.
    """
    logger.info('Starting NPR parkeerrechten import script.')
    logger.info('Script was called with: %s', sys.argv)
    t0 = time.time()
    # Establish needed database connections (Datapunt local, NPR remote):
    with NPR_ENGINE.connect() as npr_conn, DP_ENGINE.connect() as dp_conn:
        _run_import(sys.argv[1:], npr_conn, dp_conn)
    dt = time.time() - t0
    logger.info('The script took %.2f seconds to run', dt)


if __name__ == '__main__':
    main()
