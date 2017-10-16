#!/usr/bin/env python3
"""
Dump local parkeerrechten database in daily batches.
"""
import sys
import logging
import os
import subprocess

from sqlalchemy import create_engine
from sqlalchemy.sql import text

from . import settings
from . import objectstore
from . import backup

DP_ENGINE = create_engine(settings.DATAPUNT_DB_URL)

LOG_FORMAT = '%(asctime)-15s - %(name)s - %(message)s'
logging.basicConfig(format=LOG_FORMAT, level=logging.DEBUG)
logger = logging.getLogger('dump_database')


def _pg_dump(filename):
    """Construct pg_dump commandline and execute it."""
    cmd = [
        'pg_dump',
        '--host=database',
        '--username=parkeerrechten',
        '--port=5432',
        '--no-password',  # use .pgpass (or fail)
        '--format=c',
        '--table="dumptable"',
        '--exclude-table=auth*',
        '--dbname=parkeerrechten',
    ]

    logger.info('Running command: %s', cmd)
    with open(filename, 'wb') as outfile:
        p = subprocess.Popen(cmd, stdout=outfile)
    p.wait()
    logger.info('Return code: %d', p.returncode)


def _back_up_batches(dp_conn, batch_names):
    """For each batch in database run a database dump."""
    # connect to local db, prepare views:
    create_table = text(
        """
        CREATE TABLE "dumptable" AS SELECT * FROM "{}" WHERE
        "VER_BATCH_NAAM" = :batch_name """.format(settings.LOCAL_TABLE)
    )
    drop_table = text("""DROP TABLE "dumptable"; """)

    # Clean up left-overs from previous failed runs.
    try:
        dp_conn.execute(drop_table)
    except:
        pass

    # Go over the batches in the local database and dump them.
    for batch_name in batch_names:
        logger.debug('Backing up batch: %s', batch_name)
        dump_file = os.path.join(
            '/', 'tmp', 'backups', batch_name + '_' + settings.BASENAME + '.dump')

        # Create temporary tables and dump them (views will not work).
        dp_conn.execute(create_table, {'batch_name': batch_name})
        _pg_dump(dump_file)
        dp_conn.execute(drop_table, {'batch_name': batch_name})

        # Upload to the object store.
        objectstore.upload_file(settings.OBJECT_STORE_CONTAINER, dump_file)

        # Remove local dump.
        os.remove(dump_file)

    # Throw away local database table
    dp_conn.execute("""DROP TABLE "{}";""".format(settings.LOCAL_TABLE))


def _dump_database(dp_conn):
    """
    Check for back-ups to perform and run database dump process.
    """
    logger.info('Starting the NPR database dumper script')
    logger.info('Script was started with command: %s', sys.argv)
    # check that we have any table dumping to do:
    if not DP_ENGINE.has_table(settings.LOCAL_TABLE):
        logger.info('No table to back up, exiting.')
        sys.exit(0)

    # connect to database, retrieve batches
    batch_names = backup.get_batch_names_in_database(
        dp_conn, settings.LOCAL_TABLE)

    # connect to object store and see which batches are available
    backed_up = backup.get_batch_names_in_objectstore(include_leeg=True)
    logger.info('Backed-up batches present on the datastoree %s', backed_up)

    batch_names = list(set(batch_names) - set(backed_up))
    if batch_names:
        _back_up_batches(dp_conn, batch_names)
        logger.info('Made the required backups, exiting.')
    else:
        logger.info('No new backups need to be made, exiting.')


def main():
    """
    Script entrypoint, dump the local database and upload to object store.
    """
    with DP_ENGINE.connect() as dp_conn:
        _dump_database(dp_conn)


if __name__ == '__main__':
    main()

