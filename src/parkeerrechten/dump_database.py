#!/usr/bin/env python3
"""
Dump local parkeerrechten database in daily batches.
"""
import sys
import logging
import os
import subprocess

from sqlalchemy import create_engine, select, asc, distinct, Table, MetaData
from sqlalchemy.sql import text

from . import settings
from . import objectstore
from  .namecheck import filter_batch_names
from  .backup import get_backed_up_batches

NPR_ENGINE = create_engine(settings.NPR_DB_URL)
DP_ENGINE = create_engine(settings.DATAPUNT_DB_URL)


LOG_FORMAT = '%(asctime)-15s - %(name)s - %(message)s'
logging.basicConfig(format=LOG_FORMAT, level=logging.DEBUG)
logger = logging.getLogger('dump_database')


def get_batches_in_local_db(dp_conn):
    """See which batches are available in local (import) database"""
    md = MetaData()
    table = Table(
        'VW_0363_BACKUP', md, autoload=True, autoload_with=dp_conn)

    selection = (
        select([distinct(table.c.VER_BATCH_NAAM)])
        .order_by(asc(table.c.VER_BATCH_NAAM))
    )

    # on the order of a few hundred days:
    unvalidated_batchnames = [
        row[0] for row in dp_conn.execute(selection).fetchall()]

    # validate that we have only dates as batch names
    return filter_batch_names(
        unvalidated_batchnames, include_leeg=True)


def back_up_batches(dp_conn, batch_names):
    # connect to local db, prepare views:
    create_table = text(
        """CREATE TABLE "dumptable" AS SELECT * FROM "VW_0363_BACKUP" WHERE """
        """"VER_BATCH_NAAM" = :batch_name """
    )
    drop_table = text("""DROP TABLE "dumptable"; """)

    try:
        dp_conn.execute(drop_table)
    except:
        pass

    for batch_name in batch_names:
        logger.debug('Backing up batch: %s', batch_name)
        dp_conn.execute(create_table, {'batch_name': batch_name})
        dump_file = os.path.join(
            '/', 'tmp', 'backups', batch_name + '_' + settings.BASENAME + '.dump')
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
        with open(dump_file, 'wb') as outfile:
            p = subprocess.Popen(cmd, stdout=outfile)
        p.wait()
        logger.info('Return code: %d', p.returncode)
        dp_conn.execute(drop_table, {'batch_name': batch_name})

        # upload to the object store
        objectstore.upload_file(settings.OBJECT_STORE_CONTAINER, dump_file)

        # remove local dump
        os.remove(dump_file)

    # Throw away local database table
    dp_conn.execute("""DROP TABLE "VW_0363_BACKUP";""")


def main():
    logger.info('Starting the NPR database dumper script')
    logger.info('Script was started with command: %s', sys.argv)
    with DP_ENGINE.connect() as dp_conn:
        main_2(dp_conn)


def main_2(dp_conn):
    # check that we have any table dumping to do:
    if not DP_ENGINE.has_table('VW_0363_BACKUP'):
        logger.info('No table to back up, exiting.')
        sys.exit(0)

    # connect to database, retrieve batches
    batch_names = get_batches_in_local_db(dp_conn)

    # connect to object store and see which batches are available
    backed_up = get_backed_up_batches(include_leeg=True)
    logger.info('Backed-up batches present on the datastore %s', backed_up)

    batch_names = list(set(batch_names) - set(backed_up))
    if batch_names:
        back_up_batches(dp_conn, batch_names)
        logger.info('Made the required backups, exiting.')
    else:
        logger.info('No new backups need to be made, exiting.')


if __name__ == '__main__':
    main()

