# connect to database, retrieve batches
# generate pg_dump commands
# loop over the pg_dump commands, execute them to local /tmp dir
# upload the file to the object store (files that are present are skipped)
import sys
import logging
import os
import subprocess
import objectstore

from sqlalchemy import create_engine, select, asc, distinct, Table, MetaData
from sqlalchemy.sql import literal, text

import settings
from run_import import parse_date_string

NPR_ENGINE = create_engine(settings.NPR_DB_URL)
DP_ENGINE = create_engine(settings.DATAPUNT_DB_URL)
BASENAME = os.environ['BACKUP_FILE_BASENAME']


LOG_FORMAT = '%(asctime)-15s - %(name)s - %(message)s'
logging.basicConfig(format=LOG_FORMAT, level=logging.DEBUG)
logger = logging.getLogger('dump_database')


def get_batches_in_local_db(dp_conn):
    md = MetaData()
    table = Table(
        'VW_0363_BACKUP', md, autoload=True, autoload_with=dp_conn)

    selection = (
        select([distinct(table.c.VER_BATCH_NAAM)])
        .where(table.c.VER_BATCH_NAAM != literal('Leeg'))
        .order_by(asc(table.c.VER_BATCH_NAAM))
    )

    # on the order of a few hundred days:
    unvalidated_batchnames = [
        row[0] for row in dp_conn.execute(selection).fetchall()]

    # validate that we have only dates as batch names
    batch_names = []
    rejected_batch_names = []
    for b in unvalidated_batchnames:
        if b == 'Leeg':
            batch_names.append(b)
            continue

        try:
            y, m, d = parse_date_string(b)
        except ValueError:
            logger.info('Cannot parse %s as date - skipping.', b)
            rejected_batch_names.append(b)
        else:
            batch_names.append(b)

    return batch_names, rejected_batch_names


def main(dp_conn):
    # connect to database, retrieve batches
    batch_names, extra = get_batches_in_local_db(dp_conn)

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
        dp_conn.execute(create_table, {'batch_name': batch_name})

        dump_file = os.path.join(
            '/', 'tmp', 'backups', batch_name + '_' + BASENAME + '.dump')

        cmd = [
            'pg_dump',
            '--clean',
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

        # upload this stuff to the object store
        objectstore.upload_file('parkeerrechten_pgdumps', dump_file)

        os.remove(dump_file)



if __name__ == '__main__':
    logger.info('Starting the NPR database dumper script')
    logger.info('Script was started with command: %s', sys.argv)
    with DP_ENGINE.connect() as dp_conn:
        main(dp_conn)


