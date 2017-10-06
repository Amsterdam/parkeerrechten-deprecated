#!/usr/bin/env python
"""
Import Data from NPR to local database.
"""
import sys
import argparse
import time
import logging

from sqlalchemy import create_engine, select, asc, distinct, Table, MetaData
from sqlalchemy.sql import literal

from . import settings
from . import models
from . import namecheck
from . import backup

NPR_ENGINE = create_engine(settings.NPR_DB_URL)
DP_ENGINE = create_engine(settings.DATAPUNT_DB_URL)

LOG_FORMAT = '%(asctime)-15s - %(name)s - %(message)s'
logging.basicConfig(format=LOG_FORMAT, level=logging.DEBUG)
logger = logging.getLogger('run_import')


class ValidationError(Exception):
    pass


def check_args(args):
    """
    Validate that the dates, and date range are valid.
    """
    if args.startdate:
        y, m, d = namecheck.parse_date_string(args.startdate)
        if y < 2016:
            raise namecheck.ValidationError('No data before 2016')

    if args.enddate:
        y, m, d = namecheck.parse_date_string(args.enddate)
        if y < 2016:
            raise ValidationError('No data before 2016')

    if args.startdate and args.enddate:
        if args.startdate > args.enddate:
            raise ValidationError('startdate cannot be after enddate')


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--startdate', type=str,
        help='Earliest batch to download specify as follows: YYYYMMDD')
    parser.add_argument(
        '--enddate', type=str,
        help='Last batch to download specify as follows: YYYYMMDD')
    parser.add_argument(
        '--orphans', action='store_true',
        help='Download records that have no batch name.')

    args = parser.parse_args()
    try:
        check_args(args)
    except(ValidationError, ValueError) as e:
        logger.error('Commandline argument(s) are wrong')
        raise e

    return args


def get_batch_names_in_npr(npr_conn):
    # do query for all distinct batchnames in NPR here
    md = MetaData()
    view = Table(
        settings.NPR_TABLE, md, autoload=True, autoload_with=npr_conn)

    selection = (
        select([distinct(view.c.VER_BATCH_NAAM)])
        .where(view.c.VER_BATCH_NAAM != literal('Leeg'))
        .order_by(asc(view.c.VER_BATCH_NAAM))
    )

    # on the order of a few hundred days:
    unvalidated_batchnames = [
        row[0] for row in npr_conn.execute(selection).fetchall()]

    # validate that we have only dates as batch names
    batch_names = namecheck.filter_batch_names(
        unvalidated_batchnames, include_leeg=False)

    return batch_names


def batched_selection_iterator(connection, selection, batch_size, offset=0):
    while True:
        s = selection.limit(batch_size).offset(offset)
        rows = connection.execute(s).fetchall()
        # for development purposes th
        if not rows or offset:
            raise StopIteration
        yield rows

        offset += batch_size


def get_and_store_batch(npr_conn, dp_conn, batch_name):
    # set-up query for NPR
    md = MetaData()
    view = Table(
        settings.NPR_TABLE, md, autoload=True, autoload_with=npr_conn)

    selection = (
        select([view])
        .where(view.c.VER_BATCH_NAAM == literal(batch_name))
        .order_by(asc(view.c.VERW_RECHT_ID))
    )

    # Set up table in local database if needed (TODO: refactor)
    md = MetaData()
    dp_table = models.get_backup_table_def(md)
    md.create_all(DP_ENGINE)

    it = batched_selection_iterator(npr_conn, selection, settings.BATCH_SIZE, 0)
    for i, rows in enumerate(it):
        dp_conn.execute(dp_table.insert(), rows)
        logger.info(
            'For batch %s we stored %d records (iteration no: %d).',
            batch_name, len(rows), i
        )

        if settings.DEBUG:
            break


def main():
    t0 = time.time()
    logger.info('Starting NPR parkeerrechten import script.')
    logger.info('Script was called with: %s', sys.argv)

    with NPR_ENGINE.connect() as npr_conn, DP_ENGINE.connect() as dp_conn:
        main_2(npr_conn, dp_conn)
    logger.info('Starting NPR parkeerrechten import script.')
    dt = time.time() - t0
    logger.info('The script took %.2f seconds to run', dt)


def main_2(npr_conn, dp_conn):
    # Determine which batchnames we will be querying for:
    args = parse_args()

    # Check what is available on object store
    backed_up = backup.get_backed_up_batches(include_leeg=True)
    logger.info('Backed-up batches present on the datastore %s', backed_up)

    # Check what is requested by the user:
    if args.orphans:
        batch_names = ['Leeg']
    else:
        # What is available in the NPR database in requested date range.
        batch_names = get_batch_names_in_npr(npr_conn)
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
        result = DP_ENGINE.execute(
            'select count(*) from "VW_0363_BACKUP"').fetchall()
        logger.info(
            'There are now %s records in the local postgres db', result[0])
    else:
        logger.info('No new backups were needed.')


if __name__ == '__main__':
    # Establish needed database connections (Datapunt local, NPR remote):
    main()

