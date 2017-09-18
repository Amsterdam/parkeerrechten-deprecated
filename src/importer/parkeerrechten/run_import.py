#!/usr/bin/env python
"""
Import Data from NPR to local database.
"""
import os
import sys
import argparse
import time
import logging
import re

from sqlalchemy import create_engine, select, asc, distinct, Table, MetaData
from sqlalchemy.sql import literal

import settings
import datapunt
import objectstore

NPR_ENGINE = create_engine(settings.NPR_DB_URL)
DP_ENGINE = create_engine(settings.DATAPUNT_DB_URL)

BASENAME = os.environ['BACKUP_FILE_BASENAME']

DEBUG = True
BATCH_SIZE = 50000
N_DAYS_PER_RUN = 10

LOG_FORMAT = '%(asctime)-15s - %(name)s - %(message)s'
logging.basicConfig(format=LOG_FORMAT, level=logging.DEBUG)
logger = logging.getLogger('run_import')


class ValidationError(Exception):
    pass


def parse_date_string(s):
    """Parse date string in YYYYMMDD format"""
    year, month, day = time.strptime(s, '%Y%m%d')[:3]
    return year, month, day


def check_args(args):
    """
    Validate that the dates, and date range are valid.
    """
    if args.startdate:
        y, m, d = parse_date_string(args.startdate)
        if y < 2016:
            raise ValidationError('No data before 2016')

    if args.enddate:
        y, m, d = parse_date_string(args.enddate)
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
    parser.add_argument(
        '--no-debug', action='store_false',
        help='Do not run in DEBUG mode')

    args = parser.parse_args()
    try:
        check_args(args)
    except (ValidationError, ValueError) as e:
        logger.error('Commandline argument(s) are wrong')
        raise

    return args


def get_batch_names_in_npr(npr_conn, startdate, enddate):
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
    batch_names = []
    rejected_batch_names = []
    for b in unvalidated_batchnames:
        try:
            y, m, d = parse_date_string(b)
        except ValueError:
            logger.info('Cannot parse %s as date - skipping.', b)
            rejected_batch_names.append(b)
        else:
            batch_names.append(b)

    # do optional filtering of this data set (TODO: move to SQL)
    if startdate is not None:
        batch_names = [b for b in batch_names if b >= startdate]
    if enddate is not None:
        batch_names = [b for b in batch_names if b <= enddate]

    return batch_names, rejected_batch_names


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
    dp_table = datapunt.get_backup_table_def(md)
    md.create_all(DP_ENGINE)

    # iterate of NPR selection, write to local database
    batch_size = 10 if DEBUG else BATCH_SIZE

    it = batched_selection_iterator(npr_conn, selection, batch_size, 0)
    for i, rows in enumerate(it):
        dp_conn.execute(dp_table.insert(), rows)

        if DEBUG:
            break


def get_backed_up_batches():
    """Get a list of days for which PG dumps are present"""
    logger.info('Checking the object store for existing back-ups.')
    container = 'parkeerrechten_pgdumps'
    contents = objectstore._get_full_container_list(container)

    # filenames for dumps: <BASENAME>_YYYYMMDD.dump
    pattern = '(?P<date>\d{8})_' + re.escape(BASENAME) + re.escape('.dump')
    regexp = re.compile(pattern)

    dates = []
    for object_ in contents:
        logger.debug('OBJECT_ In CONTENTS %s', object_['name'])
        m = regexp.match(object_['name'])

        try:
            parse_date_string(m.group('date'))
        except:
            continue
        else:
            dates.append(m.group('date'))
    logger.info('There are %d batches backed-up already', len(dates))
    return dates


def main(npr_conn):
    # Determine which batchnames we will be querying for:
    args = parse_args()

    if args.orphans:
        batch_names = ['Leeg']
    else:
        # What is available in the NPR database?
        npr_batch_names, _ = get_batch_names_in_npr(
            npr_conn, args.startdate, args.enddate)

        # What is backed-up already? Do not download again.
        backed_up = get_backed_up_batches()
        batch_names = list(set(npr_batch_names) ^ set(backed_up))
        batch_names.sort()
        logger.info('At most %d batches will backed up', N_DAYS_PER_RUN)
        logger.info('NPR batches still to be backed up: %s', batch_names)

    logger.info(
        'Backing up following batches: %s', batch_names[:N_DAYS_PER_RUN])

    for batch_name in batch_names[:N_DAYS_PER_RUN]:
        get_and_store_batch(npr_conn, dp_conn, batch_name)

    result = DP_ENGINE.execute(
        'select count(*) from "VW_0363_BACKUP"').fetchall()

    logger.info('There are now %s records in your back-up', result[0])


if __name__ == '__main__':
    # Establish needed database connections (Datapunt local, NPR remote):
    t0 = time.time()
    logger.info('Starting NPR parkeerrechten import script.')
    logger.info('Script was called with: %s', sys.argv)

    with NPR_ENGINE.connect() as npr_conn, DP_ENGINE.connect() as dp_conn:
        main(npr_conn)
    logger.info('Starting NPR parkeerrechten import script.')
    dt = time.time() - t0
    logger.info('The script took %.2f seconds to run', dt)

