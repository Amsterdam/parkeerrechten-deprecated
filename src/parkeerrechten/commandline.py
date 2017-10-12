"""
Command line handling.
"""
import argparse
import logging

from . import namecheck


LOG_FORMAT = '%(asctime)-15s - %(name)s - %(message)s'
logging.basicConfig(format=LOG_FORMAT, level=logging.DEBUG)
logger = logging.getLogger(__name__)


class ValidationError(Exception):
    pass


def check_date_args(args):
    """
    Validate that the dates, and date range are valid.
    """
    if args.startdate:
        y, m, d = namecheck.parse_date_string(args.startdate)
        if y < 2016:
            raise ValidationError('No data before 2016')

    if args.enddate:
        y, m, d = namecheck.parse_date_string(args.enddate)
        if y < 2016:
            raise ValidationError('No data before 2016')

    if args.startdate and args.enddate:
        if args.startdate > args.enddate:
            raise ValidationError('startdate cannot be after enddate')


def parse_args(raw_args):
    """
    Parse the commandline options to this script.
    """
    # Set up parser.
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

    # Parse command line arguments, check their values.
    args = parser.parse_args(raw_args)
    try:
        check_date_args(args)
    except(ValidationError, ValueError) as e:
        logger.error('Commandline argument(s) are wrong')
        raise e

    return args
