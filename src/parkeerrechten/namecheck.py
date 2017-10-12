"""
Deal with batch names (in backup and on NPR).
"""
import logging
import time

from . settings import BACKUP_FILE_BASENAME

LOG_FORMAT = '%(asctime)-15s - %(name)s - %(message)s'
logging.basicConfig(format=LOG_FORMAT, level=logging.DEBUG)
logger = logging.getLogger(__name__)


def parse_date_string(s):
    """Parse date string in YYYYMMDD format"""
    year, month, day = time.strptime(s, '%Y%m%d')[:3]
    return year, month, day


def is_batch_name(batch_name, include_leeg):
    """
    Check whether backed-up file name matches expectation

    Note, accepted batch_names are either 'Leeg' or dates in YYYYMMDD format.
    """
    if include_leeg and batch_name == 'Leeg':
        return True

    try:
        parse_date_string(batch_name)
    except:
        return False
    else:
        return True


def filter_batch_names(batch_names, include_leeg):
    """Filter list of potential batch names, accept only valid ones """
    return [bn for bn in batch_names if is_batch_name(bn, include_leeg)]


def filter_batch_names_by_date(batch_names, start_date, end_date):
    """Filter list of batch names, accept only dates in specified range"""
    # TODO: reconsider what to do with 'Leeg', for now this function does
    # not accept it.
    if start_date is not None:
        batch_names = [b for b in batch_names if b >= start_date]
    if end_date is not None:
        batch_names = [b for b in batch_names if b <= end_date]

    return batch_names


def is_batch_file(file_name, include_leeg):
    """
    Retun True if batch file name is valid.
    """
    # TODO: name of function is wrong, refactor or rename!
    end = '_' + BACKUP_FILE_BASENAME + '.dump'
    if file_name.endswith(end):
        return is_batch_name(file_name[:-len(end)], include_leeg)
    else:
        return False


def extract_batch_name(file_name):
    """Extract batch name from a filename (only call with valid file names"""
    end = '_' + BACKUP_FILE_BASENAME + '.dump'
    return file_name[:-len(end)]
