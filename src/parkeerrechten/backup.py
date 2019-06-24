import logging

from sqlalchemy import select, asc, distinct, Table, MetaData
from sqlalchemy.exc import NoSuchTableError
from . import objectstore
from . import settings
from . import namecheck

LOG_FORMAT = '%(asctime)-15s - %(name)s - %(message)s'
logging.basicConfig(format=LOG_FORMAT, level=logging.DEBUG)
logger = logging.getLogger(__name__)


def get_batch_names_in_objectstore(include_leeg):
    """Get a list of days for which PG dumps are present"""
    logger.info('Checking the object store for existing back-ups.')

    contents = objectstore._get_full_container_list(
        settings.OBJECT_STORE_CONTAINER)
    batches = []
    for object_ in contents:
        if namecheck.is_batch_file(object_['name'], include_leeg=include_leeg):
            batches.append(namecheck.extract_batch_name(object_['name']))

    return batches


def get_batch_names_in_database(
        connection, table_or_view_name, include_leeg=False, require_table=True):
    """
    Query for all distinct batchnames in database (be it NPR, local or test).
    """
    # Introspect the database for table definition, define selection.
    md = MetaData()
    try:
        view = Table(
            table_or_view_name, md, autoload=True, autoload_with=connection)
    except NoSuchTableError:
        if require_table:
            raise
        else:
            # This happens when the local database is checked the first time
            # during the import process (which could entail several calls to
            # the run_import.py script). If the local database is not yet
            # initialized, there are no backed-up batches and we return an
            # empty list.
            return []

    selection = (
        select([distinct(view.c.VER_BATCH_NAAM)])
        .order_by(asc(view.c.VER_BATCH_NAAM))
    )

    # We expect maximum on the order of a few hundred days (for the NPR
    # database, the local database used during testing and importing
    # should have only on the order of tens of batches).
    unvalidated_batchnames = [
        row[0] for row in connection.execute(selection).fetchall()]

    # Validate that we have only dates as batch names.
    batch_names = namecheck.filter_batch_names(
        unvalidated_batchnames, include_leeg=include_leeg)

    return batch_names
