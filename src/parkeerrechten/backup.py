import logging

from . import objectstore
from .namecheck import is_batch_file, extract_batch_name
from . import settings

LOG_FORMAT = '%(asctime)-15s - %(name)s - %(message)s'
logging.basicConfig(format=LOG_FORMAT, level=logging.DEBUG)
logger = logging.getLogger(__name__)


def get_backed_up_batches(include_leeg):
    """Get a list of days for which PG dumps are present"""
    logger.info('Checking the object store for existing back-ups.')

    contents = objectstore._get_full_container_list(
        settings.OBJECT_STORE_CONTAINER)
    batches = []
    for object_ in contents:
        if is_batch_file(object_['name'], include_leeg=include_leeg):
            batches.append(extract_batch_name(object_['name']))

    return batches
