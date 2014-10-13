import logging

from .utils import setup_redis
from modeldict import RedisDict


logger = logging.getLogger('amon')


CUPS_CACHE = RedisDict('CUPS_CACHE', setup_redis())
CUPS_UUIDS = RedisDict('CUPS_UUIDS', setup_redis())


def empty():
    logger.debug('Emptying cache...')
    for k in CUPS_CACHE:
        del CUPS_CACHE[k]
    for k in CUPS_UUIDS:
        del CUPS_UUIDS[k]
    logger.debug('Cache emptied!')
