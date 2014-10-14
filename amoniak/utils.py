from ast import literal_eval
import collections
from functools import partial
import logging
import os
import re

from amoniak import VERSION
from empowering import Empowering
import erppeek
import pymongo
import redis
from rq import Queue
from raven import Client
from raven.handlers.logging import SentryHandler


logger = logging.getLogger(__name__)


__REDIS_POOL = None
__FIRST_CAP_RE = re.compile('(.)([A-Z][a-z]+)')
__ALL_CAP_RE = re.compile('([a-z0-9])([A-Z])')


def lowercase(name):
    s1 = __FIRST_CAP_RE.sub(r'\1.\2', name)
    return __ALL_CAP_RE.sub(r'\1.\2', s1).lower()


class Popper(object):
    def __init__(self, items):
        self.items = list(items)

    def pop(self, n):
        res = []
        for x in xrange(0, min(n, len(self.items))):
            res.append(self.items.pop())
        return res


class PoolWrapper(object):
    def __init__(self, pool, cursor, uid):
        self.pool = pool
        self.cursor = cursor
        self.uid = uid

    def __getattr__(self, name):
        model = lowercase(name)
        return ModelWrapper(self.pool.get(model), self.cursor, self.uid)


class ModelWrapper(object):
    def __init__(self, model, cursor, uid):
        self.model = model
        self.cursor = cursor
        self.uid = uid

    def wrapper(self, method):
        return partial(method, self.cursor, self.uid)

    def __getattr__(self, item):
        base = getattr(self.model, item)
        if callable(base):
            return lambda *args: self.wrapper(base)(*args)
        else:
            return base


def recursive_update(d, u):
    for k, v in u.iteritems():
        if isinstance(v, collections.Mapping):
            r = recursive_update(d.get(k, {}), v)
            d[k] = r
        else:
            d[k] = u[k]
    return d


def env_eval(var):
    try:
        return literal_eval(var)
    except Exception:
        return var


def config_from_environment(env_prefix, env_required=None, **kwargs):
    config = kwargs.copy()
    prefix = '%s_' % env_prefix.upper()
    for env_key, value in os.environ.items():
        env_key = env_key.upper()
        if env_key.startswith(prefix):
            key = '_'.join(env_key.split('_')[1:]).lower()
            config[key] = env_eval(value)
    if env_required:
        for required in env_required:
            if required not in config:
                logger.error('You must pass %s or define env var %s%s' %
                             (required, prefix, required.upper()))
    logger.debug('Config for %s: %s' % (env_prefix, config))
    return config


def setup_peek(**kwargs):
    peek_config = config_from_environment('PEEK', ['server'], **kwargs)
    return erppeek.Client(**peek_config)


def setup_mongodb(**kwargs):
    config = config_from_environment('MONGODB', ['host', 'database'], **kwargs)
    mongo = pymongo.MongoClient(host=config['host'])
    return mongo[config['database']]


def setup_empowering_api(**kwargs):
    config = config_from_environment('EMPOWERING', ['company_id'], **kwargs)
    em = Empowering(**config)
    return em


def setup_redis():
    global __REDIS_POOL
    if not __REDIS_POOL:
        __REDIS_POOL = redis.ConnectionPool()
    r = redis.Redis(connection_pool=__REDIS_POOL)
    return r


def setup_queue(**kwargs):
    config = config_from_environment('RQ', **kwargs)
    config['connection'] = setup_redis()
    return Queue(**config)


def setup_logging(logfile=None):
    amon_logger = logging.getLogger('amon')
    if logfile:
        hdlr = logging.FileHandler(logfile)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        amon_logger.addHandler(hdlr)
    sentry = Client()
    sentry.tags_context({'version': VERSION})
    sentry_handler = SentryHandler(sentry, level=logging.ERROR)
    amon_logger.addHandler(sentry_handler)
    amon_logger.info('Amon logger setup')


def sorted_by_key(data, key, reverse=False):
    return sorted(data, key=lambda k: k[key], reverse=reverse)