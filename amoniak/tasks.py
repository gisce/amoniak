from datetime import datetime
import logging

from .utils import setup_peek, setup_mongodb, setup_empowering_api, setup_redis
from .amon import AmonConverter, check_response
import pymongo
from rq.decorators import job
from raven import Client


sentry = Client()
logger = logging.getLogger('amon')


@job('measures', connection=setup_redis(), timeout=3600)
@sentry.capture_exceptions
def push_amon_measures(measures_ids):
    """Pugem les mesures a l'Insight Engine
    """

    em = setup_empowering_api()
    O = setup_peek()
    amon = AmonConverter(O)
    start = datetime.now()
    mongo = setup_mongodb()
    collection = mongo['tg_billing']
    mdbprofiles = collection.find({'id': {'$in': measures_ids}},
                                  {'name': 1, 'id': 1, '_id': 0,
                                  'ai': 1, 'r1': 1, 'date_end': 1},
                                  sort=[('date_end', pymongo.ASCENDING)])
    profiles = [x for x in mdbprofiles]
    logger.info("Enviant de %s (id:%s) a %s (id:%s)" % (
        profiles[-1]['date_end'], profiles[-1]['id'],
        profiles[0]['date_end'], profiles[0]['id'],
    ))
    profiles_to_push = amon.profile_to_amon(profiles)
    stop = datetime.now()
    logger.info('Mesures transformades en %s' % (stop - start))
    start = datetime.now()
    measures = em.amon_measures().create(profiles_to_push)
    stop = datetime.now()
    logger.info('Mesures enviades en %s' % (stop - start))
    logger.info("%s measures creades" % len(measures))
    mongo.connection.disconnect()


@job('contracts', connection=setup_redis(), timeout=3600)
@sentry.capture_exceptions
def push_contracts(contracts_id):
    """Pugem els contractes
    """
    em = setup_empowering_api()
    O = setup_peek()
    amon = AmonConverter(O)
    if not isinstance(contracts_id, (list, tuple)):
        contracts_id = [contracts_id]
    for pol in O.GiscedataPolissa.read(contracts_id, ['modcontractuals_ids', 'name']):
        cid = pol['id']
        upd = []
        first = True
        for modcon_id in reversed(pol['modcontractuals_ids']):
            amon_data = amon.contract_to_amon(cid, {'modcon_id': modcon_id})[0]
            if first:
                response = em.contracts().create(amon_data)
                first = False
            else:
                etag = upd[-1]['_etag']
                response = em.contract(pol['name']).update(amon_data, etag)
            if check_response(response, amon_data):
                upd.append(response)
        update_etag.delay(cid, upd[-1])


@job('etag', connection=setup_redis())
@sentry.capture_exceptions
def update_etag(pol_id, resp):
    """Actualitzem l'etag.
    """
    O = setup_peek()
    etag = resp['_etag']
    logger.info("Polissa id: %s -> etag %s" % (pol_id, etag))
    O.GiscedataPolissa.write(pol_id, {'etag': etag})