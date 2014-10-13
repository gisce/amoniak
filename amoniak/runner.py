#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import logging
import sys
import time

from empowering.utils import make_uuid, make_local_timestamp

from amoniak.tasks import push_amon_measures, push_contracts
from amoniak.utils import Popper, setup_mongodb, setup_peek, setup_logging, setup_empowering_api


if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO)
    setup_logging()
    logger = logging.getLogger('amon')

    if sys.argv[1] == 'push_all_amon_measures':
        bucket = 500
        serials = open('serials', 'r')
        for serial in serials:
            meter_name = serial.replace('\n', '').strip()
            if not meter_name.startswith('ZIV00'):
                continue
            filters = {
                'name': meter_name,
                'type': 'day',
                'value': 'a',
                'valid': True,
                'period': 0
            }
            mongo = setup_mongodb()
            collection = mongo['tg_billing']
            measures = collection.find(filters, {'id': 1})
            measures_to_push = []
            for idx, measure in enumerate(measures):
                if idx and not idx % bucket:
                    j = push_amon_measures.delay(measures_to_push)
                    logger.info("Job id:%s | %s/%s/%s" % (
                        j.id, meter_name, idx, bucket)
                    )
                    time.sleep(0.1)
                    measures_to_push = []
                measures_to_push.append(measure['id'])
        mongo.connection.disconnect()

    elif sys.argv[1] == "push_measures":
        bucket = 500
        # First get all the contracts that are in sync
        O = setup_peek()
        em = setup_empowering_api()
        # TODO: Que fem amb les de baixa? les agafem igualment? només les que
        # TODO: faci menys de X que estan donades de baixa?
        pids = O.GiscedataPolissa.search([('etag', '!=', False)])
        # Comptadors que tingui aquesta pòlissa i que siguin de telegestió
        cids = O.GiscedataLecturesComptador.search([
            ('tg', '=', 1),
            ('polissa', 'in', pids)
        ], context={'active_test': False})
        for comptador in O.GiscedataLecturesComptador.read(cids, ['name']):
            tg_name = O.GiscedataLecturesComptador.build_name_tg(comptador['id'])
            deviceId = make_uuid('giscedata.lectures.comptador', tg_name)
            logger.info("Buscant l'última lectura pel comptador: %s "
                        "device_id %s" % (tg_name, deviceId))
            res = em.amon_measures_measurements().get(where='"deviceId"=="%s"' % deviceId, sort='[("timestamp", -1)]')['_items']
            search_params = [
                ('name', '=', tg_name),
                ('type', '=', 'day'),
                ('value', '=', 'a'),
                ('valid', '=', 1),
                ('period', '=',  0)
            ]
            if not res:
                # Pujar totes
                logger.info("Les pugem totes")
            else:
                res = res[0]
                local_ts = make_local_timestamp(res['timestamp'])
                logger.info(u"Última lectura trobada: %s" % local_ts)
                search_params.append(('date_end', '>', local_ts))
            measures_ids = O.TgBilling.search(search_params, limit=0, order="date_end asc")
            logger.info("S'han trobat %s mesures per pujar" % len(measures_ids))
            popper = Popper(measures_ids)
            pops = popper.pop(bucket)
            while pops:
                j = push_amon_measures.delay(pops)
                logger.info("Job id:%s | %s/%s/%s" % (
                    j.id, tg_name, len(pops), len(popper.items))
                )
                pops = popper.pop(bucket)

    elif sys.argv[1] == 'push_all_contracts':
        O = setup_peek()
        cids = O.GiscedataLecturesComptador.search([('tg', '=', 1)], 0, 0, False, {'active_test': False})
        contracts_ids = [
            x['polissa'][0]
            for x in O.GiscedataLecturesComptador.read(cids, ['polissa'])
        ]
        contracts_ids = list(set(contracts_ids))
        contracts_ids = O.GiscedataPolissa.search([
            ('id', 'in', contracts_ids),
            ('state', 'not in', ('esborrany', 'validar'))
        ])
        popper = Popper(contracts_ids)
        bucket = 500
        pops = popper.pop(bucket)
        while pops:
            j = push_contracts.delay(pops)
            logger.info("Job id:%s" % j.id) 
            pops = popper.pop(bucket)
