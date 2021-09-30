# -*- coding: utf-8 -*-
from __future__ import absolute_import
from datetime import datetime
from dateutil.relativedelta import relativedelta
import logging
import urllib2

import libsaas

from .utils import (
    setup_peek, setup_empowering_api, setup_redis,
    sorted_by_key, Popper, setup_queue
)
from .amon import AmonConverter, check_response
from rq.decorators import job
from raven import Client
from empowering.utils import make_local_timestamp


sentry = Client()
logger = logging.getLogger('amon')


def enqueue_tariffs(tariffs=None):
    c = setup_peek()
    search_params = []
    if tariffs:
        search_params.append(('name', 'in', tariffs))
    tids = c.ProductPricelist.search(search_params)
    to_q = []
    for pricelist in c.ProductPricelist.browse(tids):
        for tatr in pricelist.tarifes_atr_compatibles:
            n_polisses = c.GiscedataPolissa.search_count([
                ('tarifa.id', '=', tatr.id),
                ('llista_preu.id', '=', pricelist.id),
                ('etag', '!=', False)
            ], context={'active_test': False})
            t = (pricelist.id, tatr.id)
            if n_polisses and t not in to_q:
                logger.info('Enqueuing %s - %s', pricelist.name, tatr.name)
                to_q.append(t)
                push_tariffs.delay(t)


def enqueue_profiles(bucket=500, contracts=None, force=False):
    # First get all the contracts that are in sync
    c = setup_peek()
    # TODO: Que fem amb les de baixa? les agafem igualment? només les que
    # TODO: faci menys de X que estan donades de baixa?
    search_params = [('etag', '!=', False)]
    if contracts:
        search_params.append(('name', 'in', contracts))
    pids = c.GiscedataPolissa.search(search_params)
    fields_to_read = ['name', 'cups', 'empowering_last_profile_measure']
    for polissa in c.GiscedataPolissa.read(pids, fields_to_read):
        last_measure = polissa.get('empowering_last_profile_measure')
        cups = polissa['cups'][1]
        if not last_measure or force:
            logger.info("Les pugem totes")
            from_date = (
                datetime.now() - relativedelta(years=3)
            ).strftime('%Y-%m-%d 01:00:00')
            logger.info(u"Pujant un any màxim: %s" % from_date)
        else:
            logger.info(u"Última lectura trobada: %s" % last_measure)
            from_date = last_measure
        # Use TM also
        for collection in ['tg.cchfact', 'tg.f1']:
            model = c.model(collection)
            measures = model.search([
                ('name', '=', cups),
                ('datetime', '>=', from_date)
            ])
            logger.info("S'han trobat %s mesures (%s) per pujar", 
                len(measures), collection
            )
            popper = Popper(measures)
            pops = popper.pop(bucket)
            while pops:
                j = push_amon_profiles.delay(pops, collection)
                logger.info("Job id:%s | %s/%s/%s" % (
                    j.id, polissa['name'], len(pops), len(popper.items))
                )
                pops = popper.pop(bucket)


def enqueue_measures(bucket=500, contracts=None, force=False):
    # First get all the contracts that are in sync
    c = setup_peek()
    # TODO: Que fem amb les de baixa? les agafem igualment? només les que
    # TODO: faci menys de X que estan donades de baixa?
    search_params = [('etag', '!=', False)]
    if contracts:
        search_params.append(('name', 'in', contracts))
    pids = c.GiscedataPolissa.search(search_params)
    # Comptadors que tingui aquesta pòlissa i que siguin de telegestió
    cids = c.GiscedataLecturesComptador.search([
        ('polissa', 'in', pids)
    ], context={'active_test': False})
    fields_to_read = ['name', 'empowering_last_measure']
    for comptador in c.GiscedataLecturesComptador.read(cids, fields_to_read):
        last_measure = comptador.get('empowering_last_measure')
        if not last_measure or force:
            # Pujar totes
            logger.info("Les pugem totes")
            from_date = (
                datetime.now() - relativedelta(years=1)
            ).strftime('%Y-%m-%d')
            logger.info(u"Pujant un any màxim: %s" % from_date)
        else:
            logger.info(u"Última lectura trobada: %s" % last_measure)
            from_date = last_measure
        measures = c.GiscedataLecturesComptador.get_aggregated_measures(
            [comptador['id']], from_date
        )
        logger.info("S'han trobat %s mesures per pujar" % (
            len(measures)
        ))
        popper = Popper(measures)
        pops = popper.pop(bucket)
        while pops:
            j = push_amon_measures.delay(pops)
            logger.info("Job id:%s | %s/%s/%s" % (
                j.id, comptador['name'], len(pops), len(popper.items))
            )
            pops = popper.pop(bucket)


def enqueue_new_contracts(bucket=1, force=False):
    search_params = [
        ('etag', '=', False),
        ('state', 'not in', ('esborrany', 'validar', 'cancelada'))
    ]
    if not force:
        with setup_empowering_api() as em:
            items = em.contracts().get(sort="[('_updated', -1)]")['_items']
            if items:
                from_date = make_local_timestamp(items[0]['_updated'])
                search_params += [
                        '|',
                        ('create_date', '>', from_date),
                        ('write_date', '>', from_date)
                ]
    O = setup_peek()
    contracts_ids = O.GiscedataPolissa.search(search_params)
    logger.info('Found %s contracts to push', len(contracts_ids))
    popper = Popper(contracts_ids)
    pops = popper.pop(bucket)
    while pops:
        j = push_contracts.delay(pops)
        logger.info("Job id:%s" % j.id)
        pops = popper.pop(bucket)


def enqueue_contracts(contracts=None, force=False):
    O = setup_peek()
    # Busquem els que hem d'actualitzar
    if contracts is None:
        polisses_ids = O.GiscedataPolissa.search([('etag', '!=', False)])
    else:
        polisses_ids = O.GiscedataPolissa.search([
            ('name', 'in', contracts)
        ], context={'active_test': False})
    if not polisses_ids:
        logger.info('No contracts found')
        return
    fields_to_read = ['name', 'etag', 'comptadors', 'modcontractual_activa']
    if force:
        logger.info('Forcing pushing {} contracts'.format(len(polisses_ids)))
        for polissa_id in polisses_ids:
            push_contracts.delay([polissa_id])
        return
    for polissa in O.GiscedataPolissa.read(polisses_ids, fields_to_read):
        modcons = []
        is_new_contract = False
        try:
            with setup_empowering_api() as em:
                last_updated = em.contract(polissa['name']).get()['_updated']
                last_updated = make_local_timestamp(last_updated)
        except (libsaas.http.HTTPError, urllib2.HTTPError) as e:
            # A 404 is possible if we delete empowering contracts in insight engine
            # but keep etag in our database.
            # In this case we must force the re-upload as new contract
            if e.code != 404:
                raise e
            is_new_contract = True
            last_updated = '0'

        w_date = O.GiscedataPolissa.perm_read(polissa['id'])[0]['write_date']
        c_w_dates = []
        for comp_perm in O.GiscedataLecturesComptador.perm_read(polissa['comptadors']):
            c_w_dates.append(comp_perm['write_date'])
        c_w_date = max(c_w_dates)
        if w_date > last_updated and not is_new_contract:
            # Ara mirem quines modificaciones contractuals hem de pujar
            polissa = O.GiscedataPolissa.browse(polissa['id'])
            for modcon in polissa.modcontractuals_ids:
                perms = modcon.perm_read()
                if perms['write_date'] > last_updated:
                    logger.info('La modcontractual %s a actualitzar write_'
                                'date: %s last_update: %s' % (
                        modcon.name, perms['write_date'], last_updated))
                    modcons.append(modcon.id)
        elif c_w_date > last_updated and not is_new_contract:
            # Si no hi ha hagut canvis a les modificacions contractuals
            # però sí que s'ha tocat algun comptador fem una actualització
            # amb la última modificació contractual
            modcons.append(polissa['modcontractual_activa'][0])
        if modcons:
            logger.info('Polissa %s actualitzada a %s després de %s' % (
                polissa.name, w_date, last_updated))
            push_contracts.delay([polissa['id']])
        if is_new_contract:
            logger.info("La polissa %s te etag pero ha estat borrada "
                        "d'empowering, es torna a pujar" % polissa['name'])
            push_contracts.delay([polissa['id']])


def enqueue_indexed(bucket=1, force=False, wreport=False):
    """Busquem els grups indexats formats per llista preu + FEE que coincideixin
    Recuperem l'ultima data publicada per saber d'es d'on pujar
    Si l'agrupació no s'ha pujat mai, busquem factures i pujem desde la data més petita
    force = True: Força pujada
    wreport = True: Crea un report al director /tmp amb els preus mitjos horaris per agrupació
    """
    O = setup_peek()
    indexed_grouppeds = {}
    pids = O.GiscedataPolissa.search([('mode_facturacio', '=', 'index')])
    if wreport:
        import pandas as pd
        writer = pd.ExcelWriter('/tmp/beedata_indexed_{}.xlsx'.format(datetime.now()))
    for pol in O.GiscedataPolissa.read(pids, ['llista_preu', 'coeficient_d', 'coeficient_k', 'name', 'tarifa']):
        fee = pol['coeficient_d'] + pol['coeficient_k']
        llprice = pol['llista_preu'][1]
        tarifa = pol['tarifa'][1]
        key = (tarifa, '{} - {}'.format(llprice, fee))
        if key not in indexed_grouppeds:
            indexed_grouppeds[key] = [pol['id']]
        else:
            indexed_grouppeds[key].append(pol['id'])
    for group_key, contracts in indexed_grouppeds.items():
        # Search last indexed publish date
        tariff_id, cost = group_key
        pindexed_id = O.EmpoweringPriceIndexed.search(
            [('tariff_id', '=', str(tariff_id)), ('tariff_cost_id', '=', str(cost))]
        )
        if pindexed_id:
            ldate = O.EmpoweringPriceIndexed.read(
                pindexed_id[0], ['empowering_price_indexed_last_push']
            )['empowering_price_indexed_last_push']
            logger.info('Grup indexats %s, pujem desde ultima data %s', group_key, ldate)
        else:
            # todo: if not exists, which date??
            dta = datetime.now()
            ldate = '{}-{}-01'.format(dta.year, str(dta.month).zfill(2))
        logger.info('Found %s indexed group to push from %s', group_key, ldate)
        fact_ids = []
        for pol_id in contracts:
            fact_ids += O.GiscedataFacturacioFactura.search([
                ('polissa_id', '=', pol_id),
                ('data_inici', '>', ldate),
                ('type', '=', 'out_invoice'),
                ('llista_preu.name', '=like', '%ndex%')
            ])
        if fact_ids:
            logger.info('Pushing %s indexed group with #facts %s', group_key, len(fact_ids))
            push_indexeds.delay((group_key, fact_ids))

@job(setup_queue(name='measures'), connection=setup_redis(), timeout=3600)
@sentry.capture_exceptions
def push_amon_measures(measures):
    """Pugem les mesures a l'Insight Engine
    """
    logging.basicConfig(level=logging.INFO)
    with setup_empowering_api() as em:
        c = setup_peek()
        amon = AmonConverter(c)
        start = datetime.now()
        measures_to_push = amon.aggregated_measures_to_amon(measures)
        logger.info("Enviant de %s (id:%s) a %s (id:%s)" % (
            measures[-1]['timestamp'], measures[-1]['meter_id'],
            measures[0]['timestamp'], measures[0]['meter_id']
        ))
        stop = datetime.now()
        logger.info('Mesures transformades en %s' % (stop - start))
        start = datetime.now()
        # Check which endpoint to use
        residential = measures_to_push.get('R')
        if residential:
            logger.debug('Pushing %s', residential)
            em.residential_timeofuse_amon_measures().create(residential)
        tertiary = measures_to_push.get('T')
        if tertiary:
            logger.debug('Pushing %s', residential)
            em.tertiary_amon_measures().create(tertiary)
        # Save last timestamp
        last_measure = measures[0]
        c.GiscedataLecturesComptador.update_empowering_last_measure(
            [last_measure['meter_id']], last_measure['timestamp']
        )
        stop = datetime.now()
        logger.info('Mesures enviades en %s' % (stop - start))
        logger.info("%s measures creades" % len(measures))


@job(setup_queue(name='profiles'), connection=setup_redis(), timeout=3600)
@sentry.capture_exceptions
def push_amon_profiles(profiles, collection):
    """Pugem les mesures a l'Insight Engine
    """
    with setup_empowering_api() as em:
        c = setup_peek()
        amon = AmonConverter(c)
        measures_to_push = amon.profiles_to_amon(profiles, collection)
        for cups, m_to_push in measures_to_push.items():
            em.amon_measures().create(m_to_push)
            last_measure = max(
                make_local_timestamp(x['timestamp'])
                for x in m_to_push['measurements']
            )
            pol_id = c.GiscedataPolissa.search([
                ('cups.name', '=', cups),
                ('state', 'not in', ('esborrany', 'validar', 'cancelada', 'baixa')),
                ('data_alta', '<=', last_measure),
                '|',
                ('data_baixa', '>=', last_measure),
                ('data_baixa', '=', False)
            ], context={'active_test': False})
            if not pol_id:
                continue
            if len(pol_id) > 1:
                raise Exception('{} contracts found! CUPS: {}. Last measure: {}'.format(len(pol_id), cups, last_measure))
            pol = c.GiscedataPolissa.read(pol_id[0], ['name', 'empowering_last_profile_measure'])
            if last_measure > pol['empowering_last_profile_measure']:
                logger.info('Updating polissa (id: %s) to last measure: %s', pol['name'], last_measure)
                c.GiscedataPolissa.write(pol_id, {
                    'empowering_last_profile_measure': last_measure
                })


@job(setup_queue(name='contracts'), connection=setup_redis(), timeout=3600)
@sentry.capture_exceptions
def push_modcontracts(modcons, etag):
    """modcons is a list of modcons to push
    """
    em = setup_empowering_api()
    O = setup_peek()
    amon = AmonConverter(O)
    fields_to_read = ['data_inici', 'polissa_id']
    modcons = O.GiscedataPolissaModcontractual.read(modcons, fields_to_read)
    modcons = sorted_by_key(modcons, 'data_inici')
    for modcon in modcons:
        amon_data = amon.contract_to_amon(
            modcon['polissa_id'][0],
            {'modcon_id': modcon['id']}
        )[0]
        response = em.contract(modcon['polissa_id'][1]).update(amon_data, etag)
        if check_response(response, amon_data):
            etag = response['_etag']
    O.GiscedataPolissa.write(modcon['polissa_id'][0], {'etag': etag})


@job(setup_queue(name='contracts'), connection=setup_redis(), timeout=3600)
@sentry.capture_exceptions
def push_contracts(contracts_id):
    """Pugem els contractes
    """
    import logging
    logging.basicConfig(level=logging.INFO)
    with setup_empowering_api() as em:
        O = setup_peek()
        amon = AmonConverter(O)
        if not isinstance(contracts_id, (list, tuple)):
            contracts_id = [contracts_id]
        for pol in O.GiscedataPolissa.read(contracts_id, ['name', 'etag']):
            amon_data = amon.contract_to_amon(pol['id'])[0]
            try:
                if pol['etag']:
                    response = em.contract(pol['name']).update(
                        amon_data, pol['etag']
                    )
                else:
                    response = em.contracts().create(amon_data)
            except urllib2.HTTPError as err:
                raise Exception('HTTPError code {}. Error: {}'.format(err.code, err.read())) 
            O.GiscedataPolissa.write([pol['id']], {'etag': response['_etag']})


@job(setup_queue(name='tariffs'), connection=setup_redis(), timeout=3600)
@sentry.capture_exceptions
def push_tariffs(tariffs):
    c = setup_peek()
    a = AmonConverter(c)
    result = a.tariff_to_amon(*tariffs)
    with setup_empowering_api() as em:
        try:
            print(result)
            em.tariffs().create(result)
        except urllib2.HTTPError as err:
            print(err.read())
            raise

@job(setup_queue(name='indexeds'), connection=setup_redis(), timeout=3600)
@sentry.capture_exceptions
def push_indexeds(indexeds):
    c = setup_peek()
    a = AmonConverter(c)
    result = a.indexed_to_amon(*indexeds)
    with setup_empowering_api() as em:
        try:
            print(result)
            response = em.price_indexed().create(result)
            if response['_status'] == 'OK':
                etag = response['_etag']
                tid = result[0]['tariffId']
                cid = result[0]['cost']
                epid = c.EmpoweringPriceIndexed.search([
                    ('tariffId', '=', tid), ('tariffCostId', '=', cid)
                ])
                ldate = max([x.datetime for x in result])
                if epid:
                    c.EmpoweringPriceIndexed.write(epid, {
                        'empowering_price_indexed_last_push': ldate,
                        'etag': etag
                    })
                else:
                    c.EmpoweringPriceIndexed.create({
                        'tariffId': tid,
                        'tariffCostId': cid,
                        'empowering_price_indexed_last_push': ldate,
                        'etag': etag
                    })
        except urllib2.HTTPError as err:
            print(err.read())
            raise
