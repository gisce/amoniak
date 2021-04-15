#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from hashlib import sha1
from copy import deepcopy
from datetime import datetime, timedelta
from pytz import timezone
import json
import logging

from .cache import CUPS_CACHE, CUPS_UUIDS
from .utils import recursive_update, reduce_history, is_tertiary
from empowering.utils import remove_none, make_uuid, make_utc_timestamp


UNITS = {1: '', 1000: 'k'}

COLLECTION_UNITS = {
    'tg.cchfact': 'Wh',
    'tg.f1': 'kWh'
}

TZ = timezone('Europe/Madrid')

logger = logging.getLogger('amon')


def get_device_serial(device_id):
    return device_id[5:].lstrip('0')


def get_street_name(cups):
    street = []
    street_name = u''
    if cups['cpo'] or cups['cpa']:
        street = u'CPO %s CPA %s' % (cups['cpo'], cups['cpa'])
    else:
        if cups['tv']:
            street.append(cups['tv'][1])
        if cups['nv']:
            street.append(cups['nv'])
        street_name += u' '.join(street)
        street = [street_name]
        for f_name, f in [(u'número', 'pnp'), (u'escalera', 'es'),
                          (u'planta', 'pt'), (u'puerta', 'pu')]:
            val = cups.get(f, '')
            if val:
                street.append(u'%s %s' % (f_name, val))
    street_name = ', '.join(street)
    return street_name


def map_datetime(raw_timestamp):
    date, nhour = raw_timestamp.split(' ')
    current_date = TZ.localize(datetime.strptime(date, '%Y-%m-%d'))
    current_date = TZ.normalize(current_date + timedelta(hours=int(nhour)))
    return make_utc_timestamp(datetime.fromtimestamp(current_date.timestamp()))



class AmonConverter(object):
    def __init__(self, connection):
        self.O = connection

    def get_cups_from_device(self, serial):
        O = self.O
        # Remove brand prefix and right zeros
        if serial in CUPS_CACHE:
            return CUPS_CACHE[serial]
        else:
            # Search de meter
            cid = O.GiscedataLecturesComptador.search([
                ('name', '=', serial)
            ], context={'active_test': False})
            if not cid:
                res = False
            else:
                cid = O.GiscedataLecturesComptador.browse(cid[0])
                res = make_uuid('giscedata.cups.ps', cid.polissa.cups.name)
                CUPS_UUIDS[res] = cid.polissa.cups.id
                CUPS_CACHE[serial] = res
            return res

    def tariff_to_amon(self, pricelist_id, tariff_id):
        c = self.O
        tariff = c.GiscedataPolissaTarifa.read(tariff_id, ['name'])
        pricelist = c.ProductPricelist.browse(pricelist_id)
        date_start = None
        date_end = None
        for v in pricelist.version_id:
            if v.date_start:
                if not date_start:
                    date_start = v.date_start
                else:
                    date_start = min(date_start, v.date_start)
            if v.date_end:
                if not date_end:
                    date_end = v.date_end
                else:
                    date_end = max(date_end, v.date_end)
            else:
                date_end = None
        price_date = datetime.now().strftime('%Y-%m-%d')
        result = {
            'tariffCostId': '{} ({})'.format(
                pricelist.name, pricelist.currency_id.name
            ),
            'tariffId': tariff['name'],
            'dateStart': date_start and make_utc_timestamp(date_start),
            'dateEnd': date_end and make_utc_timestamp(date_end),
            'powerPrice': c.GiscedataPolissaTarifa.get_periodes_preus(
                tariff_id, 'tp', pricelist_id, {'date': price_date}
            ).values(),
            'energyPrice': c.GiscedataPolissaTarifa.get_periodes_preus(
                tariff_id, 'te', pricelist_id, {'date': price_date}
            ).values()
        }
        return result

    def profiles_to_amon(self, profiles, collection='tg.cchfact'):
        c = self.O
        result = {}
        # TODO: We need a hack to convert meter serial to CUPS uuid
        # maybe we can have a global uuids cache pre-generated for that
        uuids = {}
        model = c.model(collection)
        for profile in model.read(profiles):
            m_point_id = uuids.get(profile['name'])
            if not m_point_id:
                m_point_id = make_uuid('giscedata.cups.ps', profile['name'])
                uuids[profile['name']] = m_point_id
            result.setdefault(profile['name'], {
                "measurements": [],
                "meteringPointId": m_point_id,
                "readings": [
                    {"type": "electricityConsumption", "period": "INSTANT",
                     "unit": COLLECTION_UNITS[collection]},
                ],
                "deviceId": m_point_id
            })
            result[profile['name']]['measurements'] += [
                {
                    "timestamp": make_utc_timestamp(profile['datetime']),
                    "type": "electricityConsumption",
                    "value": profile['ai']
                }
            ]
        return result

    def aggregated_measures_to_amon(self, measures):
        res = {'R': [], 'T': []}

        for m in deepcopy(measures):
            values = {}
            for agg in m['measures']:
                t = agg.pop('tipus')
                values.setdefault(t, {})
                values[t].update(agg)

            measurements = {
                'A': {
                    'timestamp': make_utc_timestamp(m['timestamp']),
                    'type': m['resource'] == 'R' and 'touElectricityConsumption' or 'tertiaryElectricityConsumption',
                    'values': values.get('A')
                },
                'R': {
                    'timestamp': make_utc_timestamp(m['timestamp']),
                    'type': m['resource'] == 'R' and 'touElectricityKiloVoltAmpHours' or 'tertiaryElectricityKiloVoltAmpHours',
                    'values': values.get('R')
                },
                'P': {
                    'timestamp': make_utc_timestamp(m['timestamp']),
                    'type': m['resource'] == 'R' and 'touPower' or 'tertiaryPower',
                    'values': values.get('P')
                }
            }
            deviceId = make_uuid('giscedata.cups.ps', m['cups'])
            readings = []
            if measurements['A']['values']:
                readings.append({
                    "type": measurements['A']['type'],
                    "unit": "kWh",
                    "period": "INSTANT",
                })
            if measurements['R']['values']:
                readings.append({
                    "type": measurements['R']['type'],
                    "unit": "kVArh",
                    "period": "INSTANT",
                })
            if measurements['P']['values']:
                readings.append({
                    "type": measurements['P']['type'],
                    "unit": "kW",
                    "period": "INSTANT",
                })
            res[m['resource']].append({
                'deviceId': deviceId,
                'meteringPointId': deviceId,
                'readings': readings,
                'measurements': [v for v in measurements.values() if v['values']]
            })
        return res

    def power_measure_to_amon(self, measures):
        """Return a list of AMON readinds.

        {
            "utilityId": "Utility Id",
            "deviceId": "c1810810-0381-012d-25a8-0017f2cd3574",
            "meteringPointId": "c1759810-90f3-012e-0404-34159e211070",
            "readings": [
                {
                    "type_": "electricityConsumption",
                    "unit": "kWh",
                    "period": "INSTANT",
                },
                {
                    "type_": "electricityKiloVoltAmpHours",
                    "unit": "kVArh",
                    "period": "INSTANT",
                }
            ],
            "measurements": [
                {
                    "type_": "electricityConsumption",
                    "timestamp": "2010-07-02T11:39:09Z", # UTC
                    "value": 7
                },
                {
                    "type_": "electricityKiloVoltAmpHours",
                    "timestamp": "2010-07-02T11:44:09Z", # UTC
                    "value": 6
                }
            ]
        }
        """
        res = []
        if not hasattr(measures, '__iter__'):
            measures = [measures]

        for measure in measures:
            mp_uuid = make_uuid(
                'giscedata.cups.ps', measure.comptador.polissa.cups.name
            )
            device_uuid = make_uuid(
                'giscedata.lectures.comptador', measure.comptador.id
            )
            readings = []
            if measure.period.tarifa.name.startswith('2'):
                # measurements of 2.X
                readings += [{
                    "type":  "touPower",
                    "unit": "%sW" % UNITS[measure.get('magn', 1000)],
                    "period": "INSTANT",
                }]
            else:
                # tertiaryMeasurements
                readings += [{
                    "type": "tertiaryPower",
                    "unit": "%sW" % UNITS[measure.get('magn', 1000)],
                    "period": "INSTANT",
                }]

            res.append({
                "deviceId": device_uuid,
                "meteringPointId": mp_uuid,
                "readings": readings,
                "measurements": [
                    {
                        "type": readings[0]["type"],
                        "timestamp": make_utc_timestamp(measure.name),
                        "values": {
                            measure.periode.name: float(measure.lectura)
                        }
                    }
                ]
            })
        return res

    def energy_measure_to_amon(self, measures):
        """Return a list of AMON readinds.

        {
            "utilityId": "Utility Id",
            "deviceId": "c1810810-0381-012d-25a8-0017f2cd3574",
            "meteringPointId": "c1759810-90f3-012e-0404-34159e211070",
            "readings": [
                {
                    "type_": "electricityConsumption",
                    "unit": "kWh",
                    "period": "INSTANT",
                },
                {
                    "type_": "electricityKiloVoltAmpHours",
                    "unit": "kVArh",
                    "period": "INSTANT",
                }
            ],
            "measurements": [
                {
                    "type_": "electricityConsumption",
                    "timestamp": "2010-07-02T11:39:09Z", # UTC
                    "value": 7
                },
                {
                    "type_": "electricityKiloVoltAmpHours",
                    "timestamp": "2010-07-02T11:44:09Z", # UTC
                    "value": 6
                }
            ]
        }
        """
        res = []
        if not hasattr(measures, '__iter__'):
            measures = [measures]

        for measure in measures:
            mp_uuid = make_uuid(
                'giscedata.cups.ps', measure.comptador.polissa.cups.name
            )
            device_uuid = make_uuid(
                'giscedata.lectures.comptador', measure.comptador.id
            )
            readings = []
            if measure.period.tarifa.name.startswith('2'):
                # measurements of 2.X
                if measure.tipus == 'A':
                    readings += [{
                        "type":  "touElectricityConsumption",
                        "unit": "%sWh" % UNITS[measure.get('magn', 1000)],
                        "period": "INSTANT",
                    }]
                elif measure.tipus == 'R':
                    readings += [{
                        "type": "touElectricityKiloVoltAmpHours",
                        "unit": "%sVArh" % UNITS[measure.get('magn', 1000)],
                        "period": "INSTANT",
                    }]
            else:
                # tertiaryMeasurements
                if measure.tipus == 'A':
                    readings += [{
                        "type": "tertiaryElectricityConsumption",
                        "unit": "%sWh" % UNITS[measure.get('magn', 1000)],
                        "period": "INSTANT",
                    }]
                elif measure.tipus == 'R':
                    readings += [{
                        "type": "tertiaryElectricityKiloVoltAmpHours",
                        "unit": "%sVArh" % UNITS[measure.get('magn', 1000)],
                        "period": "INSTANT",
                    }]

            res.append({
                "deviceId": device_uuid,
                "meteringPointId": mp_uuid,
                "readings": readings,
                "measurements": [
                    {
                        "type": readings[0]["type"],
                        "timestamp": make_utc_timestamp(measure.name),
                        "value": float(measure.consum)
                    }
                ]
            })
        return res

    def contract_to_amon(self, contract_ids, context=None):
        """Converts contracts to AMON.

        {
          "payerId":"payerID-123",
          "ownerId":"ownerID-123",
          "signerId":"signerID-123",
          "power":123,
          "dateStart":"2013-10-11T16:37:05Z",
          "dateEnd":null,
          "contractId":"contractID-123",
          "customer":{
            "customerId":"payerID-123",
            "address":{
              "city":"city-123",
              "cityCode":"cityCode-123",
              "countryCode":"ES",
              "country":"Spain",
              "street":"street-123",
              "postalCode":"postalCode-123"
            }
          },
          "meteringPointId":"c1759810-90f3-012e-0404-34159e211070",
          "devices":[
            {
              "dateStart":"2013-10-11T16:37:05Z",
              "dateEnd":null,
              "deviceId":"c1810810-0381-012d-25a8-0017f2cd3574"
            }
          ],
          "version":1,
          "activityCode":"activityCode",
          "tariffId":"tariffID-123",
        }
        """
        O = self.O
        if not context:
            context = {}
        res = []
        pol = O.GiscedataPolissa
        modcon_obj = O.GiscedataPolissaModcontractual
        if not hasattr(contract_ids, '__iter__'):
            contract_ids = [contract_ids]
        fields_to_read = [
            'modcontractual_activa', 'name', 'cups', 'comptadors', 'state',
            'tarifa', 'titular', 'pagador', 'data_alta', 'data_baixa',
            'llista_preu', 'cnae', 'modcontractuals_ids', 'potencia'
        ]
        for polissa in pol.read(contract_ids, fields_to_read):
            if polissa['state'] in ('esborrany', 'validar'):
                continue
            tarifa_atr = polissa['tarifa'][1]
            contract = {
                'contractId': polissa['name'],
                'ownerId': make_uuid('res.partner', polissa['titular'][0]),
                'payerId': make_uuid('res.partner', polissa['pagador'][0]),
                'signerId': make_uuid('res.partner', polissa['pagador'][0]),
                'power': int(polissa['potencia'] * 1000),
                'dateStart': make_utc_timestamp(polissa['data_alta']),
                'dateEnd': make_utc_timestamp(polissa['data_baixa']),
                'tariffId': tarifa_atr,
                'tariffCostId': polissa['llista_preu'][1],
                'version': int(polissa['modcontractual_activa'][1]),
                'activityCode': polissa['cnae'] and polissa['cnae'][1].split(' ')[0] or None,
                'customer': {
                    'customerId': make_uuid('res.partner', polissa['titular'][0]),
                },
                'devices': self.device_to_amon(
                    polissa['comptadors'],
                    force_serial=make_uuid('giscedata.cups.ps', polissa['cups'][1])
                )
            }
            # History fields
            history_fields = [
                ('tariffCostHistory', ['tariffCostId']),
                ('tariffHistory', ['tariffId']),
                ('powerHistory', ['power']),
                ('tertiaryPowerHistory', ['p1', 'p2', 'p3', 'p4', 'p5', 'p6'])
            ]
            for k, _ in history_fields:
                contract[k] = []
            modcon_fields = [
                'data_inici', 'data_final', 'llista_preu', 'tarifa', 'potencia'
            ]
            for modcon in O.GiscedataPolissaModcontractual.read(polissa['modcontractuals_ids'], modcon_fields):
                mod_tarifa_atr = modcon['tarifa'][1]
                contract['tariffCostHistory'].append({
                    'dateStart': make_utc_timestamp(modcon['data_inici']),
                    'dateEnd': make_utc_timestamp(modcon['data_final']),
                    'tariffCostId': modcon['llista_preu'][1]
                })
                contract['tariffHistory'].append({
                    'dateStart': make_utc_timestamp(modcon['data_inici']),
                    'dateEnd': make_utc_timestamp(modcon['data_final']),
                    'tariffId': mod_tarifa_atr
                })
                if is_tertiary(mod_tarifa_atr):
                    tertiary_power_history = {
                        'dateStart': make_utc_timestamp(modcon['data_inici']),
                        'dateEnd': make_utc_timestamp(modcon['data_final']),
                    }
                    for period, power in modcon_obj.get_potencies_dict(modcon['id']).items():
                        tertiary_power_history[period.lower()] = int(power * 1000)
                    contract['tertiaryPowerHistory'].append(tertiary_power_history)
                else:
                    contract['powerHistory'].append({
                        'dateStart': make_utc_timestamp(modcon['data_inici']),
                        'dateEnd': make_utc_timestamp(modcon['data_final']),
                        'power': int(modcon['potencia'] * 1000)
                    })

            # Reduce only for this changes
            for k, f in history_fields:
                contract[k] = reduce_history(contract[k], f)
                # Remove historic field if empty
                if not contract[k]:
                    contract.pop(k)

            # Get tertiary power
            if is_tertiary(tarifa_atr):
                contract['tertiaryPower'] = {}
                for period, power in pol.get_potencies_dict(polissa['id']).items():
                    contract['tertiaryPower'][period.lower()] = int(power * 1000)

            # Add custom fields
            customFields = pol.get_empowering_custom_fields(polissa['id'])
            if customFields:
                contract['customFields'] = customFields

            cups = self.cups_to_amon(polissa['cups'][0])
            recursive_update(contract, cups)
            res.append(remove_none(contract, context))
        return res

    def device_to_amon(self, device_ids, force_serial=None):
        if not device_ids:
            return []
        compt_obj = self.O.GiscedataLecturesComptador
        devices = []
        comptador_fields = ['data_alta', 'data_baixa']
        for comptador in compt_obj.read(device_ids, comptador_fields):
            devices.append({
                'dateStart': make_utc_timestamp(comptador['data_alta']),
                'dateEnd': make_utc_timestamp(comptador['data_baixa']),
                'deviceId': force_serial or make_uuid('giscedata.lectures.comptador', comptador['id'])
            })
        return devices

    def cups_to_amon(self, cups_id):
        cups_obj = self.O.GiscedataCupsPs
        muni_obj = self.O.ResMunicipi
        cups_fields = ['id_municipi', 'tv', 'nv', 'cpa', 'cpo', 'pnp', 'pt',
                       'name', 'es', 'pu', 'dp']
        if 'empowering' in cups_obj.fields_get():
            cups_fields.append('empowering')
        cups = cups_obj.read(cups_id, cups_fields)
        ine = muni_obj.read(cups['id_municipi'][0], ['ine'])['ine']
        res = {
            'meteringPointId': make_uuid('giscedata.cups.ps', cups['name']),
            'customer': {
                'address': {
                    'city': cups['id_municipi'][1],
                    'cityCode': ine,
                    'countryCode': 'ES',
                    'street': get_street_name(cups),
                    'postalCode': cups['dp']
                }
            },
            'experimentalGroupUserTest': False,
            'experimentalGroupUser': bool(cups.get('empowering', 0))
        }
        return res

    def indexed_to_amon(self, indexed_group, fact_ids):
        """
        indexed_group:  pricelist, cost
        fact_ids: list of invoice ids
        One grouped indexed to amon
        """
        # One grouped indexed to amon
        from base64 import b64decode
        import pandas as pd
        from StringIO import StringIO
        attach_obj = self.O.irAttachment
        llpreus, tcost = indexed_group
        df_grouped = pd.DataFrame({})
        res = []
        for fact_id in fact_ids:
            att_id = attach_obj.search([
                ('res_model', '=', 'giscedata.facturacio.factura'),
                ('res_id', '=', fact_id),
                ('name', '=like', 'PHF_%'),
            ])
            audit_data = attach_obj.read(att_id[0], ['datas'])['datas']
            audit_data = b64decode(audit_data)
            df = pd.read_csv(StringIO(audit_data), sep=';', names=['timestamp', 'price', 'n', 's'])
            df = df[['timestamp', 'price']]
            if df_grouped.empty:
                df_grouped = df.copy()
            else:
                df_grouped = pd.concat([df_grouped, df])
        df_grouped = df_grouped.groupby('timestamp').median().reset_index()
        df_grouped['timestamp'] = df_grouped['timestamp'].apply(lambda x: map_datetime(x))
        for ts_indexed_median in df_grouped.T.to_dict().values():
            res.append({
                'tariffId': str(llpreus),
                'tariffCostId': str(tcost),
                'price': ts_indexed_median['price'],
                'datetime': ts_indexed_median['timestamp'],
            })
        return res

def check_response(response, amon_data):
    logger.debug('Handlers: %s Class: %s' % (logger.handlers, logger))
    if response['_status'] != 'OK':
        content = '%s%s' % (json.dumps(amon_data), json.dumps(response))
        hash = sha1(content).hexdigest()[:8]
        logger.error(
            "Empowering response Code: %s - %s" % (response['_status'], hash),
            extra={'data': {
                'amon_data': amon_data,
                'response': response
            }}
        )
        return False
    return True
