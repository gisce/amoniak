#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from hashlib import sha1
import json
import logging

from .cache import CUPS_CACHE, CUPS_UUIDS
from .utils import recursive_update
from empowering.utils import remove_none, make_uuid, make_utc_timestamp


UNITS = {1: '', 1000: 'k'}


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


class AmonConverter(object):
    def __init__(self, connection):
        self.O = connection

    def get_cups_from_device(self, device_id):
        O = self.O
        # Remove brand prefix and right zeros
        serial = get_device_serial(device_id)
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

    def profile_to_amon(self, profiles):
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
        O = self.O
        res = []
        if not hasattr(profiles, '__iter__'):
            profiles = [profiles]
        for profile in profiles:
            mp_uuid = self.get_cups_from_device(profile['name'])
            if not mp_uuid:
                logger.info("No mp_uuid for &s" % profile['name'])
                continue
            device_uuid = make_uuid('giscedata.lectures.comptador', profile['name'])
            res.append({
                "deviceId": device_uuid,
                "meteringPointId": mp_uuid,
                "readings": [
                    {
                        "type":  "electricityConsumption",
                        "unit": "%sWh" % UNITS[profile.get('magn', 1000)],
                        "period": "CUMULATIVE",
                    },
                    {
                        "type": "electricityKiloVoltAmpHours",
                        "unit": "%sVArh" % UNITS[profile.get('magn', 1000)],
                        "period": "CUMULATIVE",
                    }
                ],
                "measurements": [
                    {
                        "type": "electricityConsumption",
                        "timestamp": make_utc_timestamp(profile['date_end']),
                        "value": float(profile['ai'])
                    },
                    {
                        "type": "electricityKiloVoltAmpHours",
                        "timestamp": make_utc_timestamp(profile['date_end']),
                        "value": float(profile['r1'])
                    }
                ]
            })
        return res


    def device_to_amon(self, device_ids):
        """Convert a device to AMON.

        {
            "utilityId": "Utility Id",
            "externalId": required string UUID,
            "meteringPointId": required string UUID,
            "metadata": {
                "max": "Max number",
                "serial": "Device serial",
                "owner": "empresa/client"
            },
        }
        """
        O = self.O
        res = []
        if not hasattr(device_ids, '__iter__'):
            device_ids = [device_ids]
        for dev_id in device_ids:
            dev = O.GiscedataLecturesComptador.browse(dev_id)
            if dev.propietat == "empresa":
                dev.propietat = "company"
            res.append(remove_none({
                "utilityId": "1",
                "externalId": make_uuid('giscedata.lectures.comptador', dev_id),
                "meteringPointId": make_uuid('giscedata.cups.ps', dev.polissa.cups.name),
                "metadata": {
                   "max": dev.giro,
                   "serial": dev.name,
                   "owner": dev.propietat,
                }
            }))
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
        fields_to_read = ['modcontractual_activa', 'name', 'cups', 'comptadors', 'state']
        for polissa in pol.read(contract_ids, fields_to_read):
            if polissa['state'] in ('esborrany', 'validar'):
                continue
            if 'modcon_id' in context:
                modcon = modcon_obj.read(context['modcon_id'])
            elif polissa['modcontractual_activa']:
                modcon = modcon_obj.read(polissa['modcontractual_activa'][0])
            else:
                logger.error("Problema amb la polissa %s" % polissa['name'])
                continue
            contract = {
                'ownerId': make_uuid('res.partner', modcon['titular'][0]),
                'payerId': make_uuid('res.partner', modcon['pagador'][0]),
                'dateStart': make_utc_timestamp(modcon['data_inici']),
                'dateEnd': make_utc_timestamp(modcon['data_final']),
                'contractId': polissa['name'],
                'tariffId': modcon['tarifa'][1],
                'power': int(modcon['potencia'] * 1000),
                'version': int(modcon['name']),
                'activityCode': modcon['cnae'] and modcon['cnae'][1] or None,
                'customer': {
                    'customerId': make_uuid('res.partner', modcon['titular'][0]),
                },
                'devices': self.device_to_amon(polissa['comptadors'])
            }
            cups = self.cups_to_amon(modcon['cups'][0])
            recursive_update(contract, cups)
            res.append(remove_none(contract, context))
        return res

    def device_to_amon(self, device_ids):
        compt_obj = self.O.GiscedataLecturesComptador
        devices = []
        comptador_fields = ['data_alta', 'data_baixa']
        for comptador in compt_obj.read(device_ids, comptador_fields):
            devices.append({
                'dateStart': make_utc_timestamp(comptador['data_alta']),
                'dateEnd': make_utc_timestamp(comptador['data_baixa']),
                'deviceId': make_uuid('giscedata.lectures.comptador',
                                      compt_obj.build_name_tg(comptador['id']))
            })
        return devices

    def cups_to_amon(self, cups_id):
        cups_obj = self.O.GiscedataCupsPs
        muni_obj = self.O.ResMunicipi
        cups_fields = ['id_municipi', 'tv', 'nv', 'cpa', 'cpo', 'pnp', 'pt',
                       'name', 'es', 'pu', 'dp']
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
            }
        }
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
