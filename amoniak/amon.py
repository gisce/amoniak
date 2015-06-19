#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from hashlib import sha1
import json
import logging

from .cache import CUPS_CACHE, CUPS_UUIDS
from .utils import recursive_update
from empowering.utils import null_to_none, remove_none, make_uuid, make_utc_timestamp


UNITS = {1: '', 1000: 'k'}


logger = logging.getLogger('amon')


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
        def get_device_serial(device_id):
            field_to_read = 'name'
            return O.GiscedataLecturesComptador.read([device_id],[field_to_read])[0][field_to_read]

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
            mp_uuid = self.get_cups_from_device(profile['comptador'][0])
            if not mp_uuid:
                logger.info("No mp_uuid for &s" % profile['comptador'][0])
                continue
            device_uuid = make_uuid('giscedata.lectures.comptador', profile['comptador'][0])

            if   (profile['tipus'] == 'A'):
                res.append({
                    "deviceId": device_uuid,
                    "meteringPointId": mp_uuid,
                    "readings": [
                        {
                            "type":  "electricityConsumption",
                            "unit": "%sWh" % UNITS[profile.get('magn', 1000)],
                            "period": "CUMULATIVE",
                        }
                    ],
                    "measurements": [
                        {
                            "type": "electricityConsumption",
                            "timestamp": make_utc_timestamp(profile['date_end']),
                            "value": float(profile['lectura'])
                        }
                    ]
                })

            elif (profile['tipus'] == 'R'):
                res.append({
                    "deviceId": device_uuid,
                    "meteringPointId": mp_uuid,
                    "readings": [
                        {
                            "type": "electricityKiloVoltAmpHours",
                            "unit": "%sVArh" % UNITS[profile.get('magn', 1000)],
                            "period": "CUMULATIVE",
                        }
                    ],
                    "measurements": [
                        {
                            "type": "electricityKiloVoltAmpHours",
                            "timestamp": make_utc_timestamp(profile['date_end']),
                            "value": float(profile['lectura'])
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


    def building_to_amon(self,building_id):
        """ Convert building to AMON

         {
              "buildingConstructionYear": 2014,
              "dwellingArea": 196,
              "buildingType": "Apartment",
              "dwellingPositionInBuilding": "first_floor",
              "dwellingOrientation": "SE",
              "buildingWindowsType": "double_panel",
              "buildingWindowsFrame": "PVC",
              "buildingHeatingSource": "district_heating",
              "buildingHeatingSourceDhw": "gasoil",
              "buildingSolarSystem": "not_installed"
         }
        """
        if not building_id:
            return None

        O = self.O
        building_obj = O.EmpoweringCupsBuilding

        fields_to_read =  ['buildingConstructionYear', 'dwellingArea', 'buildingType', 'dwellingPositionInBuilding',
                           'dwellingOrientation', 'buildingWindowsType', 'buildingWindowsFrame',
                           'buildingHeatingSource', 'buildingHeatingSourceDhw', 'buildingSolarSystem']
        building = building_obj.read(building_id)

        return remove_none(null_to_none({ field: building[field] for field in fields_to_read}))

    def eprofile_to_amon(self,profile_id):
        """ Convert profile to AMON

        {
          "totalPersonsNumber": 3,
          "minorsPersonsNumber": 0,
          "workingAgePersonsNumber": 2,
          "retiredAgePersonsNumber": 1,
          "malePersonsNumber": 2,
          "femalePersonsNumber": 1,
          "educationLevel": {
            "edu_prim": 0,
            "edu_sec": 1,
            "edu_uni": 1,
            "edu_noStudies": 1
        }
        """
        if not profile_id:
            return None

        O = self.O
        profile_obj = O.EmpoweringModcontractualProfile
        fields_to_read = ['totalPersonsNumber', 'minorPersonsNumber', 'workingAgePersonsNumber', 'retiredAgePersonsNumber',
                          'malePersonsNumber', 'femalePersonsNumber', 'eduLevel_prim', 'eduLevel_sec', 'eduLevel_uni',
                          'eduLevel_noStudies']
        profile = profile_obj.read(profile_id)

        return remove_none(null_to_none({
            "totalPersonsNumber": profile['totalPersonsNumber'],
            "minorsPersonsNumber": profile['minorPersonsNumber'],
            "workingAgePersonsNumber": profile['workingAgePersonsNumber'],
            "retiredAgePersonsNumber": profile['retiredAgePersonsNumber'],
            "malePersonsNumber": profile['malePersonsNumber'],
            "femalePersonsNumber": profile['femalePersonsNumber'],
            "educationLevel": {
                "edu_prim": profile['eduLevel_prim'],
                "edu_sec": profile['eduLevel_sec'],
                "edu_uni": profile['eduLevel_uni'],
                "edu_noStudies": profile['eduLevel_noStudies']
            }
        }))


    def service_to_amon(self,service_id):
        """ Convert service to AMON

         {
            "OT701": "p1;P2;px"
         }
        """
        if not service_id:
            return None

        O = self.O
        service_obj = O.EmpoweringModcontractualService
        fields_to_read = ['OT101', 'OT103', 'OT105', 'OT106', 'OT109', 'OT201', 'OT204', 'OT401', 'OT502', 'OT503', 'OT603',
                         'OT603g', 'OT701', 'OT703']
        service = service_obj.read(service_id)

        return remove_none(null_to_none({ field: service[field] for field in fields_to_read}))



    def contract_to_amon(self, contract_ids, context=None):
        """Converts contracts to AMON.
        {
          "contractId": "contractId-123",
          "ownerId": "ownerId-123",
          "payerId": "payerId-123",
          "signerId": "signerId-123",
          "power": 123,
          "dateStart": "2013-10-11T16:37:05Z",
          "dateEnd": null,
          "weatherStationId": "weatherStatioId-123",
          "version": 1,
          "activityCode": "activityCode",
          "tariffId": "tariffID-123",
          "meteringPointId": "c1759810-90f3-012e-0404-34159e211070",
          "experimentalGroupUser": True,
          "experimentalGroupUserTest": True,
          "activeUser": True,
          "activeUserDate": "2014-10-11T16:37:05Z",
          "customer": {
            "customerId": "customerId-123",
            "address": {
              "buildingId": "building-123",
              "city": "city-123",
              "cityCode": "cityCode-123",
              "countryCode": "ES",
              "country": "Spain",
              "street": "street-123",
              "postalCode": "postalCode-123",
              "province": "Barcelona",
              "provinceCode": "provinceCode-123",
              "parcelNumber": "parcelNumber-123"
            },
            "buildingData": {
              "buildingConstructionYear": 2014,
              "dwellingArea": 196,
              "buildingType": "Apartment",
              "dwellingPositionInBuilding": "first_floor",
              "dwellingOrientation": "SE",
              "buildingWindowsType": "double_panel",
              "buildingWindowsFrame": "PVC",
              "buildingHeatingSource": "district_heating",
              "buildingHeatingSourceDhw": "gasoil",
              "buildingSolarSystem": "not_installed"
            },
            "profile": {
              "totalPersonsNumber": 3,
              "minorsPersonsNumber": 0,
              "workingAgePersonsNumber": 2,
              "retiredAgePersonsNumber": 1,
              "malePersonsNumber": 2,
              "femalePersonsNumber": 1,
              "educationLevel": {
                "edu_prim": 0,
                "edu_sec": 1,
                "edu_uni": 1,
                "edu_noStudies": 1
              }
            },
            "customisedGroupingCriteria": {
              "criteria_1": "CLASS 1",
              "criteria_2": "XXXXXXX",
              "criteria_3": "YYYYYYY"
            },
            "customisedServiceParameters": {
              "OT701": "p1;P2;px"
            }
          },
          "devices": [
            {
              "dateStart": "2013-10-11T16:37:05Z",
              "dateEnd": null,
              "deviceId": "c1810810-0381-012d-25a8-0017f2cd3574"
            }
          ]
        }
        """
        O = self.O
        if not context:
            context = {}
        res = []
        pol = O.GiscedataPolissa
        modcon_obj = O.GiscedataPolissaModcontractual

        building_obj = O.EmpoweringCupsBuilding
        profile_obj = O.EmpoweringModcontractualProfile
        service_obj = O.EmpoweringModcontractualService

        if not hasattr(contract_ids, '__iter__'):
            contract_ids = [contract_ids]
        fields_to_read = ['modcontractual_activa', 'name', 'cups', 'comptadors', 'state']
        for polissa in pol.read(contract_ids, fields_to_read):
            if polissa['state'] in ('esborrany', 'validar'):
                continue

            modcon_id = None
            if 'modcon_id' in context:
                modcon_id = context['modcon_id']
            elif polissa['modcontractual_activa']:
                modcon_id = polissa['modcontractual_activa'][0]
            else:
                logger.error("Problema amb la polissa %s" % polissa['name'])
                continue
            modcon = modcon_obj.read(modcon_id)

            def  get_first(x):
                return x[0] if x else None

            building_id = get_first(building_obj.search([('cups_id', '=', modcon['cups'][0])]))
            profile_id = get_first(profile_obj.search([('modcontractual_id', '=', modcon_id)]))
            service_id = get_first(service_obj.search([('modcontractual_id', '=', modcon_id)]))

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
                    'buildingData': self.building_to_amon(building_id),
                    'profile': self.eprofile_to_amon(profile_id),
                    'customisedServiceParameters': self.service_to_amon(service_id)
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
#                'deviceId': make_uuid('giscedata.lectures.comptador',
#                                      compt_obj.build_name_tg(comptador['id']))
                'deviceId': make_uuid('giscedata.lectures.comptador', comptador['id'])
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
                    #'street': get_street_name(cups),
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
