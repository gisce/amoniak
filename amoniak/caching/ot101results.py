# -*- coding: utf-8 -*-
from results import OTCaching


class OT101Caching(OTCaching):
    def __init__(self, empowering_service, mongo_connection):
        super(OT101Caching, self).__init__(empowering_service, 'ot101_results',
                                  mongo_connection, 'ot101', 'empowering_error',
                                  'month', 'consumption')
