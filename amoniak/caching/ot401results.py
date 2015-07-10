# -*- coding: utf-8 -*-
from results import OTCaching


class OT401Caching(OTCaching):
    def __init__(self, empowering_service, mongo_connection):
        super(OT401Caching, self).__init__(empowering_service, 'ot401_results',
                                  mongo_connection, 'ot401', 'empowering_error',
                                  'month', 'consumption')
