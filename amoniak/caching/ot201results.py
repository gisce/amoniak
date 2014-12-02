# -*- coding: utf-8 -*-
from results import OTCaching


class OT201Caching(OTCaching):
    def __init__(self, empowering_service, mongo_connection):
        super(OT201Caching, self).__init__(empowering_service, 'ot201_results',
                            mongo_connection, 'ot201', 'empowering_error',
                            'month', 'actualConsumption')
