# -*- coding: utf-8 -*-
from results import OTCaching


class OT103Caching(OTCaching):
    def __init__(self, empowering_service, mongo_connection):
        super(OT103Caching, self).__init__(empowering_service, 'ot103_results',
                            mongo_connection, 'ot103', 'empowering_error',
                            'month', 'consumption')
