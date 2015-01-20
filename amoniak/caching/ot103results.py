# -*- coding: utf-8 -*-

from datetime import datetime
from dateutil.relativedelta import relativedelta

from results import OTCaching


class OT103Caching(OTCaching):
    def __init__(self, empowering_service, mongo_connection):
        super(OT103Caching, self).__init__(empowering_service, 'ot103_results',
                            mongo_connection, 'ot103', 'empowering_error',
                            'month', 'consumption')

    def get_cached(self, contract, period):
        month_range = 13
        end_period_date = datetime.strptime(period, '%Y%m')
        end_period_date = datetime(year=end_period_date.year,
                                   month=end_period_date.month,
                                   day=1)
        end_period = int(period)
        start_period_date = end_period_date + relativedelta(months=-(month_range))
        start_period = int(start_period_date.strftime('%Y%m'))

        query = {'contractId': contract,
                 'month': {'$gt': start_period,
                            '$lte': end_period}}
        res = self._result_collection.find(query).sort('month', 1)
        cached = [x for x in res]

        for elem in cached:
            for hidden_key in self._hidden_keys:
                if hidden_key in elem:
                    elem.pop(hidden_key)
        return {"_items": cached}
