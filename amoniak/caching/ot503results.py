# -*- coding: utf-8 -*-
from datetime import datetime
from dateutil.relativedelta import relativedelta

from results import *


class OT503Caching(OTCaching):
    def __init__(self, empowering_service, mongo_connection):
        super(OT503Caching, self).__init__(empowering_service, 'ot503_results',
                            mongo_connection, 'ot503', 'empowering_error',
                            'day', 'consumption')

    def _get_period_sum(self, contract, period):
        period_start = int(period + '01')
        period_end = int(period + '31')

        aggregate = [
            {
                "$match": {
                    "contractId": contract,
                    "day": {
                        "$gte": period_start,
                        "$lte": period_end
                    }
                }
            },
            {
                "$group": {
                    "_id": "$contractId",
                    "total": {
                        "$sum": "$consumption"
                    }
                }
            }
        ]

        result = self._result_collection.aggregate(aggregate)
        if ('result' in result and len(result['result']) > 0 and
            'total' in result['result'][0]):
            return result['result'][0]['total']
        else:
            return None

    def _delete_month_period(self, contract, period):
        """
        " Delete all cached in the given period
        """
        period_start = int(period + '01')
        period_end = int(period + '31')
        remove = {
            "contractId": contract,
            "day": {
                "$gte": period_start,
                "$lte": period_end
            }
        }
        self._result_collection.remove(remove)

    def _delete_all_periods_except(self, contract, period_list):
        """
        " Delete al results for the contract not in the period_list
        """
        keep_ids = []
        for period in period_list:
            period_start = int(period + '01')
            period_end = int(period + '31')
            query = {
                "contractId": contract,
                "day": {
                    "$gte": period_start,
                    "$lte": period_end
                }
            }
            ids = [x["_id"] for x in self._result_collection.find(query, {"_id": 1})]
            keep_ids.extend(ids)

        remove = {
            "contractId": contract,
            "_id": { '$nin': keep_ids }
        }

        # Identify them
        invalids_cursor = self._result_collection.find(remove,
                                                       {self._period_key: 1})
        to_delete = [x[self._period_key] for x in invalids_cursor]
        # Delete them

        self._result_collection.remove(remove)
        # Notify deleted
        return to_delete

    def validate_contract(self, values, contract, period=None, log_errors=True):
        """ Validate the contract according to the values dict.
        " Values dict contain period as key and value as value.
        " Will create the error log in collection according to log_errors param
        "
        " values example:
        " {'201301': 42.2, '201302': 75.3}
        "
        " OT503 specific
        " This ot uses daily measures will check if the sum of the dailys
        " is equal to the stored monthly. If the sum of the dailys is equal
        " to the month the dailys are considered valid, deleted otherwise
        """

        # If period specified discard all other possible values in values dict
        if period and period in values:
            values = {period: values[period]}
        elif period and period not in values:
            values = {}

        # Different algorism than super validate_contract:
        # here we will delete al periods not in valid_periods
        valid_periods = []
        for v_period, v_value in values.iteritems():
            error = None
            error_details = {}
            cached_value = self._get_period_sum(contract, v_period)
            if cached_value == None:
                error = NO_RESULT_ERROR
            elif not self._is_valid(cached_value, v_value):
                # Stored and empowering result missmatch
                error = WRONG_VALUE_ERROR
                error_details.update({
                    'expected': v_value,
                    'cached': cached_value
                })
                self._delete_month_period(contract, v_period)
            else:
                # Result is OK
                # All periods not in this list will be deleted
                valid_periods.append(v_period)

            if error and log_errors:
                self._insert_error(contract, v_period, error,
                                   error_details)

        if period and period not in valid_periods:
            # Only checking one period and is invalid
            self._delete_month_period(contract, period)
        elif not period:
            # We are checking all contract data
            # must delete all not checked results
            deleteds = self._delete_all_periods_except(contract, valid_periods)
            error = NO_STORED_ERROR
            for deleted in deleteds:
                self._insert_error(contract, deleted, error)
        else:
            # Period specified and is valid -> OK nothing to do
            pass

    def get_cached(self, contract, period):
        end_period_date = datetime.strptime(period, '%Y%m')

        end_period_date += relativedelta(months=1)
        end_period_date = datetime(year=end_period_date.year,
                                   month=end_period_date.month,
                                   day=1)
        end_period_date -= relativedelta(days=1)
        end_period = int(end_period_date.strftime('%Y%m%d'))

        # Set the start period at the begining of the month
        start_period_date = datetime(year=end_period_date.year,
                                     month=end_period_date.month,
                                     day=1)
        start_period = int(start_period_date.strftime('%Y%m%d'))

        query = {'contractId': contract,
                 'day': {'$gte': start_period,
                          '$lte': end_period}}
        res = self._result_collection.find(query).sort('day', 1)
        cached = [x for x in res]
        for elem in cached:
            for hidden_key in self._hidden_keys:
                if hidden_key in elem:
                    elem.pop(hidden_key)
        return {"_items": cached}
