# -*- coding: utf-8 -*-

import time

AVALIABLE_ONLINE_TOOLS = ['ot101', 'ot201', 'ot103', 'ot503']

NO_RESULT_ERROR = 'No empowering result'
NO_STORED_ERROR = 'No stored value to compare with'
WRONG_VALUE_ERROR = 'Wrong value'

"""
" Useful class to cache the empowering results in a mongo database.
" Once pull_contract is called the mongo database will create a collection
" with the empowering resource name (db.ot101, db.ot103...)
"
" Also the cached results can be validated comapring with a dict
" with the correct values using the method validate_contract
" THis will delete the invalid cached results and fill the
" collection db.empowering_error with the errors found.
"
" Using the function error_report an string is generated
" using the db.empowering_error to generate a report.
"""


class OTCaching(object):

    def __init__(self, empowering_service, empowering_resource,
                 mongo_connection, ot_code, log_error_collection,
                 period_key, value_key):
        self._empowering_resource = getattr(empowering_service,
                                            empowering_resource)
        self._result_collection = getattr(mongo_connection, ot_code)
        self._log_error_collection = getattr(mongo_connection,
                                             log_error_collection)
        # Key used in results to specify the period
        self._period_key = period_key
        self._value_key = value_key
        self._ot_code = ot_code

        # Hidden keys not returned by get_cached
        self._hidden_keys = ['companyId', '_updated', '_etag',
                             '_id', '_created', '_links']

    def pull_contract(self, contract, period=None):
        """ Will ask for results of online tool for the specidied
        " contract for ALL periods if is not specidied in period param.
        " Pulled results will be stored in the mongo database
        "
        " If result already exist is replaced by the new one
        """
        results = self._empowering_resource().pull(contract=contract,
                                                   period=period)
        if '_items' not in results:
            # If _items not i results nothing to do, no results found
            return 0

        for result in results['_items']:
            result_period = result[self._period_key]
            if self._get(contract, result_period):
                # Delete cached result to replace it
                self._delete_cached(contract, result_period)
            self._store(result)

    def validate_contract(self, values, contract, period=None, log_errors=True):
        """ Validate the contract according to the values dict.
        " Values dict contain period as key and value as value.
        " Will create the error log in collection according to log_errors param
        "
        " values example:
        " {'201301': 42.2, '201302': 75.3}
        """
        cached_results = self._get(contract, period)
        for result in cached_results:
            cached_period = str(int(result[self._period_key]))
            cached_value = result[self._value_key]
            error = None
            error_details = {}

            if cached_period not in values:
                # No stored value to compare with.
                # So we discart the empowering result
                error = NO_STORED_ERROR
            elif not self._is_valid(cached_value, values[cached_period]):
                # Stored and empowering result missmatch
                # So we discart the empowering result
                error = WRONG_VALUE_ERROR
                error_details.update({
                    'expected': values[cached_period],
                    'cached': cached_value
                })
            else:
                pass #Everything OK :)

            if error:
                self._delete_cached(contract, cached_period)
                if log_errors:
                    self._insert_error(contract, cached_period, error,
                                       error_details)

            if cached_period in values:
                # Pop from values to know if there are missing results
                values.pop(cached_period)

        for v_period, v_value in values.iteritems():
            # There are still values to be checked
            error = NO_RESULT_ERROR
            self._insert_error(contract, v_period, error)

    def error_report(self, ot_code=None, contract=None, period=None,
                     validation_date='today'):
        """
        " @return an string containing a report of the errors happened
        """
        report = ''
        search_params = {}

        if validation_date == 'today':
            validation_date = time.strftime('%Y-%m-%d')

        if validation_date:
            search_params.update({'validation_date': validation_date})
        if period:
            search_params.update({'period': period})
        if contract:
            search_params.update({'contract': contract})

        old_search = {'validation_date': {'$ne': validation_date}}
        old_errors = self._log_error_collection.find(old_search).count()
        if old_errors:
            report += 'WARNING: There are %d stored old errors.\n' % old_errors

        filter_msg = 'REPORT FILTER: %s ot - %s contract %s period %s date\n'
        filter_msg %= (
            ot_code and ot_code or 'all',
            contract and contract or 'all',
            period and period or 'all',
            validation_date and validation_date or 'all'
        )
        report += filter_msg

        if not ot_code:
            ot_codes = AVALIABLE_ONLINE_TOOLS
        else:
            ot_codes = [ot_code]

        for ot in ot_codes:
            report += '%s\n' % ot
            for error in (WRONG_VALUE_ERROR, NO_RESULT_ERROR, NO_STORED_ERROR):
                errors = search_params.copy()
                errors.update({'error': error, 'ot_code': ot})
                count = self._log_error_collection.find(errors).count()
                report += '\t%s: %d\n' % (error, count)

        return report

    def error_clear(self, ot_code=None, contract=None, period=None,
                    validation_date='today'):
        """
        " Clear errors from database, si recomended to call this methond
        " just after error_report with the same parameters.
        """
        search_params = {}

        if validation_date == 'today':
            validation_date = time.strftime('%Y-%m-%d')

        if validation_date:
            search_params.update({'validation_date': validation_date})
        if ot_code:
            search_params.update({'ot_code': ot_code})
        if period:
            search_params.update({'period': period})
        if contract:
            search_params.update({'contract': contract})

        self._log_error_collection.remove(search_params)

    def get_cached(self, contract, period):
        cached = self._get(contract, period)
        for elem in cached:
            for hidden_key in self._hidden_keys:
                if hidden_key in elem:
                    elem.pop(hidden_key)
        return {"_items": cached}

    def _get(self, contract, period=None):
        query = {'contractId': contract}
        if period:
            query.update({self._period_key: int(period)})

        return [x for x in self._result_collection.find(query)]

    def _store(self, result):
        self._result_collection.insert(result)

    def _delete_cached(self, contract, period=None):
        remove_query = {'contractId': contract}
        if period:
            remove_query.update({
                self._period_key: int(period)
            })

        self._result_collection.remove(remove_query)

    def _is_valid(self, cached, reference):
        """ Return True if cached value is valid according to the
        " value reference, false otherwise
        """
        return abs(reference - cached) < 2

    def _insert_error(self, contract, period, error_message,
                      error_details=None):
        error = {
            'ot_code': self._ot_code,
            'contract': contract,
            'period': period,
            'error': error_message,
            'validation_date': time.strftime('%Y-%m-%d')
        }
        if error_details:
            error.update(error_details)
        if not self._log_error_collection.find(error).count():
            # Avoid insert errors twice
            self._log_error_collection.insert(error)

