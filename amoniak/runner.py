#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import logging
import sys

from amoniak.tasks import (
    enqueue_all_amon_measures, enqueue_measures, enqueue_contracts,
    enqueue_new_contracts,
)
from amoniak.utils import setup_logging


if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO)
    setup_logging()
    logger = logging.getLogger('amon')

    if sys.argv[1] == 'enqueue_all_amon_measures':
        logger.info('Enqueuing all amon measures')
        enqueue_all_amon_measures()

    elif sys.argv[1] == "enqueue_measures":
        logger.info('Enqueuing measures')
        enqueue_measures()

    elif sys.argv[1] == 'enqueue_contracts':
        logger.info('Enqueuing updated contracts')
        enqueue_contracts()
        logger.info('Enqueuing new contracts')
        enqueue_new_contracts()