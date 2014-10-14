#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

import click

from amoniak import tasks
from amoniak.utils import setup_logging
from amoniak import VERSION


@click.group()
@click.option('--log-level', default='info')
def amoniak(log_level):
    log_level = log_level.upper()
    log_level = getattr(logging, log_level, 'INFO')
    logging.basicConfig(level=log_level)
    setup_logging()
    logger = logging.getLogger('amon')
    logger.info('Running amoniak version: %s' % VERSION)


@amoniak.command()
def enqueue_all_amon_measures():
    logger = logging.getLogger('amon')
    logger.info('Enqueuing all amon measures')
    tasks.enqueue_all_amon_measures()


@amoniak.command()
def enqueue_measures():
    logger = logging.getLogger('amon')
    logger.info('Enqueuing measures')
    tasks.enqueue_measures()


@amoniak.command()
def enqueue_contracts():
    logger = logging.getLogger('amon')
    logger.info('Enqueuing updated contracts')
    tasks.enqueue_contracts()
    logger.info('Enqueuing new contracts')
    tasks.enqueue_new_contracts()


if __name__ == '__main__':
    amoniak(obj={})