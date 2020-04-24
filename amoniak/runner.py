#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os

import click

from amoniak import tasks
from amoniak.utils import setup_logging
from amoniak import VERSION


def print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo(VERSION)
    ctx.exit()

@click.group()
@click.option('--log-level', default='info')
@click.option('--async/--no-async', default=True)
@click.option('--version', is_flag=True, callback=print_version,
              expose_value=False, is_eager=True)
def amoniak(log_level, async):
    MODE = {True: 'ASYNC', False: 'SYNC'}
    log_level = log_level.upper()
    log_level = getattr(logging, log_level, 'INFO')
    logging.basicConfig(level=log_level)
    setup_logging()
    logger = logging.getLogger('amon')
    logger.info('Running amoniak version: %s' % VERSION)
    logger.info('Running in %s mode' % MODE[async])
    os.environ['RQ_ASYNC'] = str(async)

@amoniak.command()
def enqueue_all_amon_measures():
    logger = logging.getLogger('amon')
    logger.info('Enqueuing all amon measures')
    tasks.enqueue_all_amon_measures()


@amoniak.command()
@click.option('--force', default=False, is_flag=True)
@click.argument('contracts', nargs=-1)
def enqueue_measures(contracts, force):
    logger = logging.getLogger('amon')
    force_log = force and '(F) ' or ''
    if contracts:
        logger.info('{}Enqueuing measures for contracts: {}'.format(force_log, ', '.join(contracts)))
    else:
        logger.info('{}Enqueuing all measures with etag'.format(force_log))
        contracts = None
    logger.info('Enqueuing measures')
    tasks.enqueue_measures(contracts=contracts)


@amoniak.command()
def enqueue_contracts():
    logger = logging.getLogger('amon')
    logger.info('Enqueuing updated contracts')
    tasks.enqueue_contracts()
    logger.info('Enqueuing new contracts')
    tasks.enqueue_new_contracts()


@amoniak.command()
@click.option('--force', default=False, is_flag=True)
@click.argument('contracts', nargs=-1)
def enqueue_contract(contracts, force):
    logger = logging.getLogger('amon')
    force_log = force and '(F) ' or ''
    if contracts:
        logger.info('{}Enqueuing contracts: {}'.format(force_log, ', '.join(contracts)))
    else:
        logger.info('{}Enqueuing all contracts without etag'.format(force_log))
        contracts = None
    tasks.enqueue_contracts(contracts, force)


if __name__ == '__main__':
    amoniak(obj={})