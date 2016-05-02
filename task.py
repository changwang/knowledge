# -*- coding: utf-8 -*-

import requests
from celery import Celery
import time
import ujson
from requests.auth import HTTPDigestAuth
from logger import logger

from db import fetch_from_table, fetch_one_row

BROKER_URL = 'redis://localhost:6379/0'
BACKEND_URL = 'redis://localhost:6379/1'

celery_app = Celery('tasks', broker=BROKER_URL, backend=BACKEND_URL)

SN_EVENT_PATH = '/api/now/v1/wf/context/{context_id}/{event_name}'


@celery_app.task
def query_table(name, limit, offset, **kwargs):
    logger.info("Start querying {0}, limit size {1}, offset {2}".format(name, limit, offset))
    data = fetch_from_table(name, limit, offset)
    results = []

    for row in data:
        results.append(dict(row))

    post_request.delay(**kwargs)

    return ujson.dumps(results), len(results)


@celery_app.task
def query_row(name, id, **kwargs):
    pk = kwargs['pk']
    logger.info("Start querying {0}, id {1}".format(name, id))
    row = fetch_one_row(name, id, pk=pk)

    post_request.delay(**kwargs)

    return ujson.dumps(dict(row))


@celery_app.task
def post_request(**kwargs):
    is_async = kwargs.get('is_async', False)
    if is_async:
        logger.info('start simulating heavy lifting job')
        time.sleep(10)

    logger.info("Parameters + " + str(kwargs))
    path = SN_EVENT_PATH.format(**kwargs)
    url = '{protocol}://{host}:{port}{path}'.format(path=path, **kwargs)
    logger.info('Post request to %s' % (url,))
    requests.post(url, auth=HTTPDigestAuth(kwargs['sn_username'], kwargs['sn_password']))
