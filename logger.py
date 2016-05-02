# -*- coding: utf-8 -*-
import logging

__all__ = [
    'logger'
]

FORMAT = '%(asctime)-15s %(clientip)s %(user)-8s %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger(__file__)
