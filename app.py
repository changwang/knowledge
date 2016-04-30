# -*- coding: utf-8 -*-
from wsgiref import simple_server
from task import query_table, query_row

import falcon

SN_USERNAME = 'X-SN-Username'
SN_PASSWORD = 'X-SN-Password'

WF_CONTEXT_HEADER = 'X-Workflow-Context'
WF_EVENT_NAME = 'X-Workflow-Event-Name'

TABLE_NAME_HEADER = 'X-Table-Name'
TABLE_QUERY_LIMIT_HEADER = 'X-Query-Limit'
TABLE_QUERY_OFFSET_HEADER = 'X-Query-Offset'
TABLE_QUERY_RETURN_COUNT = 'X-Query-Count'
TABLE_QUERY_PRIMARY_KEY = 'X-Query-Primary-Key'

DEFAULT_QUERY_LIMIT = 20
MAXIMUM_QUERY_LIMIT = 50


def validate_wf_context(req, resp, resource, params):
    context = req.get_header(WF_CONTEXT_HEADER)
    if not context:
        raise falcon.HTTPBadRequest('Bad Request', 'Workflow Context ID is required')


class Table(object):
    @falcon.before(validate_wf_context)
    def on_get(self, req, resp, name):
        if not name:
            raise falcon.HTTPBadRequest('Bad Request', 'Table Name is required')

        resp.append_header(TABLE_NAME_HEADER, name)

        limit = self._get_limit_param(req)
        resp.append_header(TABLE_QUERY_LIMIT_HEADER, str(limit))

        offset = self._get_offset_param(req)
        resp.append_header(TABLE_QUERY_OFFSET_HEADER, str(offset))

        resp.append_header(WF_CONTEXT_HEADER, req.get_header(WF_CONTEXT_HEADER))

        try:
            result = query_table.delay(name, limit, offset, **compose_post_params(req))
            payload, count = result.get(timeout=10, propagate=True)
            if payload:
                resp.append_header(TABLE_QUERY_RETURN_COUNT, str(count))
                resp.status = falcon.HTTP_200
                resp.body = payload
        except Exception, e:
            raise falcon.HTTPBadRequest('Bad Request', str(e))

    def _get_limit_param(self, req):
        limit = req.get_param('limit')
        if not limit:
            return DEFAULT_QUERY_LIMIT
        try:
            limit = int(limit)
        except Exception, e:
            raise falcon.HTTPBadRequest('Bad Request', e.message)

        if limit > MAXIMUM_QUERY_LIMIT:
            error_msg = 'Query limit is too large, maximum size is 50'
            raise falcon.HTTPBadRequest('Bad Request', error_msg)
        return limit

    def _get_offset_param(self, req):
        offset = req.get_param('offset')
        if not offset:
            return 0
        try:
            return int(offset)
        except Exception, e:
            raise falcon.HTTPBadRequest('Bad Request', e.message)


class Row(object):
    @falcon.before(validate_wf_context)
    def on_get(self, req, resp, name, id):
        if not name:
            raise falcon.HTTPBadRequest('Bad Request', 'Table Name is Required')

        if not id:
            raise falcon.HTTPBadRequest('Bad Request', 'Table Record ID is Required')

        primary_key = req.get_header(TABLE_QUERY_PRIMARY_KEY) if req.get_header(TABLE_QUERY_PRIMARY_KEY) else 'id'

        try:
            kwargs = compose_post_params(req)
            kwargs['pk'] = primary_key
            result = query_row.delay(name, id, **kwargs)
            payload = result.get(timeout=10, propagate=True)
            if payload:
                resp.status = falcon.HTTP_200
                resp.body = payload
        except Exception, e:
            raise falcon.HTTPBadRequest('Bad Request', str(e))


def compose_post_params(req):
    context = req.get_header(WF_CONTEXT_HEADER)
    event_name = req.get_header(WF_EVENT_NAME)
    sn_username = req.get_header(SN_USERNAME)
    sn_password = req.get_header(SN_PASSWORD)
    return {
        'protocol': req.protocol,
        'host': req.remote_addr,
        'port': 8080,
        'context_id': context,
        'event_name': event_name,
        'sn_username': sn_username,
        'sn_password': sn_password
    }


tables = Table()
row = Row()

api = application = falcon.API()
api.add_route('/tables/{name}', tables)
api.add_route('/tables/{name}/{id}', row)

if __name__ == '__main__':
    httpd = simple_server.make_server('0.0.0.0', 8000, api)
    httpd.serve_forever()
