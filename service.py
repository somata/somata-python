from gevent import monkey; monkey.patch_all()

from bottle import route, run, request
import os

PORT = os.environ.get('SOMATA_PORT', 8000)

class Service:
    def __init__(self, name, methods, options={}):
        @route('/<method_name>.json', method='post')
        def method_route(method_name):
            try:
                method_fn = methods[method_name]
            except Exception as err:
                return {
                    'type': 'error',
                    'data': "No such method %s" % method_name
                }
            try:
                data = method_fn(*request.json['args'])
                return {
                    'type': 'response',
                    'data': data
                }
            except Exception as err:
                return {
                    'type': 'error',
                    'data': str(err)
                }

        run(host='0.0.0.0', port=PORT, server='gevent')

