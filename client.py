import requests
import os

DNS_SUFFIX = os.environ.get('SOMATA_DNS_SUFFIX')

class Client:
    def __init__(self, service, options={}):
        service_url = service
        if DNS_SUFFIX:
            service_url += '.' + DNS_SUFFIX

        self.base_url = 'http://%s/' % service_url

    def request(self, method, *args):
        method_url = self.base_url + '%s.json' % method
        data = {'args': args}
        request = requests.post(method_url, json=data)
        response = request.json()

        if type(response) == dict:
            if 'type' and 'data' in response:
                if response['type'] == 'response':
                    return response['data']
                elif response['type'] == 'error':
                    raise Exception(response['data'])
            else:
                print('Invalid response object', response)
                return None

        else:
            print('Invalid response format', response)
            return None

