import json
import warnings

import requests
from requests.auth import HTTPBasicAuth


class RMQ:
    def __init__(self, ip_address, port, virtual_host, username, password):
        self.host = ip_address,
        self.port = port,
        self.virtual_host = virtual_host,
        self.username = username
        self.password = password
        self.exchange = 'amq.default'

    def _api_get(self, api_path):
        assert api_path.startswith('/api/')

        r = requests.get(f'http://{self.host}:{self.port}/{api_path[1:]}',
                         auth=HTTPBasicAuth(self.username, self.password))

        return r.json()

    def _api_post(self, api_path, post_json_payload):
        assert api_path.startswith('/api/')

        r = requests.get(f'http://{self.host}:{self.port}/{api_path[1:]}',
                         auth=HTTPBasicAuth(self.username, self.password),
                         json=post_json_payload)

        return r.json()

    def get_queue_info(self, queue_name):
        return self._api_get(f'/api/queues/{self.virtual_host}/{queue_name}')

    def write_json(self, queue_name, json_obj):
        payload = {'properties':       {},
                   'routing_key':      queue_name,
                   'payload':          json.dumps(json_obj, ensure_ascii=False),
                   'payload_encoding': 'string',
                   }
        return self._api_post(f'/api/exchanges/{self.virtual_host}/{self.exchange}/publish', payload)

    def write_jsons(self, queue_name, json_iterator):
        for json_obj in json_iterator:
            self.write_json(json_obj)

    def is_alive(self):
        result = self._api_get(f'/api/aliveness-test/{self.virtual_host}')
        return result.get('status', None) == 'ok'

    def health_check(self):
        result = self._api_get(f'/api/healthchecks/node')
        if result.get('status', None) == 'ok':
            return True

        warning_message = result.get('reason', None)
        if warning_message is not None:
            warnings.warn(warning_message)

        return False
