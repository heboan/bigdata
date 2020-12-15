# -*- coding=utf-8 -*-
# Author: heboan
# Time: 2020/12/14

import requests
import xmltodict
from urllib.parse import urlparse, urlunparse



class Uri(object):
    def __init__(self, service_endpoint):
        if not (service_endpoint.startswith("http://") or service_endpoint.startswith("https://")):
            service_endpoint = "http://" + service_endpoint

        service_uri = urlparse(service_endpoint)
        self.scheme = service_uri.scheme or 'http'
        self.hostname = service_uri.hostname or service_uri.path
        self.port = service_uri.port
        self.is_https = service_uri.scheme == 'https' or False

    def to_url(self, api_path=None):
        path = api_path or ''
        if self.port:
            result_url = urlunparse((self.scheme, self.hostname + ":" + str(self.port), path, None, None, None))
        else:
            result_url = urlunparse((self.scheme, self.hostname, path, None, None, None))

        return result_url


class BaseRequestApi(object):

    def __init__(self,  service_endpoint=None, auth=None, timeout=None, verify=True):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.auth = auth
        self.session.verify = verify
        self.server_uri = Uri(service_endpoint) if service_endpoint else None

    def _validate_configuration(self):
        if not self.server_uri:
            raise Exception('server_uri is required')

    def request(self, api_path, method='GET', **kwargs):
        self._validate_configuration()
        headers = {} if method == 'GET' else {"Content-Type": "application/json"}
        xml_headers = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
        if 'doc_type' in kwargs and kwargs['doc_type'] == 'xml':
            headers = xml_headers
            kwargs.pop('doc_type')

        if 'headers' in kwargs and kwargs['headers']:
            headers.update(kwargs['headers'])

        api_endpoint = self.server_uri.to_url(api_path)
        response = self.session.request(method=method, url=api_endpoint, headers=headers, timeout=self.timeout, **kwargs)
        if response.status_code in (200,201,202):
            if not response.content:
                data = {}
            elif headers == xml_headers:
                data =  xmltodict.parse(response.text)
            else:
                data = response.json()
            return data
        else:
            msg = "Response finished with status: {status}. Detail: {msg}".format(
                status=response.status_code,
                msg=response.text
            )
            raise Exception(msg)

    def construct_parameters(self, arguments):
        # (('a': 1), ('b': 2)) -> {'a': 1, 'b': 2}
        params = dict((key, value) for key, value in arguments if value is not None)
        return params





