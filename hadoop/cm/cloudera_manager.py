# -*- coding=utf-8 -*-
# Author: heboan
# Time: 2021/01/18

import requests
from common.base import Uri
import sys


class ClouderaManager(object):

    def __init__(self, service_endpoint=None, auth=None, timeout=None):
        self.timeout = timeout
        self.session = requests.Session()
        self.headers = {"Content-Type": "application/json"}
        self.session.headers.update(self.headers)
        self.server_uri = Uri(service_endpoint) if service_endpoint else None
        self.session.get(service_endpoint + '/version', auth=auth)

    def request(self, api_path):
        api_endpoint = self.server_uri.to_url(api_path)
        response = self.session.get(api_endpoint)
        if response.status_code in (200, 201, 202):
            if not response.content:
                data = {}
            else:
                data = response.json()
            return data
        else:
            msg = "Response finished with status: {status}. Detail: {msg}".format(
                status=response.status_code,
                msg=response.text
            )
            raise Exception(msg)


    def clusterName(self, tag="name"):
        """
        获取集群名
        name: 集群名
        displayName: 自定义的集群名
        """
        api_path = "/api/v14/clusters"

        return self.request(api_path=api_path)['items'][0].get(tag)

    def hosts(self):
        """
        获取所有主机
        """
        api_path = "/api/v14/hosts"
        return self.request(api_path=api_path)


if __name__ == '__main__':
    cm = ClouderaManager(service_endpoint="https://cm.xxxx.com/api", auth=("username", "password"))
    h = cm.clusterName(tag='displayName')
    print(h)