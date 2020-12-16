# -*- coding=utf-8 -*-
# Author: heboan
# Time: 2020/12/15

from common.base import BaseRequestApi
from hadoop.yarn.constants import ApplicationState


LEGAL_APPLICATION_STATES = {s for s, _ in ApplicationState}


def validate_application_state(state):

    if state:
        if state not in LEGAL_APPLICATION_STATES:
            msg = 'Application State {} 是非法的'.format(state)
            raise Exception(msg)
    else:
        msg = "state argument is required to be provided"
        raise Exception(msg)


class NodeManager(BaseRequestApi):

    def node_information(self):
        api_path = '/ws/v1/node/info'
        return self.request(api_path=api_path, doc_type='xml')

    def node_applications(self, state=None, user=None):
        validate_application_state(state)
        api_path = '/ws/v1/node/apps'
        local_args = (('state', state), ('user', user))
        params = self.construct_parameters(local_args)
        return self.request(api_path=api_path, doc_type='xml', params=params)

    def node_containers(self):
        api_path = '/ws/v1/node/containers'
        return self.request(api_path=api_path, doc_type='xml')

    def node_container(self, container_id):
        api_path = '/ws/v1/node/containers/{}'.format(container_id)
        return self.request(api_path=api_path, doc_type='xml')

    def node_jmx(self, qry=None):
        # http://xxxxxxx:8042/jmx?qry=Hadoop:service=NodeManager,name=NodeManagerMetrics
        api_path = '/jmx'
        if qry:
            api_path = '/jmx?qry={}'.format(qry)
        return self.request(api_path=api_path)



if __name__ == "__main__":
    NM = NodeManager(service_endpoint="172.17.8.225:8042")
    info = NM.node_jmx("Hadoop:service=NodeManager,name=NodeManagerMetrics")
    print(info)
