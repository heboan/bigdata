# -*- coding=utf-8 -*-
# Author: heboan
# Time: 2020/12/15

from hadoop.yarn.constants import YarnApplicationState, FinalApplicationStatus, ClusterContainerSignal
from common.base import BaseRequestApi
from hadoop.hadoop_conf import get_active_resource_manager, check_is_active_rm


LEGAL_STATES = {s for s, _ in YarnApplicationState}
LEGAL_FINAL_STATUSES = {s for s, _ in FinalApplicationStatus}
LEGAL_CLUSTER_CONTAINER_STATUSES = {s for s, _ in ClusterContainerSignal}


def validate_yarn_application_state(state, required=False):
    if state:
        if state not in LEGAL_STATES:
            msg = 'Yarn Application State %s 非法' % (state,)
            raise Exception(msg)
    else:
        if required:
            msg = "state argument is required to be provided"
            raise Exception(msg)


def validate_yarn_application_states(states, required=False):
    if states:
        if not isinstance(states, list):
            msg = "States should be list"
            raise Exception(msg)

        illegal_states = set(states) - LEGAL_STATES
        if illegal_states:
            msg = 'Yarn Application States %s 非法' % (
                ",".join(illegal_states),
            )
            raise Exception(msg)
    else:
        if required:
            msg = "states argument is required to be provided"
            raise Exception(msg)


def validate_final_application_status(final_status, required=False):
    if final_status:
        if final_status not in LEGAL_FINAL_STATUSES:
            msg = 'Final Application Status %s 非法' % (final_status,)
            raise Exception(msg)
    else:
        if required:
            msg = "final_status argument is required to be provided"
            raise Exception(msg)


def validate_cluster_container_status(cluster_container_status, required=False):
    if cluster_container_status:
        if cluster_container_status not in LEGAL_CLUSTER_CONTAINER_STATUSES:
            msg = 'Cluster Container Status %s 非法' % (cluster_container_status,)
            raise Exception(msg)
    else:
        if required:
            msg = "cluster_container_status argument is required to be provided"
            raise Exception(msg)


class ResourceManager(BaseRequestApi):
    def __init__(self, service_endpoints=None, auth=None, timeout=30, verify=True):
        active_service_endpoint = None
        if not service_endpoints:
            active_service_endpoint = get_active_resource_manager(timeout, auth, verify)
        else:
            for endpoint in service_endpoints:
                if check_is_active_rm(endpoint, timeout, auth, verify):
                    active_service_endpoint = endpoint
                    break

        if active_service_endpoint:
            super(ResourceManager, self).__init__(service_endpoint=active_service_endpoint, auth=auth, timeout=30, verify=verify)
        else:
            raise Exception("No active RMs found")

    def get_active_url(self):
        pass

    def cluster_infomation(self):
        api_path = '/ws/v1/cluster/info'
        return self.request(api_path=api_path, doc_type='xml')

    def cluster_metrics(self):
        api_path = '/ws/v1/cluster/metrics'
        return self.request(api_path=api_path, doc_type='xml')

    def cluster_scheduler(self):
        api_path = '/ws/v1/cluster/scheduler'
        return self.request(api_path=api_path, doc_type='xml')

    def cluster_applications(self, state=None, states=None,
                             final_status=None, user=None,
                             queue=None, limit=None,
                             started_time_begin=None, started_time_end=None,
                             finished_time_begin=None, finished_time_end=None,
                             application_types=None, application_tags=None,
                             name=None, de_selects=None):
        api_path = '/ws/v1/cluster/apps'

        validate_yarn_application_state(state)
        validate_yarn_application_states(states)
        validate_final_application_status(final_status)

        loc_args = (
            ('state', state),
            ('states', ','.join(states) if states else None),
            ('finalStatus', final_status),
            ('user', user),
            ('queue', queue),
            ('limit', limit),
            ('startedTimeBegin', started_time_begin),
            ('startedTimeEnd', started_time_end),
            ('finishedTimeBegin', finished_time_begin),
            ('finishedTimeEnd', finished_time_end),
            ('applicationTypes', ','.join(application_types) if application_types else None),
            ('applicationTags', ','.join(application_tags) if application_tags else None),
            ('name', name),
            ('deSelects', ','.join(de_selects) if de_selects else None)
        )

        params = self.construct_parameters(loc_args)
        return self.request(api_path, params=params)

    def cluster_application(self, application_id):
        path = '/ws/v1/cluster/apps/{appid}'.format(appid=application_id)
        return self.request(path)



if __name__ == '__main__':
    pass