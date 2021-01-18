# -*- coding=utf-8 -*-
# Author: heboan
# Time: 2020/12/15

import os
import xml.etree.ElementTree as ET
import requests
from common.base import BaseRequestApi


# CONF_DIR = os.getenv('HADOOP_CONF_DIR', '/etc/hadoop/conf')
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = curPath[:curPath.find("bigdata_monitor/")+len("bigdata_monitor/")]
CONF_DIR = os.path.abspath(rootPath + 'hadoop/conf')


def _get_rm_ids(hadoop_conf_path):
    rm_ids = parse(os.path.join(hadoop_conf_path, 'yarn-site.xml'), 'yarn.resourcemanager.ha.rm-ids')
    if rm_ids is not None:
        rm_ids = rm_ids.split(',')
    return rm_ids


def _is_https_only():
    # determine if HTTPS_ONLY is the configured policy, else use http
    hadoop_conf_path = CONF_DIR
    http_policy = parse(os.path.join(hadoop_conf_path, 'yarn-site.xml'), 'yarn.http.policy')
    if http_policy == 'HTTPS_ONLY':
        return True
    return False


def _get_resource_manager(hadoop_conf_path, rm_id=None):
    # compose property name based on policy (and rm_id)
    is_https_only = _is_https_only()

    if is_https_only:
        prop_name = 'yarn.resourcemanager.webapp.https.address'
    else:
        prop_name = 'yarn.resourcemanager.webapp.address'

    # Adjust prop_name if rm_id is set
    if rm_id:
        prop_name = "{name}.{rm_id}".format(name=prop_name, rm_id=rm_id)

    rm_address = parse(os.path.join(hadoop_conf_path, 'yarn-site.xml'), prop_name)

    return ('https://' if is_https_only else 'http://') + rm_address if rm_address else None


def check_is_active_rm(url, timeout=30, auth=None, verify=True):
    try:
        res = requests.get(url + '/cluster', timeout=timeout, auth=auth, verify=verify)
    except requests.RequestException as e:
        # log.warning("Exception encountered accessing RM '{url}': '{err}', continuing...".format(url=url, err=e))
        return False

    if res.status_code != 200:
        # log.warning("Failed to access RM '{url}' - HTTP Code '{status}', continuing...".format(url=url, status=response.status_code))
        return False
    else:
        return True


def get_active_resource_manager(timeout=30, auth=None, verify=True):
    # log.info('Getting resource manager endpoint from config: {config_path}'.format(config_path=os.path.join(CONF_DIR, 'yarn-site.xml')))
    hadoop_conf_path = CONF_DIR
    rm_ids = _get_rm_ids(hadoop_conf_path)
    if rm_ids:
        for rm_id in rm_ids:
            ret = _get_resource_manager(hadoop_conf_path, rm_id)
            if ret:
                if check_is_active_rm(ret, timeout, auth, verify):
                    return ret
        return None
    else:
        return _get_resource_manager(hadoop_conf_path, None)


# hdfs

def _get_nameservices(hadoop_conf_path):
    nameservices = parse(os.path.join(hadoop_conf_path, 'hdfs-site.xml'), 'dfs.nameservices')
    if nameservices is not None:
        nameservices = nameservices.split(',')
    return nameservices


def _get_namenodes(hadoop_conf_path, nameservice=None):

    namenodes = []
    prop_ha_name = ""
    if nameservice:
        prop_ha_name = "dfs.ha.namenodes.{nameservice}".format(nameservice=nameservice)

    nn_tags = parse(os.path.join(hadoop_conf_path, 'hdfs-site.xml'), prop_ha_name).split(',')
    for nn_tag in nn_tags:
        prop_rpc_name = "{name}.{nameservice}.{nn_tag}".format(name="dfs.namenode.http-address",
                                                               nameservice=nameservice,
                                                               nn_tag=nn_tag)
        namenode = parse(os.path.join(hadoop_conf_path, 'hdfs-site.xml'), prop_rpc_name)
        namenodes.append(namenode)
    return namenodes


def get_active_namenode(namenodes):
    assert isinstance(namenodes, list)
    for nn in namenodes:
        # service_endpoint = '{namenode}:50070'.format(namenode=nn)
        api_path = '/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus'
        bq = BaseRequestApi(service_endpoint=nn, timeout=30)
        try:
            res = bq.request(api_path)
            if res['beans'][0]['State'] == "active":
                return nn
        except  requests.exceptions.ConnectionError:
            pass
    return None


def get_active_namenodes():
    """

    :return:  {"nameservice": "active_namenode", ...}
    """

    active_namenodes = {}
    hadoop_conf_path = CONF_DIR
    nameservices = _get_nameservices(hadoop_conf_path)
    if nameservices:
        for nameservice in nameservices:
            ret = _get_namenodes(hadoop_conf_path, nameservice)
            if ret:
                nn = get_active_namenode(ret)
                if nn:
                    active_namenodes[nameservice] = nn
    return active_namenodes


def parse(config_path, key):
    tree = ET.parse(config_path)
    root = tree.getroot()
    # Construct list with profit values
    ph1 = [dict((el.tag, el.text) for el in p) for p in root.findall('./property')]
    # Construct dict with property key values
    ph2 = dict((obj['name'], obj['value']) for obj in ph1)

    value = ph2.get(key, None)
    return value


if __name__ == '__main__':
    pass