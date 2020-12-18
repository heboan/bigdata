# -*- coding=utf-8 -*-
# Author: heboan
# Time: 2020/12/16

from common.base import BaseRequestApi
from hadoop.hadoop_conf import get_active_namenodes, get_active_namenode

class NamenodeManager(BaseRequestApi):
    """
    :param service_endpoints:list
    :parm auth:tuple
    :parm verify:Boolean
    :parm nameservice:str
    """
    def __init__(self, service_endpoints=None, auth=None, timeout=30, verify=True, nameservice=None):
        self.active_service_endpoint = None
        if not service_endpoints:
            self.active_service_endpoint = get_active_namenodes().get(nameservice, {})
        else:
            self.active_service_endpoint = get_active_namenode(service_endpoints)

        if self.active_service_endpoint:
            super(NamenodeManager, self).__init__(service_endpoint=self.active_service_endpoint, auth=auth, timeout=timeout, verify=verify)
        else:
            raise Exception("No active Nms found")


    def fsname_system_state(self):
        api_path = '/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState'
        return self.request(api_path)

    def fsname_system(self):
        api_path = '/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem'
        return self.request(api_path)

    def fsname_mertics(self):
        result = {}
        mertics = [
            'CapacityTotal',      # 集群总容量
            'CapacityRemaining',  # 集群剩余容量
            'CorruptBlocks',      # 损坏的块
            'MissingBlocks',      # 丢失的块
            'BlockCapacity',      # 最大可分配的块
            'BlocksTotal',        # 已分配的块
            'FilesTotal',         # 文件总数
            'TotalLoad',          # 所有DataNode上当前并发文件访问(读写)的数量,高TotalLoad通常会导致作业执行性能下降
            'UnderReplicatedBlocks',  # 副本不足的块数
            'NumLiveDataNodes',       # 活动的DataNode数
            'NumDeadDataNodes',       # 失效的DataNode数
            'NumStaleDataNodes',      # 过时的DataNode数
            'VolumeFailuresTotal'     # 失败的卷数
        ]

        fsname_system_state = self.fsname_system_state()['beans'][0]
        fsname_system = self.fsname_system()['beans'][0]

        for mertic in mertics:
            try:
                result[mertic] = fsname_system_state[mertic]
            except KeyError:
                result[mertic] = fsname_system[mertic]
        return result

    def rpc_activity(self):
        api_path = "jmx?qry=Hadoop:service=NameNode,name=RpcActivityForPort8020"
        result = self.request(api_path)['beans'][0]
        return result

    def jvm_metrics(self):
        api_path = "jmx?qry=Hadoop:service=NameNode,name=JvmMetrics"
        result = self.request(api_path)['beans'][0]
        return result


if __name__ == '__main__':
     nm = NamenodeManager(nameservice='dev1')
     print(nm.jvm_metrics())