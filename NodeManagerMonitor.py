import sys
sys.path.append('/home/heba/bigdata_manager')

from hadoop.yarn.node_manager import NodeManager
from prom.pushgateway_manager import PushgatewayManager


if __name__ == "__main__":
    with open('/home/heba/done_update_jdk_hosts.txt', 'r') as f:
        for line in f.readlines():
            hostname = line.strip()
            NM = NodeManager(service_endpoint="{}:8042".format(hostname))
            value = NM.node_jmx("Hadoop:service=NodeManager,name=NodeManagerMetrics")['beans'][0]['ContainersFailed']
            job = 'nn-' + hostname

            pm = PushgatewayManager(server='http://pushgateway.heboan.com')
            pm.push(job=job, metric="ContainersFailed", value=value, labels={'hostname': hostname})
            print(hostname + "发送完成--->" + str(value) )





