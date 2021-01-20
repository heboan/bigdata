from prometheus_client import CollectorRegistry, Gauge, Counter,Histogram, Summary, pushadd_to_gateway
from prom.constants import MetricType


MetricTypes = {s for s, _ in MetricType}


def validate_mertric_type(metric_type, required=True):
    """
    根据metric类型返回metric函数
    """
    if metric_type:
        if metric_type not in MetricTypes:
            msg = 'Prometheus Metric Type {} 非法'.format(metric_type)
            raise Exception(msg)

        if metric_type == 'Gauge':
            return Gauge
        elif metric_type == 'Counter':
            return Counter
        elif metric_type == 'Histogram':
            return Histogram
        else:
            return Summary
    else:
        if required:
            msg = "metric_type argument is required to be provided"
            raise Exception(msg)


class PushgatewayManager(object):
    def __init__(self, server):
        self.registry = CollectorRegistry()
        self.server = server

    def push(self, job, metric, value,labels, metric_type='Gauge', metric_desc='metric'):
        # 获取指标类型函数
        metricFunc = validate_mertric_type(metric_type)

        # labels必须是字典类型，如{'hostname': 'c1.heboan.com', 'ip': '192.168.88.1'}
        if not isinstance(labels, dict):
            msg = "tag parameter must be of dict type"
            raise Exception(msg)

        f = metricFunc(metric, metric_desc, labels.keys(), registry=self.registry)
        f.labels(*labels.values()).set(value)

        # f = metricFunc(metric, metric_desc, ['hostname', 'ip'], registry=self.registry)
        #f.labels('c1.heboan.com', '192.168.1.100').set(value)

        # job和metric都是一样的会，值会被覆盖，一般一个job里面关联多个metric
        pushadd_to_gateway(self.server, job=job, registry=f, timeout=200)


if __name__ == '__main__':
    pm = PushgatewayManager(server='192.168.88.1:9091')
    labels = {'hostname':'c1.heboan.com', 'ip':'192.168.1.100'}
    pm.push(job="nn-192.168.88.1",metric="loveyou",value=30,labels=labels)
