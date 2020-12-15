# -*- coding=utf-8 -*-
# Author: heboan
# Time: 2020/12/14

import time
from common.base import BaseRequestApi


class AwxManager(BaseRequestApi):

    def __init__(self, service_endpoint=None, auth=None, timeout=30, verify=True):
        super(AwxManager, self).__init__(service_endpoint, auth, timeout, verify)

    def job_launch(self, template_id, **kwargs):
        api_path = "/api/v2/job_templates/{0}/launch/".format(template_id)
        data = {}
        if 'data' in kwargs and kwargs['data']:
            data = kwargs['data']
        job_id = self.request(api_path=api_path, method='POST', json=data)['id']
        return job_id

    def job_status(self, job_id):
        api_path = "/api/v2/jobs/{}/".format(job_id)
        status = 'None'
        # 判断任务有没有运行结束，每20s一个周期，10*20s后会判定任务运行失败
        for i in range(10):
            try:
                res = self.request(api_path)
                if res.get('finished'):
                    status = res.get('status')
                    break
                time.sleep(20)
            except Exception as err:
                status = 'failed'
                break
        return status


if __name__ == '__main__':
    data = {
        "extra_vars": {
            'host_list': 'aliyun',
            'domain': 'heboan',
            'nginx_snippet': 'snippet..',
            'type': 'change'
        }
    }

    awx = AwxManager(service_endpoint='', auth=('username', 'password'))
    awx.job_launch(template_id='template_id', data=data)