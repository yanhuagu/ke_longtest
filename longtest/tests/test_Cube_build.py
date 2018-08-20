# -*- coding: UTF-8 -*-

#!/usr/bin/python
from BeautifulReport import BeautifulReport

import unittest
from datetime import datetime
from nose.plugins.plugintest import run_buffered as run

import ddt as ddt
import pymysql
import requests
import json
import time
import os


START_TIME = 0
END_TIME = 0
# 0 -> normal run, 1 -> run to failure
RUN_MODE = 0

# def killYarnApplications():
#     appListOutput = os.popen('yarn application -list')
#     for line in appListOutput.readlines():
#         for str in line.split():
#             if str.startswith('application_'):
#                 os.popen('yarn application -kill ' + str)
#                 print ('Kill app id: ' + str)
#     print ('Kill yarn app.')


def disableCube(cubename):
    disableCube_url = cubeTest.base_url + "/cubes/"+str(cubename)+"/disable"
    response = requests.request("put", disableCube_url, headers=cubeTest.headers)
    return response.status_code

def enableCube(cubename):
    enableCube_url = cubeTest.base_url + "/cubes/"+str(cubename)+"/enable"
    response = requests.request("put", enableCube_url, headers=cubeTest.headers)
    return response.status_code



@ddt.ddt
class cubeTest(unittest.TestCase):
    base_url = "http://10.1.1.83:7070/kylin/api"
    headers = {
        'content-type': "application/json",
        'authorization': "Basic QURNSU46S1lMSU4=",
        'cache-control': "no-cache",
        'accept': "application/vnd.apache.kylin-v2+json"
    }

    conn = pymysql.connect(host='10.1.40.102', port=3306, user='root', passwd='root123', db='longtest')
    cur = conn.cursor()

    def tearDown(self):
        # 每个测试用例执行之后做操作

        pass

    def setUp(self):
        # 每个测试用例执行之前做操作
        pass

    @classmethod
    def tearDownClass(self):
        # 必须使用 @ classmethod装饰器, 所有test运行完后运行一次
        cubeTest.cur.close()
        cubeTest.conn.close()
        pass

    @classmethod
    def setUpClass(self):
        # 必须使用@classmethod 装饰器,所有test运行前运行一次
        pass




    @BeautifulReport.add_test_img('bbbb')

    def testCubeBuild(self):
        """
            构建date range为一天的segment
        """
        starttime = datetime.now()

        url = cubeTest.base_url + "/cubes/kylin_sales_cube/segments/build"
        # payload = '''{"buildType": "BUILD", "startTime": '''+str(time.time()*1000-694224000000)+''', "endTime": '''+str(time.time()*1000-694381945000)+''', "mpValues": "","project": "ssb"}
        # '''
        payload = '''{"buildType": "BUILD", "startTime": '''+str((int(round(time.time() * 1000))-209060570206))+''', "endTime": '''+str((int(round(time.time() * 1000))-208974170206))+''', "mpValues": "","project": "learn_kylin"}'''

        status_code = 0
        try_time = 1
        while status_code != 200 and try_time <= 1:
            print ('Submit build job, try_time = ' + str(try_time))
            try:
                response = requests.request("PUT", url, data=payload, headers=cubeTest.headers)
                status_code = response.status_code
            except:
                status_code = 0
                pass
            if status_code != 200:
                time.sleep(60)
                try_time += 1

        self.assertEqual(status_code, 200, 'Build job submitted failed.'+"uri = "+response.url+' '+"********  payload  ********  " + str(payload)
                         +' '+"********  status_code  ******** "+str(response.status_code)+' '+'********  response  ********  '+str(response.text)
                         )

        if status_code == 200:
            # print 'Build job is submitted...'
            job_response = json.loads(response.text)
            job_uuid = job_response['data']['uuid']
            job_url = cubeTest.base_url + "/jobs/" + job_uuid
            job_response = requests.request("GET", job_url, headers=cubeTest.headers)

            self.assertEqual(job_response.status_code, 200, 'Build job information fetched failed.'+"uri = "+response.url+' '+"********  payload  ********  " + str(payload)
                         +' '+"********  status_code  ******** "+str(response.status_code)+' '+'********  response  ********  '+str(response.text)
                         )

            job_info = json.loads(job_response.text)
            job_status = job_info['data']['job_status']
            try_time = 1
            total_try_time = 6000
            # print 'Run mode: ' + str(RUN_MODE)
            if RUN_MODE == '1':
                total_try_time = 5
            while job_status in ('RUNNING', 'PENDING') and try_time <= total_try_time:
                # print 'Wait for job complete, try_time = ' + str(try_time)
                try:
                    job_response = requests.request("GET", job_url, headers=cubeTest.headers)
                    job_info = json.loads(job_response.text)
                    job_status = job_info['data']['job_status']
                except:
                    job_status = 'UNKNOWN'
                    pass
                if job_status in ('RUNNING', 'PENDING', 'UNKNOWN'):
                    time.sleep(60)
                    try_time += 1

            if RUN_MODE == '1':
                while job_status in ('RUNNING'):
                    job_response = requests.request("GET", job_url, headers=cubeTest.headers)
                    job_info = json.loads(job_response.text)
                    job_status = job_info['data']['job_status']
                    # killYarnApplications()
                self.assertEquals(job_status, 'ERROR', 'Build cube failed, job status is ' + job_status+"uri = "+response.url+' '+"********  payload  ********  " + str(payload)
                         +' '+"********  status_code  ******** "+str(response.status_code)+' '+'********  response  ********  '+str(response.text)
                         )
            else:
                self.assertEquals(job_status, 'FINISHED', 'Build cube failed, job status is ' + job_status+"uri = "+response.url+' '+"********  payload  ********  " + str(payload)
                         +' '+"********  status_code  ******** "+str(response.status_code)+' '+'********  response  ********  '+str(response.text)
                         )
            # print 'Job complete.'
            timeend = datetime.now()
            dtime = (timeend - starttime).seconds
            sql = '''
            INSERT INTO `longtest`.`result`(`id`, `name`, `request`, `respons`, `starttime`, `endtime`, `status`, `date`,`D_time`) VALUES (null, '{namepro}', '', '', '{timestart}', '{timeend}', '{status}','{timeend}', '{D_time}');
            '''
            sql = sql.format(namepro="testBuild", request1=str(response.url), respons1=str(response.text),
                             timestart=starttime, timeend=timeend, D_time=dtime, status=str(response.status_code))





if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.makeSuite(cubeTest))
    run(test_suite)

    # result = BeautifulReport(test_suite)
    # result.report(filename='Longtest Report', description='Longtest Report', log_path='.')


