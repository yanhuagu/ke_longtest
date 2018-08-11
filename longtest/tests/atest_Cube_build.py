# -*- coding: UTF-8 -*-

#!/usr/bin/python
from BeautifulReport import BeautifulReport

import HTMLTestRunner

import random
import unittest
from datetime import datetime

import ddt as ddt
import pymysql
import requests
import json
import time
import sys
import os
from nose.plugins.plugintest import run_buffered as run
from nose.plugins.multiprocess import MultiProcess


START_TIME = 0
END_TIME = 0
# 0 -> normal run, 1 -> run to failure
RUN_MODE = 0

def killYarnApplications():
    appListOutput = os.popen('yarn application -list')
    for line in appListOutput.readlines():
        for str in line.split():
            if str.startswith('application_'):
                os.popen('yarn application -kill ' + str)
                print ('Kill app id: ' + str)
    print ('Kill yarn app.')


def disableCube(cubename):
    disableCube_url = longTest.base_url + "/cubes/"+str(cubename)+"/disable"
    response = requests.request("put", disableCube_url, headers=longTest.headers)
    return response.status_code

def enableCube(cubename):
    enableCube_url = longTest.base_url + "/cubes/"+str(cubename)+"/enable"
    response = requests.request("put", enableCube_url, headers=longTest.headers)
    return response.status_code



@ddt.ddt
class longTest(unittest.TestCase):
    base_url = "http://10.1.2.108:7070/kylin/api"
    headers = {
        'content-type': "application/json",
        'authorization': "Basic QURNSU46S1lMSU4=",
        'cache-control': "no-cache",
        'accept': "application/vnd.apache.kylin-v2+json"
    }

    conn = pymysql.connect(host='10.1.40.100', port=3306, user='root', passwd='root123', db='longtest')
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
        longTest.cur.close()
        longTest.conn.close()
        pass

    @classmethod
    def setUpClass(self):
        # 必须使用@classmethod 装饰器,所有test运行前运行一次
        pass





    def testBuild(self):

        starttime = datetime.now()

        url = longTest.base_url + "/cubes/b/segments/build"
        payload = '''{"buildType": "BUILD", "startTime": '''+str(time.time()*1000-648911117000)+''', "endTime": '''+str(time.time()*1000-648675779000)+''', "mpValues": "","project": "test256to310"}
        '''
        status_code = 0
        try_time = 1
        while status_code != 200 and try_time <= 3:
            print ('Submit build job, try_time = ' + str(try_time))
            try:
                response = requests.request("PUT", url, data=payload, headers=longTest.headers)
                status_code = response.status_code
            except:
                status_code = 0
                pass
            if status_code != 200:
                time.sleep(60)
                try_time += 1

        self.assertEqual(status_code, 200, 'Build job submitted failed.')

        if status_code == 200:
            # print 'Build job is submitted...'
            job_response = json.loads(response.text)
            job_uuid = job_response['data']['uuid']
            job_url = longTest.base_url + "/jobs/" + job_uuid
            job_response = requests.request("GET", job_url, headers=longTest.headers)

            self.assertEqual(job_response.status_code, 200, 'Build job information fetched failed.')

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
                    job_response = requests.request("GET", job_url, headers=longTest.headers)
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
                    job_response = requests.request("GET", job_url, headers=longTest.headers)
                    job_info = json.loads(job_response.text)
                    job_status = job_info['data']['job_status']
                    killYarnApplications()
                self.assertEquals(job_status, 'ERROR', 'Build cube failed, job status is ' + job_status)
            else:
                self.assertEquals(job_status, 'FINISHED', 'Build cube failed, job status is ' + job_status)
            # print 'Job complete.'
            timeend = datetime.now()
            dtime = (timeend - starttime).seconds
            sql = '''
            INSERT INTO `longtest`.`result`(`id`, `name`, `request`, `respons`, `starttime`, `endtime`, `status`, `date`,`D_time`) VALUES (null, '{namepro}', '', '', '{timestart}', '{timeend}', '{status}','{timeend}', '{D_time}');
            '''
            sql = sql.format(namepro="testBuild", request1=str(response.url), respons1=str(response.text),
                             timestart=starttime, timeend=timeend, D_time=dtime, status=str(response.status_code))







if __name__ == '__main__':
    RUN_MODE = sys.argv.pop()
    END_TIME = sys.argv.pop()
    START_TIME = sys.argv.pop()
    suite = unittest.TestSuite()
    # tests = [longTest("testBuild"),longTest("testQuery")]
    tests = [longTest("testQuery")]
    #
    suite.addTests(tests)


    # filename = str(time.time())+"test_result.html"
    filename = "Users/yanhua.gu/work/code/ke_longtest/longtest/test_result.html"

    with open(filename, 'w') as fp:
        runner = HTMLTestRunner.HTMLTestRunner(
            stream=fp,
            title='Test Report',
            description='Generated by HTMLTestRunner',
            verbosity = 2)
        run(suite=suite, argv=['nosetests', '-v', '--processes=2'], plugins=[MultiProcess()])
        runner.run()
