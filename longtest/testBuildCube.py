#!/usr/bin/python

import unittest
import requests
import json
import time
import sys
import os

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
                print 'Kill app id: ' + str
    print 'Kill yarn app.'

class testBuildCube(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testBuild(self):
        base_url = "http://10.1.2.108:7070/kylin/api"
        headers = {
            'content-type': "application/json",
            'authorization': "Basic QURNSU46S1lMSU4=",
            'cache-control': "no-cache",
            'accept': "application/vnd.apache.kylin-v2+json"
        }

        url = base_url + "/cubes/b/segments/build"


        # reload metadata before build cubes
        # cache_response = requests.request("PUT", base_url + "/cache/all/all/update", headers=headers)
        # self.assertTrue(long(END_TIME) > long(START_TIME))
        # self.assertEqual(cache_response.status_code, 200, 'Metadata cache not refreshed.')

        # payload = "{\"startTime\": " + START_TIME + ", \"endTime\": " + END_TIME + ", \"buildType\":\"BUILD\"}"
        payload = '''{"buildType": "BUILD", "startTime": '''+str(time.time()*1000-648911117000)+''', "endTime": '''+str(time.time()*1000-648675779000)+''', "mpValues": "","project": "test256to310"}
        '''

        status_code = 0
        try_time = 1
        while status_code != 200 and try_time <= 3:
            print 'Submit build job, try_time = ' + str(try_time)
            try:
                response = requests.request("PUT", url, data=payload, headers=headers)
                status_code = response.status_code
            except:
                status_code = 0
                pass
            if status_code != 200:
                time.sleep(60)
                try_time += 1

        self.assertEqual(status_code, 200, 'Build job submitted failed.')

        if status_code == 200:
            print 'Build job is submitted...'
            job_response = json.loads(response.text)
            job_uuid = job_response['data']['uuid']
            job_url = base_url + "/jobs/" + job_uuid
            job_response = requests.request("GET", job_url, headers=headers)

            self.assertEqual(job_response.status_code, 200, 'Build job information fetched failed.')

            job_info = json.loads(job_response.text)
            job_status = job_info['data']['job_status']
            try_time = 1
            total_try_time = 60
            print 'Run mode: ' + str(RUN_MODE)
            if RUN_MODE == '1':
                total_try_time = 5
            while job_status in ('RUNNING', 'PENDING') and try_time <= total_try_time:
                print 'Wait for job complete, try_time = ' + str(try_time)
                try:
                    job_response = requests.request("GET", job_url, headers=headers)
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
                    job_response = requests.request("GET", job_url, headers=headers)
                    job_info = json.loads(job_response.text)
                    job_status = job_info['data']['job_status']
                    killYarnApplications()
                self.assertEquals(job_status, 'ERROR', 'Build cube failed, job status is ' + job_status)
            else:
                self.assertEquals(job_status, 'FINISHED', 'Build cube failed, job status is ' + job_status)

            print 'Job complete.'

if __name__ == '__main__':
    print 'Test Build Cube for Kylin sample.'
    RUN_MODE = sys.argv.pop()
    END_TIME = sys.argv.pop()
    START_TIME = sys.argv.pop()
    unittest.main()
