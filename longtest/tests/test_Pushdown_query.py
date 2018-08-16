# -*- coding: UTF-8 -*-

#!/usr/bin/python

from BeautifulReport import BeautifulReport


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




@ddt.ddt
class pushdownTest(unittest.TestCase):
    base_url = "http://10.1.1.83:7070/kylin/api"
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
        pushdownTest.cur.close()
        pushdownTest.conn.close()
        pass

    @classmethod
    def setUpClass(self):
        # 必须使用@classmethod 装饰器,所有test运行前运行一次
        pass

    # tpch_kap_24
    @ ddt.data(
               ['''{"acceptPartial":true,"limit":50000,"offset":0,"project":"tpch_kap_24","sql":"select   c_count,   count(*) as custdist from   (     select       c_custkey,       count(o_orderkey) as c_count     from       customer left outer join orders on         c_custkey = o_custkey         and o_comment not like '%unusual%accounts%'     group by       c_custkey   ) c_orders group by   c_count order by   custdist desc,   c_count desc;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],#Q13 hive可查
               ['''{"acceptPartial":true,"limit":50000,"offset":0,"project":"tpch_kap_24","sql":"with tmp1 as (     select p_partkey from part where p_name like 'forest%' ), tmp2 as (     select s_name, s_address, s_suppkey     from supplier, nation     where s_nationkey = n_nationkey     and n_name = 'CANADA' ), tmp3 as (     select l_partkey, 0.5 * sum(l_quantity) as sum_quantity, l_suppkey     from lineitem, tmp2     where l_shipdate >= '1994-01-01' and l_shipdate <= '1995-01-01'     and l_suppkey = s_suppkey      group by l_partkey, l_suppkey ), tmp4 as (     select ps_partkey, ps_suppkey, ps_availqty     from partsupp      where ps_partkey IN (select p_partkey from tmp1) ), tmp5 as ( select     ps_suppkey from     tmp4, tmp3 where     ps_partkey = l_partkey     and ps_suppkey = l_suppkey     and ps_availqty > sum_quantity ) select     s_name,     s_address from     supplier where     s_suppkey IN (select ps_suppkey from tmp5) order by s_name;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],# Q20

               # ['''{"acceptPartial":true,"limit":50000,"offset":0,"project":"a","sql":"select * from KYLIN_ACCOUNT","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
               # [''''''],
               # ['''''']
               )
    @ ddt.unpack
    @BeautifulReport.add_test_img('bbbb')

    def testPushdownQuery(self,payload):

        query_url = pushdownTest.base_url + "/query"
        starttime = datetime.now()
        response = requests.request("POST", query_url, data=payload, headers=pushdownTest.headers)
        timeend = datetime.now()
        dtime = (timeend - starttime).seconds
        sql = '''
        INSERT INTO `longtest`.`result`(`id`, `name`, `request`, `respons`, `starttime`, `endtime`, `status`, `date`,`D_time`) VALUES (null, '{namepro}', '', '', '{timestart}', '{timeend}', '{status}','{timeend}', '{D_time}');
        '''
        sql = sql.format(namepro="testPushdownQuery",request1=str(response.url),respons1=str(response.text),timestart = starttime,timeend = timeend,D_time = dtime,status = str(response.status_code))
        pushdownTest.cur.execute(sql)
        pushdownTest.conn.commit()
        time.sleep(random.randint(1,6))
        self.assertEqual(
                         response.status_code,200,"uri = "+response.url+' '+"payload = " + str(payload)
                         +' '+"status_code = "+str(response.status_code)+' '+'response = '+str(response.text)
                         )
        self.assertEqual(
                         json.loads(response.text)['code'],'000',"uri = "+response.url+' '+"payload = " + str(payload)
                         +' '+"status_code = "+str(response.status_code)+' '+'response = '+str(response.text)
                         )






if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.makeSuite(pushdownTest))
    run(test_suite)