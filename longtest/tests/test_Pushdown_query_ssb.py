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
class pushdownTest_ssb(unittest.TestCase):
    base_url = "http://10.1.1.83:7070/kylin/api"
    # base_url = "http://10.1.40.104:7298/kylin/api"

    headers = {
        'content-type': "application/json",
        # 'authorization': "Basic QURNSU46S1lMSU4=",
        'authorization': "Basic YWRtaW46YWRtaW5AMTIz",
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
        pushdownTest_ssb.cur.close()
        pushdownTest_ssb.conn.close()
        pass

    @classmethod
    def setUpClass(self):
        # 必须使用@classmethod 装饰器,所有test运行前运行一次
        pass

    # tpch_kap_24
    @ ddt.data(

        ##learn kylin
        #
        # [
        #     '''{"acceptPartial":true,"limit":"5","offset":0,"project":"learn_kylin","sql":"select lstg_format_name, sum(price) as GMV from kylin_sales where lstg_format_name='FP-GTC' group by lstg_format_name","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
        # ['''{"acceptPartial":true,"limit":"5","offset":0,"project":"learn_kylin","sql":"select sum(price) as GMV, count(1) as TRANS_CNT from kylin_sales","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
        # ['''{"acceptPartial":true,"limit":"5","offset":0,"project":"learn_kylin","sql":"select kylin_sales.lstg_format_name, sum(price) as GMV, count(*) as TRANS_CNT from kylin_sales group by kylin_sales.lstg_format_name;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
        # ['''{"acceptPartial":true,"limit":"5","offset":0,"project":"learn_kylin","sql":"select kylin_sales.lstg_format_name,sum(kylin_sales.price) as GMV, count(*) as TRANS_CNT from kylin_sales group by kylin_sales.lstg_format_name having sum(price)>5000","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
        # ['''{"acceptPartial":true,"limit":"5","offset":0,"project":"learn_kylin","sql":"select kylin_sales.lstg_format_name,sum(kylin_sales.price) as GMV, count(*) as TRANS_CNT from kylin_sales where kylin_sales.lstg_format_name is null group by kylin_sales.lstg_format_name having sum(price)>5000 and count(*)>72 ","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
        # # ['''{"acceptPartial":true,"limit":"5","offset":0,"project":"learn_kylin","sql":"select kylin_cal_dt.week_beg_dt,sum(kylin_sales.price) as GMV  , count(*) as TRANS_CNT , sum(kylin_sales.item_count) as total_items from kylin_sales  inner JOIN kylin_cal_dt ON kylin_sales.part_dt = kylin_cal_dt.cal_dt inner JOIN kylin_category_groupings ON kylin_sales.leaf_categ_id = kylin_category_groupings.leaf_categ_id AND kylin_sales.lstg_site_id = kylin_category_groupings.site_id where kylin_sales.lstg_format_name='FP-GTC'  and kylin_cal_dt.week_beg_dt between DATE '2013-05-01' and DATE '2013-08-01' group by kylin_cal_dt.week_beg_dt;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
        # # ['''{"acceptPartial":true,"limit":"5","offset":0,"project":"learn_kylin","sql":"SELECT  kylin_cal_dt.week_beg_dt  ,kylin_category_groupings.meta_categ_name  ,sum(price) as GMV, count(*) as TRANS_CNT , sum(kylin_sales.item_count) as total_items FROM kylin_sales  inner JOIN kylin_cal_dt ON kylin_sales.part_dt = kylin_cal_dt.cal_dt inner JOIN kylin_category_groupings ON kylin_sales.leaf_categ_id = kylin_category_groupings.leaf_categ_id AND kylin_sales.lstg_site_id = kylin_category_groupings.site_id where kylin_cal_dt.week_beg_dt between DATE '2013-09-01' and DATE '2013-10-01'  and kylin_category_groupings.categ_lvl3_name='Other'  group by kylin_cal_dt.week_beg_dt  ,kylin_category_groupings.meta_categ_name;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
        # ['''{"acceptPartial":true,"limit":"5","offset":0,"project":"learn_kylin","sql":"select count(*) as cnt from kylin_sales where lstg_format_name>='AAAA' and 'BBBB'>=lstg_format_name;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
        # ['''{"acceptPartial":true,"limit":"5","offset":0,"project":"learn_kylin","sql":"select kylin_sales.lstg_format_name, sum(price) as GMV, count(seller_id) as TRANS_CNT  from kylin_sales where kylin_sales.lstg_format_name > 'AB'  group by kylin_sales.lstg_format_name having count(seller_id) > 2;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],



        ###ssb
        # ['''{"acceptPartial":true,"limit":"5","offset":0,"project":"ssb","sql":"select sum(v_revenue) as revenue  from p_lineorder  left join dates on lo_orderdate = d_datekey  where d_year = 1993  and lo_discount between 1 and 3  and lo_quantity < 25;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
        # ['''{"acceptPartial":true,"limit":"5","offset":0,"project":"ssb","sql":"select sum(v_revenue) as revenue from p_lineorder left join dates on lo_orderdate = d_datekey where d_yearmonthnum = 199401 and lo_discount between 4 and 6 and lo_quantity between 26 and 35;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
        ['''{"acceptPartial":true,"limit":"5","offset":0,"project":"ssb","sql":"select sum(v_revenue) as revenue from p_lineorder left join dates on lo_orderdate = d_datekey where d_weeknuminyear = 6 and d_year = 1994 and lo_discount between 5 and 7 and lo_quantity between 26 and 35;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}''']
           )
    @ ddt.unpack
    @BeautifulReport.add_test_img('bbbb')

    def testPushdownQuery_ssb(self,payload):
        """
            pushdown query test
        """

        query_url = pushdownTest_ssb.base_url + "/query"
        starttime = datetime.now()
        response = requests.request("POST", query_url, data=payload, headers=pushdownTest_ssb.headers)
        timeend = datetime.now()
        dtime = (timeend - starttime).seconds
        sql = '''
        INSERT INTO `longtest`.`result`(`id`, `name`, `request`, `respons`, `starttime`, `endtime`, `status`, `date`,`D_time`) VALUES (null, '{namepro}', '', '', '{timestart}', '{timeend}', '{status}','{timeend}', '{D_time}');
        '''
        sql = sql.format(namepro="pushdownTest_ssb",request1=str(response.url),respons1=str(response.text),timestart = starttime,timeend = timeend,D_time = dtime,status = str(response.status_code))
        pushdownTest_ssb.cur.execute(sql)
        pushdownTest_ssb.conn.commit()
        self.assertEqual(
                         response.status_code,200,"uri = "+response.url+' '+"payload = " + str(payload)
                         +' '+"status_code = "+str(response.status_code)+' '+'response = '+str(response.text)
                         )
        self.assertEqual(
                         json.loads(response.text)['code'],'000',"uri = "+response.url+' '+"payload = " + str(payload)
                         +' '+"status_code = "+str(response.status_code)+' '+'response = '+str(response.text)
                         )
        time.sleep(random.randint(1,6))






if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.makeSuite(pushdownTest_ssb))
    run(test_suite)