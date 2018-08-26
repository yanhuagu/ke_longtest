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
class pushdownTest_tpch(unittest.TestCase):
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
        pushdownTest_tpch.cur.close()
        pushdownTest_tpch.conn.close()
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



        # #####tpch
        ['''{"acceptPartial":true,"limit":50000,"offset":0,"project":"tpch_kap_24","sql":"select  c_count,  count(*) as custdist from  (   select    c_custkey,    count(distinct o_orderkey) as c_count   from    customer left outer join v_orders on     c_custkey = o_custkey   where o_comment not like '%unusual%accounts%'   group by    c_custkey  ) c_orders group by  c_count order by  custdist desc,  c_count desc;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
        ['''{"acceptPartial":true,"limit":50000,"offset":0,"project":"tpch_kap_24","sql":"with tmp3 as (     select l_partkey, 0.5 * sum(l_quantity) as sum_quantity, l_suppkey     from v_lineitem     inner join    supplier on l_suppkey = s_suppkey     inner join    nation on s_nationkey = n_nationkey     inner join    part on l_partkey = p_partkey     where l_shipdate >= '1994-01-01' and l_shipdate <= '1995-01-01'     and n_name = 'CANADA'     and p_name like 'forest%'     group by l_partkey, l_suppkey )  select     s_name,     s_address from        v_partsupp     inner join    supplier on ps_suppkey = s_suppkey     inner join tmp3 on ps_partkey = l_partkey and ps_suppkey = l_suppkey where     ps_availqty > sum_quantity group by     s_name, s_address order by     s_name;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
        ['''{"acceptPartial":true,"limit":50000,"offset":0,"project":"tpch_kap_24","sql":"select s_name, count(*) as numwait from (     select         l1.l_suppkey,         s_name,         l1.l_orderkey     from          v_lineitem l1         inner join  v_orders on l1.l_orderkey = o_orderkey         inner join  supplier on l1.l_suppkey = s_suppkey         inner join  nation on s_nationkey = n_nationkey         inner join (             select                 l_orderkey,                 count (distinct l_suppkey)             from                  v_lineitem inner join  v_orders on l_orderkey = o_orderkey             where                 o_orderstatus = 'F'             group by                 l_orderkey             having                 count (distinct l_suppkey) > 1         ) l2 on l1.l_orderkey = l2.l_orderkey         inner join (             select                 l_orderkey,                 count (distinct l_suppkey)             from                  v_lineitem inner join  v_orders on l_orderkey = o_orderkey             where                 o_orderstatus = 'F'                 and l_receiptdelayed = 1             group by                 l_orderkey             having                 count (distinct l_suppkey) = 1         ) l3 on l1.l_orderkey = l3.l_orderkey     where         o_orderstatus = 'F'         and l_receiptdelayed = 1         and n_name = 'SAUDI ARABIA'     group by         l1.l_suppkey,         s_name,         l1.l_orderkey ) group by     s_name order by     numwait desc,     s_name limit 100; ","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
        ['''{"acceptPartial":true,"limit":50000,"offset":0,"project":"tpch_kap_24","sql":"with avg_tmp as (     select         avg(c_acctbal) as avg_acctbal     from          customer     where         c_acctbal > 0.00 and substring(c_phone, 1, 2) in ('13','31','23','29','30','18','17') )  select     cntrycode,     count(1) as numcust,     sum(c_acctbal) as totacctbal from (     select         substring(c_phone, 1, 2) as cntrycode,         c_acctbal     from           customer left join  v_orders on c_custkey = o_custkey, avg_tmp     where          substring(c_phone, 1, 2) in ('13','31','23','29','30','18','17')         and c_acctbal > avg_acctbal         and o_orderkey is null ) t group by     cntrycode order by     cntrycode; ","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}''']
    )
    @ ddt.unpack
    @BeautifulReport.add_test_img('bbbb')

    def testPushdownQuery_tpch(self,payload):
        """
            pushdown query test
        """

        query_url = pushdownTest_tpch.base_url + "/query"
        starttime = datetime.now()
        response = requests.request("POST", query_url, data=payload, headers=pushdownTest_tpch.headers)
        timeend = datetime.now()
        dtime = (timeend - starttime).seconds
        sql = '''
        INSERT INTO `longtest`.`result`(`id`, `name`, `request`, `respons`, `starttime`, `endtime`, `status`, `date`,`D_time`) VALUES (null, '{namepro}', '', '', '{timestart}', '{timeend}', '{status}','{timeend}', '{D_time}');
        '''
        sql = sql.format(namepro="testPushdownQuery",request1=str(response.url),respons1=str(response.text),timestart = starttime,timeend = timeend,D_time = dtime,status = str(response.status_code))
        pushdownTest_tpch.cur.execute(sql)
        pushdownTest_tpch.conn.commit()
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
    test_suite.addTest(unittest.makeSuite(pushdownTest_tpch))
    run(test_suite)