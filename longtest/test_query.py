# -*- coding: UTF-8 -*-
import random
import time
import unittest
import requests
import pymysql
from datetime import datetime
import HTMLTestRunner

class MyTest(unittest.TestCase):  # 继承unittest.TestCase
    base_url = "http://10.1.2.108:7070/kylin/api"
    headers = {
        'content-type': "application/json",
        'authorization': "Basic QURNSU46S1lMSU4=",
        'cache-control': "no-cache",
        'accept': "application/vnd.apache.kylin-v2+json"
    }

    conn = pymysql.connect(host='10.1.40.100', port=3306, user='root', passwd='root123', db='longtest')  # db：库名
    cur = conn.cursor()

    # 关闭指针对象


    def tearDown(self):
        # 每个测试用例执行之后做操作

        pass

    def setUp(self):
        # 每个测试用例执行之前做操作
        pass

    @classmethod
    def tearDownClass(self):
        # 必须使用 @ classmethod装饰器, 所有test运行完后运行一次
        MyTest.cur.close()
        MyTest.conn.close()
        pass

    @classmethod
    def setUpClass(self):
        # 必须使用@classmethod 装饰器,所有test运行前运行一次
        pass


    def testQuery1(self):


        query_url = MyTest.base_url + "/query"
        payload = '''{"acceptPartial":true,"limit":50000,"offset":0,"project":"test256to310","sql":"select kylin_cal_dt.week_beg_dt,sum(kylin_sales.price) as GMV  , count(*) as TRANS_CNT , sum(kylin_sales.item_count) as total_items from kylin_sales  inner JOIN kylin_cal_dt ON kylin_sales.part_dt = kylin_cal_dt.cal_dt inner JOIN kylin_category_groupings ON kylin_sales.leaf_categ_id = kylin_category_groupings.leaf_categ_id AND kylin_sales.lstg_site_id = kylin_category_groupings.site_id where kylin_sales.lstg_format_name='FP-GTC'  and kylin_cal_dt.week_beg_dt between DATE '2013-05-01' and DATE '2013-08-01' group by kylin_cal_dt.week_beg_dt;"}'''
        starttime = datetime.now()
        response = requests.request("POST", query_url, data=payload, headers=MyTest.headers)
        timeend = datetime.now()
        dtime = (timeend - starttime).seconds
        sql = '''
        INSERT INTO `longtest`.`result`(`id`, `name`, `request`, `respons`, `starttime`, `endtime`, `status`, `date`,`D_time`) VALUES (null, '{namepro}', '{request1}', '{respons1}', '{timestart}', '{timeend}', '{status}','{timeend}', '{D_time}');
        '''
        sql = sql.format(namepro="testQuery1",request1=str(response.url),respons1=str(response.text),timestart = starttime,timeend = timeend,D_time = dtime,status = str(response.status_code))
        MyTest.cur.execute(sql)
        MyTest.conn.commit()
        time.sleep(random.randint(1,6))
        print response.url
        self.assertEqual(
                         response.status_code,201,"uri = "+response.url+'\n'+"payload = " + str(payload)
                         +'\n'+"status_code = "+str(response.status_code)+'\n'+'response = '+str(response.text)
                         )

    # def testQuery2(self):
    #     query_url = MyTest.base_url + "/query"
    #     payload = '''{"acceptPartial":true,"limit":50000,"offset":0,"project":"test256to310","sql":"select kylin_cal_dt.week_beg_dt,sum(kylin_sales.price) as GMV  , count(*) as TRANS_CNT , sum(kylin_sales.item_count) as total_items from kylin_sales  inner JOIN kylin_cal_dt ON kylin_sales.part_dt = kylin_cal_dt.cal_dt inner JOIN kylin_category_groupings ON kylin_sales.leaf_categ_id = kylin_category_groupings.leaf_categ_id AND kylin_sales.lstg_site_id = kylin_category_groupings.site_id where kylin_sales.lstg_format_name='FP-GTC'  and kylin_cal_dt.week_beg_dt between DATE '2013-05-01' and DATE '2013-08-01' group by kylin_cal_dt.week_beg_dt;"}'''
    #     starttime = datetime.now()
    #     response = requests.request("POST", query_url, data=payload, headers=MyTest.headers)
    #     timeend = datetime.now()
    #     dtime = (timeend - starttime).seconds
    #     sql = '''
    #     INSERT INTO `longtest`.`result`(`id`, `name`, `request`, `respons`, `starttime`, `endtime`, `status`, `date`,`D_time`) VALUES (null, '{namepro}', '{request1}', '{respons1}', '{timestart}', '{timeend}', '{status}','{timeend}', '{D_time}');
    #     '''
    #     sql = sql.format(namepro="testQuery2", request1=str(response.url), respons1=str(response.text), timestart=starttime,
    #                      timeend=timeend, D_time=dtime, status=str(response.status_code))
    #     MyTest.cur.execute(sql)
    #     MyTest.conn.commit()
    #
    #     time.sleep(random.randint(1,6))
    #
    #
    # def testQuery3(self):
    #     query_url = MyTest.base_url + "/query"
    #     payload = '''{"acceptPartial":true,"limit":50000,"offset":0,"project":"test256to310","sql":"select kylin_cal_dt.week_beg_dt,sum(kylin_sales.price) as GMV  , count(*) as TRANS_CNT , sum(kylin_sales.item_count) as total_items from kylin_sales  inner JOIN kylin_cal_dt ON kylin_sales.part_dt = kylin_cal_dt.cal_dt inner JOIN kylin_category_groupings ON kylin_sales.leaf_categ_id = kylin_category_groupings.leaf_categ_id AND kylin_sales.lstg_site_id = kylin_category_groupings.site_id where kylin_sales.lstg_format_name='FP-GTC'  and kylin_cal_dt.week_beg_dt between DATE '2013-05-01' and DATE '2013-08-01' group by kylin_cal_dt.week_beg_dt;"}'''
    #     starttime = datetime.now()
    #     response = requests.request("POST", query_url, data=payload, headers=MyTest.headers)
    #     timeend = datetime.now()
    #     dtime = (timeend - starttime).seconds
    #     sql = '''
    #     INSERT INTO `longtest`.`result`(`id`, `name`, `request`, `respons`, `starttime`, `endtime`, `status`, `date`,`D_time`) VALUES (null, '{namepro}', '{request1}', '{respons1}', '{timestart}', '{timeend}', '{status}','{timeend}', '{D_time}');
    #     '''
    #     sql = sql.format(namepro="testQuery3", request1=str(response.url), respons1=str(response.text), timestart=starttime,
    #                      timeend=timeend, D_time=dtime, status=str(response.status_code))
    #     MyTest.cur.execute(sql)
    #     MyTest.conn.commit()
    #     time.sleep(random.randint(1,6))





if __name__ == '__main__':
    suite = unittest.TestSuite()
    tests = [MyTest("testQuery1")]

    # tests = [MyTest("testQuery1"), MyTest("testQuery2"), MyTest("testQuery3")]
    suite.addTests(tests)

    # now = time.strftime("%Y-%m-%d %H_%M_%S", time.localtime())
    filename = "/Users/yanhua.gu/work/code/ke_longtest/longtest/test_result.html"
    fp = open(filename, 'w')
    runner = HTMLTestRunner.HTMLTestRunner(
        stream=fp,
        title='Test Report',
        description='generated by HTMLTestRunner',
        verbosity = 2)
    runner.run(suite)
    fp.close()
