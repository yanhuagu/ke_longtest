# -*- coding: UTF-8 -*-
import random
import time
import unittest
import requests
import pymysql
from datetime import datetime

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

    def test_a_run(self):
        self.assertEqual(1, 1)  # 测试用例

    def test_b_run(self):
        self.assertEqual(2, 2)  # 测试用例

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
        time.sleep(random.randint(6,60))

    def testQuery2(self):
        query_url = MyTest.base_url + "/query"
        payload = '''{"acceptPartial":true,"limit":50000,"offset":0,"project":"test256to310","sql":"select kylin_cal_dt.week_beg_dt,sum(kylin_sales.price) as GMV  , count(*) as TRANS_CNT , sum(kylin_sales.item_count) as total_items from kylin_sales  inner JOIN kylin_cal_dt ON kylin_sales.part_dt = kylin_cal_dt.cal_dt inner JOIN kylin_category_groupings ON kylin_sales.leaf_categ_id = kylin_category_groupings.leaf_categ_id AND kylin_sales.lstg_site_id = kylin_category_groupings.site_id where kylin_sales.lstg_format_name='FP-GTC'  and kylin_cal_dt.week_beg_dt between DATE '2013-05-01' and DATE '2013-08-01' group by kylin_cal_dt.week_beg_dt;"}'''
        starttime = datetime.now()
        response = requests.request("POST", query_url, data=payload, headers=MyTest.headers)
        timeend = datetime.now()
        dtime = (timeend - starttime).seconds
        sql = '''
        INSERT INTO `longtest`.`result`(`id`, `name`, `request`, `respons`, `starttime`, `endtime`, `status`, `date`,`D_time`) VALUES (null, '{namepro}', '{request1}', '{respons1}', '{timestart}', '{timeend}', '{status}','{timeend}', '{D_time}');
        '''
        sql = sql.format(namepro="testQuery2", request1=str(response.url), respons1=str(response.text), timestart=starttime,
                         timeend=timeend, D_time=dtime, status=str(response.status_code))
        MyTest.cur.execute(sql)
        MyTest.conn.commit()
        time.sleep(random.randint(6,60))


    def testQuery3(self):
        query_url = MyTest.base_url + "/query"
        payload = '''{"acceptPartial":true,"limit":50000,"offset":0,"project":"test256to310","sql":"select kylin_cal_dt.week_beg_dt,sum(kylin_sales.price) as GMV  , count(*) as TRANS_CNT , sum(kylin_sales.item_count) as total_items from kylin_sales  inner JOIN kylin_cal_dt ON kylin_sales.part_dt = kylin_cal_dt.cal_dt inner JOIN kylin_category_groupings ON kylin_sales.leaf_categ_id = kylin_category_groupings.leaf_categ_id AND kylin_sales.lstg_site_id = kylin_category_groupings.site_id where kylin_sales.lstg_format_name='FP-GTC'  and kylin_cal_dt.week_beg_dt between DATE '2013-05-01' and DATE '2013-08-01' group by kylin_cal_dt.week_beg_dt;"}'''
        starttime = datetime.now()
        response = requests.request("POST", query_url, data=payload, headers=MyTest.headers)
        timeend = datetime.now()
        dtime = (timeend - starttime).seconds
        sql = '''
        INSERT INTO `longtest`.`result`(`id`, `name`, `request`, `respons`, `starttime`, `endtime`, `status`, `date`,`D_time`) VALUES (null, '{namepro}', '{request1}', '{respons1}', '{timestart}', '{timeend}', '{status}','{timeend}', '{D_time}');
        '''
        sql = sql.format(namepro="testQuery3", request1=str(response.url), respons1=str(response.text), timestart=starttime,
                         timeend=timeend, D_time=dtime, status=str(response.status_code))
        MyTest.cur.execute(sql)
        MyTest.conn.commit()
        time.sleep(random.randint(6,60))


        # sql_files = glob.glob('sql/*.sql')
        # index = 0
        query_url = MyTest.base_url + "/query"
        # for sql_file in sql_files:
        #     index += 1
        #     if IS_PLUS =='0' and sql_file.endswith('-plus.sql'):
        #         print 'Skip Plus SQL file: ' + sql_file
        #         continue
        #
        #     sql_statement = ''
        #     sql_statement_lines = open(sql_file).readlines()
        #     for sql_statement_line in sql_statement_lines:
        #         if not sql_statement_line.startswith('--'):
        #             sql_statement += sql_statement_line.strip() + ' '
        #     payload = "{\"sql\": \"" + sql_statement.strip() + "\", \"offset\": 0, \"limit\": \"50000\", \"acceptPartial\":false, \"project\":\"learn_kylin\"}"
        #     print 'Test Query #' + str(index) + ': \n' + sql_statement
        #     response = requests.request("POST", query_url, data=payload, headers=testQuery.headers)
        #
        #     self.assertEqual(response.status_code, 200, 'Query failed.')
        #
        #     actual_result = json.loads(response.text)
        #     print actual_result
        #     print 'Query duration: ' + str(actual_result['data']['duration']) + 'ms'
        #     del actual_result['data']['duration']
        #     del actual_result['data']['hitExceptionCache']
        #     del actual_result['data']['storageCacheUsed']
        #     del actual_result['data']['totalScanCount']
        #     del actual_result['data']['totalScanBytes']
        #     del actual_result['data']['sparderUsed']
        #     del actual_result['data']['lateDecodeEnabled']
        #     del actual_result['data']['timeout']
        #     del actual_result['data']['server']
        #
        #     expect_result = json.loads(open(sql_file[:-4] + '.json').read().strip())
        #     self.assertEqual(actual_result, expect_result, 'Query result does not equal.')



if __name__ == '__main__':
    unittest.main()  # 运行所有的测试用例