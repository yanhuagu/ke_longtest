#!/usr/bin/python

import unittest
import requests
import json
import glob
import sys

import time

IS_PLUS = 1

class testQuery(unittest.TestCase):
    base_url = "http://10.1.2.108:7070/kylin/api"
    headers = {
        'content-type': "application/json",
        'authorization': "Basic QURNSU46S1lMSU4=",
        'cache-control': "no-cache",
        'accept': "application/vnd.apache.kylin-v2+json"
    }

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testQuery(self):
        query_url = testQuery.base_url + "/query"
        payload = '''{"acceptPartial":true,"limit":50000,"offset":0,"project":"test256to310","sql":"select kylin_cal_dt.week_beg_dt,sum(kylin_sales.price) as GMV  , count(*) as TRANS_CNT , sum(kylin_sales.item_count) as total_items from kylin_sales  inner JOIN kylin_cal_dt ON kylin_sales.part_dt = kylin_cal_dt.cal_dt inner JOIN kylin_category_groupings ON kylin_sales.leaf_categ_id = kylin_category_groupings.leaf_categ_id AND kylin_sales.lstg_site_id = kylin_category_groupings.site_id where kylin_sales.lstg_format_name='FP-GTC'  and kylin_cal_dt.week_beg_dt between DATE '2013-05-01' and DATE '2013-08-01' group by kylin_cal_dt.week_beg_dt;"}'''
        response = requests.request("POST", query_url, data=payload, headers=testQuery.headers)
        print response

        # sql_files = glob.glob('sql/*.sql')
        # index = 0
        query_url = testQuery.base_url + "/query"
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

    # def testQueryPushDown(self):
    #     sql_files = glob.glob('sql/*.sql')
    #     index = 0
    #     url = testQuery.base_url + "/cubes/kylin_sales_cube/disable"
    #     status_code = 0
    #     try_time = 1
    #     while status_code != 200 and try_time <= 3:
    #         print 'Disable cube, try_time = ' + str(try_time)
    #         try:
    #             response = requests.request("PUT", url, headers=testQuery.headers)
    #             status_code = response.status_code
    #         except:
    #             status_code = 0
    #             pass
    #         if status_code != 200:
    #             time.sleep(10)
    #             try_time += 1
    #
    #     self.assertEqual(status_code, 200, 'Disable cube failed.')
    #
    #     # Sleep 3 seconds to ensure cache wiped while do query pushdown
    #     time.sleep(3)
    #
    #     query_url = testQuery.base_url + "/query"
    #     for sql_file in sql_files:
    #         index += 1
    #         sql_statement = ''
    #         sql_statement_lines = open(sql_file).readlines()
    #         for sql_statement_line in sql_statement_lines:
    #             if not sql_statement_line.startswith('--'):
    #                 sql_statement += sql_statement_line.strip() + ' '
    #         payload = "{\"sql\": \"" + sql_statement.strip() + "\", \"offset\": 0, \"limit\": \"50000\", \"acceptPartial\":false, \"project\":\"learn_kylin\"}"
    #         print 'Test Query #' + str(index) + ': \n' + sql_statement
    #         response = requests.request("POST", query_url, data=payload, headers=testQuery.headers)
    #
    #         self.assertEqual(response.status_code, 200, 'Query failed.')
    #
    #         actual_result = json.loads(response.text)
    #         print actual_result
    #         print 'Query duration: ' + str(actual_result['data']['duration']) + 'ms'
    #         del actual_result['data']['duration']
    #         del actual_result['data']['hitExceptionCache']
    #         del actual_result['data']['storageCacheUsed']
    #         del actual_result['data']['totalScanCount']
    #         del actual_result['data']['totalScanBytes']
    #         del actual_result['data']['columnMetas']
    #         del actual_result['data']['sparderUsed']
    #         del actual_result['data']['lateDecodeEnabled']
    #         del actual_result['data']['timeout']
    #         del actual_result['data']['server']
    #
    #         expect_result = json.loads(open(sql_file[:-4] + '.json').read().strip())
    #         del expect_result['data']['columnMetas']
    #         expect_result['data']['cube'] = ''
    #         expect_result['data']['pushDown'] = True
    #         self.assertEqual(actual_result, expect_result, 'Query pushdown\'s result does not equal with expected result.')


if __name__ == '__main__':
    print 'Test Query for Kylin sample.'
    unittest.main()