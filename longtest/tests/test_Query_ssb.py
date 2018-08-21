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
from nose.plugins.plugintest import run_buffered as run



@ddt.ddt
class queryTest_ssb(unittest.TestCase):
    # base_url = "http://10.1.1.83:7070/kylin/api"
    base_url = "http://10.1.40.104:7298/kylin/api"

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
        queryTest_ssb.cur.close()
        queryTest_ssb.conn.close()
        pass

    @classmethod
    def setUpClass(self):
        # 必须使用@classmethod 装饰器,所有test运行前运行一次
        pass



    @ ddt.data(

        ##ssb
        # ['''{"acceptPartial":true,"limit":"5","offset":0,"project":"ssb","sql":"select sum(lo_revenue) as lo_revenue, d_year, p_brand from p_lineorder left join dates on lo_orderdate = d_datekey left join part on lo_partkey = p_partkey left join supplier on lo_suppkey = s_suppkey where p_category = 'MFGR#12' and s_region = 'AMERICA' group by d_year, p_brand order by d_year, p_brand;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
        # ['''{"acceptPartial":true,"limit":"5","offset":0,"project":"ssb","sql":"select sum(lo_revenue) as lo_revenue, d_year, p_brand from p_lineorder left join dates on lo_orderdate = d_datekey left join part on lo_partkey = p_partkey left join supplier on lo_suppkey = s_suppkey where p_brand between 'MFGR#2221' and 'MFGR#2228' and s_region = 'ASIA' group by d_year, p_brand order by d_year, p_brand;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
        # ['''{"acceptPartial":true,"limit":"5","offset":0,"project":"ssb","sql":"select sum(lo_revenue) as lo_revenue, d_year, p_brand from p_lineorder left join dates on lo_orderdate = d_datekey left join part on lo_partkey = p_partkey left join supplier on lo_suppkey = s_suppkey where p_brand = 'MFGR#2239' and s_region = 'EUROPE' group by d_year, p_brand order by d_year, p_brand;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
        # ['''{"acceptPartial":true,"limit":"5","offset":0,"project":"ssb","sql":"select c_nation, s_nation, d_year, sum(lo_revenue) as lo_revenue from p_lineorder left join dates on lo_orderdate = d_datekey left join customer on lo_custkey = c_custkey left join supplier on lo_suppkey = s_suppkey where c_region = 'ASIA' and s_region = 'ASIA'and d_year >= 1992 and d_year <= 1997 group by c_nation, s_nation, d_year order by d_year asc, lo_revenue desc;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
        # ['''{"acceptPartial":true,"limit":"5","offset":0,"project":"ssb","sql":"select c_city, s_city, d_year, sum(lo_revenue) as lo_revenue from p_lineorder left join dates on lo_orderdate = d_datekey left join customer on lo_custkey = c_custkey left join supplier on lo_suppkey = s_suppkey where c_nation = 'UNITED STATES' and s_nation = 'UNITED STATES' and d_year >= 1992 and d_year <= 1997 group by c_city, s_city, d_year order by d_year asc, lo_revenue desc;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
        # ['''{"acceptPartial":true,"limit":"5","offset":0,"project":"ssb","sql":"select c_city, s_city, d_year, sum(lo_revenue) as lo_revenue from p_lineorder left join dates on lo_orderdate = d_datekey left join customer on lo_custkey = c_custkey left join supplier on lo_suppkey = s_suppkey where (c_city='UNITED KI1' or c_city='UNITED KI5') and (s_city='UNITED KI1' or s_city='UNITED KI5') and d_year >= 1992 and d_year <= 1997 group by c_city, s_city, d_year order by d_year asc, lo_revenue desc;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
        # ['''{"acceptPartial":true,"limit":"5","offset":0,"project":"ssb","sql":"select c_city, s_city, d_year, sum(lo_revenue) as lo_revenue from p_lineorder left join dates on lo_orderdate = d_datekey left join customer on lo_custkey = c_custkey left join supplier on lo_suppkey = s_suppkey where (c_city='UNITED KI1' or c_city='UNITED KI5') and (s_city='UNITED KI1' or s_city='UNITED KI5') and d_yearmonth = 'Dec1997' group by c_city, s_city, d_year order by d_year asc, lo_revenue desc;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
        # ['''{"acceptPartial":true,"limit":"5","offset":0,"project":"ssb","sql":"select d_year, c_nation, sum(lo_revenue) - sum(lo_supplycost) as profit from p_lineorder left join dates on lo_orderdate = d_datekey left join customer on lo_custkey = c_custkey left join supplier on lo_suppkey = s_suppkey left join part on lo_partkey = p_partkey where c_region = 'AMERICA' and s_region = 'AMERICA' and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2') group by d_year, c_nation order by d_year, c_nation;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
        # ['''{"acceptPartial":true,"limit":"5","offset":0,"project":"ssb","sql":"select d_year, s_nation, p_category, sum(lo_revenue) - sum(lo_supplycost) as profit from p_lineorder left join dates on lo_orderdate = d_datekey left join customer on lo_custkey = c_custkey left join supplier on lo_suppkey = s_suppkey left join part on lo_partkey = p_partkey where c_region = 'AMERICA'and s_region = 'AMERICA' and (d_year = 1997 or d_year = 1998) and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2') group by d_year, s_nation, p_category order by d_year, s_nation, p_category;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
        # ['''{"acceptPartial":true,"limit":"5","offset":0,"project":"ssb","sql":"select d_year, s_city, p_brand, sum(lo_revenue) - sum(lo_supplycost) as profit from p_lineorder left join dates on lo_orderdate = d_datekey left join customer on lo_custkey = c_custkey left join supplier on lo_suppkey = s_suppkey left join part on lo_partkey = p_partkey where c_region = 'AMERICA'and s_nation = 'UNITED STATES' and (d_year = 1997 or d_year = 1998) and p_category = 'MFGR#14' group by d_year, s_city, p_brand order by d_year, s_city, p_brand;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}''']
#
#

#tpch
        # ['''{"acceptPartial":true,"limit":50000,"offset":0,"project":"tpch_kap_24","sql":"select  l_returnflag,  l_linestatus,  sum(l_quantity) as sum_qty,  sum(l_extendedprice) as sum_base_price,  sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,  avg(l_quantity) as avg_qty,  avg(l_extendedprice) as avg_price,  avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= '1998-09-16' group by l_returnflag, l_linestatus order by l_returnflag,l_linestatus;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],#Q1
        # ['''{"acceptPartial":true,"limit":50000,"offset":0,"project":"tpch_kap_24","sql":"select  p_partkey as min_p_partkey,  min(ps_supplycost) as min_ps_supplycost from  part,  partsupp,  supplier,  nation,  region where  p_partkey = ps_partkey  and s_suppkey = ps_suppkey  and s_nationkey = n_nationkey  and n_regionkey = r_regionkey  and r_name = 'EUROPE' group by  p_partkey;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],#Q2
        # ['''{"acceptPartial":true,"limit":50000,"offset":0,"project":"tpch_kap_24","sql":"  select  s_acctbal,  s_name,  n_name,  p_partkey,  p_mfgr,  s_address,  s_phone,  s_comment from  part,  supplier,  partsupp,  nation,  region,  q2_min_ps_supplycost where  p_partkey = ps_partkey  and s_suppkey = ps_suppkey  and p_size = 37  and p_type like '%COPPER'  and s_nationkey = n_nationkey  and n_regionkey = r_regionkey  and r_name = 'EUROPE'  and ps_supplycost = min_ps_supplycost  and p_partkey = min_p_partkey order by  s_acctbal desc,  n_name,  s_name,  p_partkey limit 100;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],#Q2
        # ['''{"acceptPartial":true,"limit":50000,"offset":0,"project":"tpch_kap_24","sql":"select  l_orderkey,  sum(l_extendedprice * (1 - l_discount)) as revenue,  o_orderdate,  o_shippriority from  customer,  orders,  lineitem where  c_mktsegment = 'BUILDING'  and c_custkey = o_custkey  and l_orderkey = o_orderkey  and o_orderdate < '1995-03-22'  and l_shipdate > '1995-03-22' group by  l_orderkey,  o_orderdate,  o_shippriority order by  revenue desc,  o_orderdate limit 10; ","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],#Q3
        # ['''{"acceptPartial":true,"limit":50000,"offset":0,"project":"tpch_kap_24","sql":"select    o_orderpriority,    count(*) as order_count  from    orders as o  where    o_orderdate >= '1996-05-01'    and o_orderdate < '1996-08-01'    and exists (      select        *      from        lineitem      where        l_orderkey = o.o_orderkey        and l_commitdate < l_receiptdate    )  group by    o_orderpriority  order by    o_orderpriority;  ","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],#Q4
        # ['''{"acceptPartial":true,"limit":50000,"offset":0,"project":"tpch_kap_24","sql":"select    _name,    sum(l_extendedprice * (1 - l_discount)) as revenue  from    customer,    orders,    lineitem,    supplier,    nation,    region  where    c_custkey = o_custkey    and l_orderkey = o_orderkey    and l_suppkey = s_suppkey    and c_nationkey = s_nationkey    and s_nationkey = n_nationkey    and n_regionkey = r_regionkey    and r_name = 'AFRICA'    and o_orderdate >= '1993-01-01'    and o_orderdate < '1994-01-01'  group by    n_name  order by    revenue desc;  ","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],#Q5
        # ['''{"acceptPartial":true,"limit":50000,"offset":0,"project":"tpch_kap_24","sql":"select    tsum(l_extendedprice * l_discount) as revenue  from    lineitem  where    l_shipdate >= '1993-01-01'    and l_shipdate < '1994-01-01'    and l_discount between 0.06 - 0.01 and 0.06 + 0.01    and l_quantity < 25;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],#Q6
        # ['''{"acceptPartial":true,"limit":50000,"offset":0,"project":"tpch_kap_24","sql":"select    supp_nation,    cust_nation,    l_year,    sum(volume) as revenue  from    (      select        n1.n_name as supp_nation,        n2.n_name as cust_nation,        year(l_shipdate) as l_year,        l_extendedprice * (1 - l_discount) as volume      from        supplier,        lineitem,        orders,        customer,        nation n1,        nation n2      where        s_suppkey = l_suppkey        and o_orderkey = l_orderkey        and c_custkey = o_custkey        and s_nationkey = n1.n_nationkey        and c_nationkey = n2.n_nationkey        and (          (n1.n_name = 'KENYA' and n2.n_name = 'PERU')          or (n1.n_name = 'PERU' and n2.n_name = 'KENYA')        )        and l_shipdate between '1995-01-01' and '1996-12-31'    ) as shipping  group by    supp_nation,    cust_nation,    l_year  order by    supp_nation,    cust_nation,    l_year;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],#Q7
        # ['''{"acceptPartial":true,"limit":50000,"offset":0,"project":"tpch_kap_24","sql":"select    o_year,    sum(case      when nation = 'PERU' then volume      else 0    end) / sum(volume) as mkt_share  from    (      select        year(o_orderdate) as o_year,        l_extendedprice * (1 - l_discount) as volume,        n2.n_name as nation      from        part,        supplier,        lineitem,        orders,        customer,        nation n1,        nation n2,        region      where        p_partkey = l_partkey        and s_suppkey = l_suppkey        and l_orderkey = o_orderkey        and o_custkey = c_custkey        and c_nationkey = n1.n_nationkey        and n1.n_regionkey = r_regionkey        and r_name = 'AMERICA'        and s_nationkey = n2.n_nationkey        and o_orderdate between '1995-01-01' and '1996-12-31'        and p_type = 'ECONOMY BURNISHED NICKEL'    ) as all_nations  group by    o_year  order by    o_year;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],#Q8
        # ['''{"acceptPartial":true,"limit":50000,"offset":0,"project":"tpch_kap_24","sql":"select    nation,    o_year,    sum(amount) as sum_profit  from    (      select        n_name as nation,        year(o_orderdate) as o_year,        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount      from        part,        supplier,        lineitem,        partsupp,        orders,        nation      where        s_suppkey = l_suppkey        and ps_suppkey = l_suppkey        and ps_partkey = l_partkey        and p_partkey = l_partkey        and o_orderkey = l_orderkey        and s_nationkey = n_nationkey        and p_name like '%plum%'    ) as profit  group by    nation,    o_year  order by    nation,    o_year desc;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],#Q9
        # ['''{"acceptPartial":true,"limit":50000,"offset":0,"project":"tpch_kap_24","sql":"select    c_custkey,    c_name,    sum(l_extendedprice * (1 - l_discount)) as revenue,    c_acctbal,    n_name,    c_address,    c_phone,    c_comment  from    customer,    orders,    lineitem,    nation  where    c_custkey = o_custkey    and l_orderkey = o_orderkey    and o_orderdate >= '1993-07-01'    and o_orderdate < '1993-10-01'    and l_returnflag = 'R'    and c_nationkey = n_nationkey  group by    c_custkey,    c_name,    c_acctbal,    c_phone,    n_name,    c_address,    c_comment  order by    revenue desc  limit 20;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],#Q10
        # ['''{"acceptPartial":true,"limit":50000,"offset":0,"project":"tpch_kap_24","sql":"select  ps_partkey,  sum(ps_supplycost * ps_availqty) as part_value from  partsupp,  supplier,  nation where  ps_suppkey = s_suppkey  and s_nationkey = n_nationkey  and n_name = 'GERMANY' group by ps_partkey; ","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],#Q11
        # ['''{"acceptPartial":true,"limit":50000,"offset":0,"project":"tpch_kap_24","sql":"select  ps_partkey, part_value as value from (  select   ps_partkey,   part_value,   total_value  from   q11_part_tmp_cached join q11_sum_tmp_cached ) a where  part_value > total_value * 0.0001 order by  value desc;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],#Q11
        # ['''{"acceptPartial":true,"limit":50000,"offset":0,"project":"tpch_kap_24","sql":"select  l_shipmode,  sum(case   when o_orderpriority = '1-URGENT'    or o_orderpriority = '2-HIGH'    then 1   else 0  end) as high_line_count,  sum(case   when o_orderpriority <> '1-URGENT'    and o_orderpriority <> '2-HIGH'    then 1   else 0  end) as low_line_count from  orders,  lineitem where  o_orderkey = l_orderkey  and l_shipmode in ('REG AIR', 'MAIL')  and l_commitdate < l_receiptdate  and l_shipdate < l_commitdate  and l_receiptdate >= '1995-01-01'  and l_receiptdate < '1996-01-01' group by  l_shipmode order by  l_shipmode;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],#Q12
        # ['''{"acceptPartial":true,"limit":50000,"offset":0,"project":"tpch_kap_24","sql":"select  100.00 * sum(case   when p_type like 'PROMO%'    then l_extendedprice * (1 - l_discount)   else 0  end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue from  lineitem,  part where  l_partkey = p_partkey  and l_shipdate >= '1995-08-01'  and l_shipdate < '1995-09-01';","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],#Q14
        # ['''{"acceptPartial":true,"limit":50000,"offset":0,"project":"tpch_kap_24","sql":"select  l_suppkey as supplier_no,  sum(l_extendedprice * (1 - l_discount)) as total_revenue from  lineitem where  l_shipdate >= '1996-01-01'  and l_shipdate < '1996-04-01' group by l_suppkey;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],#Q15
        # ['''{"acceptPartial":true,"limit":50000,"offset":0,"project":"tpch_kap_24","sql":"select  p_brand,  p_type,  p_size,  count(distinct ps_suppkey) as supplier_cnt from  partsupp left join part on  p_partkey = ps_partkey  where p_brand <> 'Brand#34'  and p_type not like 'ECONOMY BRUSHED%'  and p_size in (22, 14, 27, 49, 21, 33, 35, 28)  and partsupp.ps_suppkey not in (   select    s_suppkey   from    supplier   where    s_comment like '%Customer%Complaints%'  ) group by  p_brand,  p_type,  p_size order by  supplier_cnt desc,  p_brand,  p_type,  p_size limit 10;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],#Q16
        # ['''{"acceptPartial":true,"limit":50000,"offset":0,"project":"tpch_kap_24","sql":"with q17_part as (   select p_partkey from part where     p_brand = 'Brand#23'   and p_container = 'MED BOX' ), q17_avg as (   select l_partkey as t_partkey, 0.2 * avg(l_quantity) as t_avg_quantity   from lineitem    where l_partkey IN (select p_partkey from q17_part)   group by l_partkey ), q17_price as (   select   l_quantity,   l_partkey,   l_extendedprice   from   lineitem   where   l_partkey IN (select p_partkey from q17_part) ) select cast(sum(l_extendedprice) / 7.0 as decimal(32,2)) as avg_yearly from q17_avg, q17_price where  t_partkey = l_partkey and l_quantity < t_avg_quantity;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],#Q17
        # ['''{"acceptPartial":true,"limit":50000,"offset":0,"project":"tpch_kap_24","sql":"select  l_orderkey,  sum(l_quantity) as t_sum_quantity from  lineitem where  l_orderkey is not null group by  l_orderkey;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],#Q18
        # ['''{"acceptPartial":true,"limit":50000,"offset":0,"project":"tpch_kap_24","sql":"select  sum(l_extendedprice* (1 - l_discount)) as revenue from  lineitem left join part on  p_partkey = l_partkey   where  p_brand = 'Brand#32'   and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')   and l_quantity >= 7 and l_quantity <= 7 + 10   and p_size between 1 and 5   and l_shipmode in ('AIR', 'AIR REG')   and l_shipinstruct = 'DELIVER IN PERSON' limit 10;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],#Q19


##learn kylin
        #
        ['''{"acceptPartial":true,"limit":"5","offset":0,"project":"learn_kylin","sql":"select lstg_format_name, sum(price) as GMV from kylin_sales where lstg_format_name='FP-GTC' group by lstg_format_name","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
        # ['''{"acceptPartial":true,"limit":"5","offset":0,"project":"learn_kylin","sql":"select sum(price) as GMV, count(1) as TRANS_CNT from kylin_sales","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
        # ['''{"acceptPartial":true,"limit":"5","offset":0,"project":"learn_kylin","sql":"select kylin_sales.lstg_format_name, sum(price) as GMV, count(*) as TRANS_CNT from kylin_sales group by kylin_sales.lstg_format_name;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
        # ['''{"acceptPartial":true,"limit":"5","offset":0,"project":"learn_kylin","sql":"select kylin_sales.lstg_format_name,sum(kylin_sales.price) as GMV, count(*) as TRANS_CNT from kylin_sales group by kylin_sales.lstg_format_name having sum(price)>5000","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
        # ['''{"acceptPartial":true,"limit":"5","offset":0,"project":"learn_kylin","sql":"select kylin_sales.lstg_format_name,sum(kylin_sales.price) as GMV, count(*) as TRANS_CNT from kylin_sales where kylin_sales.lstg_format_name is null group by kylin_sales.lstg_format_name having sum(price)>5000 and count(*)>72 ","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
        # # ['''{"acceptPartial":true,"limit":"5","offset":0,"project":"learn_kylin","sql":"select kylin_cal_dt.week_beg_dt,sum(kylin_sales.price) as GMV  , count(*) as TRANS_CNT , sum(kylin_sales.item_count) as total_items from kylin_sales  inner JOIN kylin_cal_dt ON kylin_sales.part_dt = kylin_cal_dt.cal_dt inner JOIN kylin_category_groupings ON kylin_sales.leaf_categ_id = kylin_category_groupings.leaf_categ_id AND kylin_sales.lstg_site_id = kylin_category_groupings.site_id where kylin_sales.lstg_format_name='FP-GTC'  and kylin_cal_dt.week_beg_dt between DATE '2013-05-01' and DATE '2013-08-01' group by kylin_cal_dt.week_beg_dt;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
        # # ['''{"acceptPartial":true,"limit":"5","offset":0,"project":"learn_kylin","sql":"SELECT  kylin_cal_dt.week_beg_dt  ,kylin_category_groupings.meta_categ_name  ,sum(price) as GMV, count(*) as TRANS_CNT , sum(kylin_sales.item_count) as total_items FROM kylin_sales  inner JOIN kylin_cal_dt ON kylin_sales.part_dt = kylin_cal_dt.cal_dt inner JOIN kylin_category_groupings ON kylin_sales.leaf_categ_id = kylin_category_groupings.leaf_categ_id AND kylin_sales.lstg_site_id = kylin_category_groupings.site_id where kylin_cal_dt.week_beg_dt between DATE '2013-09-01' and DATE '2013-10-01'  and kylin_category_groupings.categ_lvl3_name='Other'  group by kylin_cal_dt.week_beg_dt  ,kylin_category_groupings.meta_categ_name;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
        # ['''{"acceptPartial":true,"limit":"5","offset":0,"project":"learn_kylin","sql":"select count(*) as cnt from kylin_sales where lstg_format_name>='AAAA' and 'BBBB'>=lstg_format_name;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],
        # ['''{"acceptPartial":true,"limit":"5","offset":0,"project":"learn_kylin","sql":"select kylin_sales.lstg_format_name, sum(price) as GMV, count(seller_id) as TRANS_CNT  from kylin_sales where kylin_sales.lstg_format_name > 'AB'  group by kylin_sales.lstg_format_name having count(seller_id) > 2;","backdoorToggles":{"DEBUG_TOGGLE_HTRACE_ENABLED":false}}'''],



    )
    @ ddt.unpack
    @BeautifulReport.add_test_img('aaaa')

    def testCubeQuery_ssb(self,payload):
        """
            testQuery
        """
        query_url = queryTest_ssb.base_url + "/query"
        starttime = datetime.now()
        response = requests.request("POST", query_url, data=payload, headers=queryTest_ssb.headers)
        timeend = datetime.now()
        dtime = (timeend - starttime).seconds
        sql = '''
        INSERT INTO `longtest`.`result`(`id`, `name`, `request`, `respons`, `starttime`, `endtime`, `status`, `date`,`D_time`) VALUES (null, '{namepro}', '', '', '{timestart}', '{timeend}', '{status}','{timeend}', '{D_time}');
        '''
        sql = sql.format(namepro="testQuery",request1=str(response.url),respons1=str(response.text),timestart = starttime,timeend = timeend,D_time = dtime,status = str(response.status_code))
        queryTest_ssb.cur.execute(sql)
        queryTest_ssb.conn.commit()
        time.sleep(random.randint(1,6))
        self.assertEqual(
                         response.status_code,200,"uri = "+response.url+' '+"********  payload  ********  " + str(payload)
                         +' '+"********  status_code  ******** "+str(response.status_code)+' '+'********  response  ********  '+str(response.text)
                         )

        self.assertEqual(
                         json.loads(response.text)['code'],"000","uri = "+response.url+' '+"********  payload  ********  " + str(payload)
                         +' '+"********  status_code  ******** "+str(response.status_code)+' '+'********  response  ********  '+str(response.text)
                         )


if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.makeSuite(queryTest_ssb))
    run(test_suite)

    # result = BeautifulReport(test_suite)
    # result.report(filename='Longtest Report', description='Longtest Report', log_path='.')


