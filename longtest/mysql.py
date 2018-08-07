# !/usr/bin/env python
# -*- coding:utf-8 -*-
# Author:lcj
import pymysql
#连接数据库
conn = pymysql.connect(host='10.1.40.100',port= 3306,user = 'root',passwd='root123',db='longtest') #db：库名
#创建游标
cur = conn.cursor()
#删除cj表中数据
result = cur.execute("select * from result")

#提交
conn.commit()
print cur.fetchall()
#关闭指针对象
cur.close()
#关闭连接对象
conn.close()