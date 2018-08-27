# coding=utf-8
import unittest

from tomorrow import threads

from BeautifulReport import BeautifulReport


# 获取路径
casepath = 'tests/'

def add_case(case_path=casepath, rule="test*.py"):
    '''加载所有的测试用例'''
    discover = unittest.defaultTestLoader.discover(case_path,pattern=rule, top_level_dir=None)
    return discover

@threads(4)
def run_case(all_case):
    '''执行所有的用例, 并把结果写入测试报告'''
    result = BeautifulReport(all_case)
    result.report(filename='Longtest Report', description='Longtest Report', log_path='.')




if __name__ == "__main__":
    # 用例集合
    cases = add_case()
    print(cases)
    for i  in cases:
        print(i)
        run_case(i)
