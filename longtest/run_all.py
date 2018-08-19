
"""
@Version: 1.0
@Project: BeautyReport
@Author: Raymond
@Data: 2017/11/17 下午3:48
@File: sample.py
@License: MIT
"""
from nose.plugins.plugintest import run_buffered as run

import unittest
from BeautifulReport import BeautifulReport

if __name__ == '__main__':
    test_suite = unittest.defaultTestLoader.discover('tests/', pattern='test*.py')

    result = BeautifulReport(test_suite)
    result.report(filename='Longtest Report', description='Longtest Report', log_path='.')
    #
    # run(test_suite)
