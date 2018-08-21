# #!/usr/bin/env python
# # --coding:utf-8--
#
# from http.server import BaseHTTPRequestHandler, HTTPServer
# from os import path
# from urllib.parse import urlparse
#
# curdir = path.dirname(path.realpath(__file__))
# sep = '/'
#
# # MIME-TYPE
# mimedic = [
#     ('.html', 'text/html'),
#     ('.htm', 'text/html'),
#     ('.js', 'application/javascript'),
#     ('.css', 'text/css'),
#     ('.json', 'application/json'),
#     ('.png', 'image/png'),
#     ('.jpg', 'image/jpeg'),
#     ('.gif', 'image/gif'),
#     ('.txt', 'text/plain'),
#     ('.avi', 'video/x-msvideo'),
# ]
#
#
# class testHTTPServer_RequestHandler(BaseHTTPRequestHandler):
#     # GET
#     def do_GET(self):
#         sendReply = False
#         querypath = urlparse(self.path)
#         filepath, query = querypath.path, querypath.query
#
#         if filepath.endswith('/'):
#             filepath += 'index.html'
#         filename, fileext = path.splitext(filepath)
#         for e in mimedic:
#             if e[0] == fileext:
#                 mimetype = e[1]
#                 sendReply = True
#
#         if sendReply == True:
#             try:
#                 with open(path.realpath(curdir + sep + filepath), 'rb') as f:
#                     content = f.read()
#                     self.send_response(200)
#                     self.send_header('Content-type', mimetype)
#                     self.end_headers()
#                     self.wfile.write(content)
#             except IOError:
#                 self.send_error(404, 'File Not Found: %s' % self.path)
#
#
# def run():
#     port = 8000
#     print('starting server, port', port)
#
#     # Server settings
#     server_address = ('', port)
#     httpd = HTTPServer(server_address, testHTTPServer_RequestHandler)
#     print('running server...')
#     httpd.serve_forever()
#
#
# if __name__ == '__main__':
#     run()
#
#


#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os   #Python的标准库中的os模块包含普遍的操作系统功能
import re   #引入正则表达式对象
import urllib   #用于对URL进行编解码
# from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler  #导入HTTP处理相关的模块
from http.server import BaseHTTPRequestHandler, HTTPServer


#自定义处理程序，用于处理HTTP请求
class TestHTTPHandler(BaseHTTPRequestHandler):
    #处理GET请求
    def do_GET(self):
        #获取URL
        print ('URL=',self.path)
        #页面输出模板字符串
        templateStr = '''
        <html>   
        <head>   
        <title>QR Link Generator</title>   
        </head>   
        <body>   
        hello Python!
        </body>   
        </html>
        '''

        self.protocal_version = 'HTTP/1.1'  #设置协议版本
        self.send_response(200) #设置响应状态码
        self.send_header("Welcome", "Contect")  #设置响应头
        self.end_headers()
        self.wfile.write(templateStr)   #输出响应内容

        #启动服务函数
def start_server(port):
        http_server = HTTPServer(('', int(port)), TestHTTPHandler)
        http_server.serve_forever() #设置一直监听并接收请求

#os.chdir('static')  #改变工作目录到 static 目录
start_server(8001)  #启动服务，监听8000端口