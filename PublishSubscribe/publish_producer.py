#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Created by LYleonard on 2019/12/18
# Describe: Publish/Subscribe--测试
import sys
import pika

class ProducerTool:
    def __init__(self
                 # , username, password
                 ):
        # 凭证信息
        credentials = pika.PlainCredentials(username="root", password="root")
        # 连接参数设置
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host="192.168.29.129", credentials=credentials))
        # 管道
        self.channel = self.connection.channel()

    def al_producer_message(self):
        # 创建broker
        self.channel.exchange_declare(exchange="logs", exchange_type="fanout")

        message = " ".join(sys.argv[1:]) or "A"
        self.channel.basic_publish(exchange="one_step", routing_key="", body=message)
        print(" [x] Sent %r" % message)

        # message = " ".join(sys.argv[1:]) or "B"
        # self.channel.basic_publish(exchange="two_step", routing_key="", body=message)
        # print(" [x] Sent %r" % message)
        #
        # message = " ".join(sys.argv[1:]) or "C"
        # self.channel.basic_publish(exchange="three_step", routing_key="", body=message)
        # print(" [x] Sent %r" % message)
        self.connection.close()

if __name__ == '__main__':
    pt = ProducerTool()
    pt.al_producer_message()


