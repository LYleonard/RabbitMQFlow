#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Created by LYleonard on 2019/12/18
# Describe:
import time

from MyProject.Producer2ConsumerUtils import ProducerConsumerUtils

class AlgorithmGroupProducerConsumerUtils(ProducerConsumerUtils):

    def __init__(self, username, password, host):
        self.username = username
        self.password = password
        self.host = host
        self.message_list = []

    def callback_work_queues_consumer(self, ch, method, properties, body):
        """
        Work Queues 消息消费者回调方法,重写
        :param ch:
        :param method:
        :param properties:
        :param body: 消息体
        :return: null
        """
        print("Work Queues callback_work_queues_consumer ==> Received %r" % body)
        time.sleep(body.count(b'.'))
        print("Work Queues callback_work_queues_consumer ==> Done")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        self.message_list.append(str(body, encoding="utf-8"))

        if "DG_SEND_OK" in self.message_list and "TG_SEND_OK" in self.message_list and "IG_SEND_OK" in self.message_list:
            print("============DG_TG_IG==============")
            print("Message for subscribe1: ", self.message_list)
            print("Will Process AlgorithmGroup's WorkFlow ....")
            print("=========Algorithm Start WorkFlow=========")
            self.message_list = []

if __name__ == '__main__':
    username = "root"
    password = "root"
    host = "192.168.29.129"
    queue_name = "Algorithm_Start"
    pt = AlgorithmGroupProducerConsumerUtils(username=username, password=password, host=host)
    pt.work_queues_consumer(queue_name=queue_name)
