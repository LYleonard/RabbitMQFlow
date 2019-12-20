#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Created by LYleonard on 2019/12/18
# Describe:
import time

from MyProject.Producer2ConsumerUtils import ProducerConsumerUtils

class TrackGroupProducerConsumerUtils(ProducerConsumerUtils):
    def callback_work_queues_consumer(self, ch, method, properties, body):
        """
        Work Queues 消息消费者回调方法,重写
        :param ch:
        :param method:
        :param properties:
        :param body: 消息体
        :return: null
        """
        # print("Work Queues callback_work_queues_consumer ==> Received %r" % body)
        # time.sleep(body.count(b'.'))
        # print("Work Queues callback_work_queues_consumer ==> Done")
        ch.basic_ack(delivery_tag=method.delivery_tag)

        body_message = str(body, encoding="utf-8")
        if body_message == "DG_Track_SEND_OK":
            print("============algorithm_OK==============")
            print("Message for subscribe1: ", body_message)
            print("Will Process TrackGroup's WorkFlow ....")
            print("=============TrackGroup's=============")

            tg_queue_name = "Algorithm_Start"
            tg_message = "TG_SEND_OK"
            pcu = ProducerConsumerUtils(host=self.host, username=self.username, password=self.password)
            pcu.work_queues_producer(queue_name=tg_queue_name, message=tg_message)

if __name__ == '__main__':
    username = "root"
    password = "root"
    host = "192.168.29.129"
    queue_name = "DG_Track_SEND_OK"
    pt = TrackGroupProducerConsumerUtils(username=username, password=password, host=host)
    pt.work_queues_consumer(queue_name=queue_name)
