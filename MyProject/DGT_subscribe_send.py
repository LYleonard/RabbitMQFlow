#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Created by LYleonard on 2019/12/18
# Describe:
from MyProject.Producer2ConsumerUtils import ProducerConsumerUtils

class TrackDataGroupProducerConsumerUtils(ProducerConsumerUtils):
    def callback(self, ch, method, properties, body):
        print("Message: %s" % body)
        body_message = str(body, encoding="utf-8")
        if body_message == "algorithm_OK":
            print("============algorithm_OK==============")
            print("Message for subscribe1: ", body_message)
            print("Will Process DataGroup's Track Extract WorkFlow ....")
            print("=======DataGroup's Track Extract=======")

            dgt_queue_name = dgt_message = "DG_Track_SEND_OK"
            pcu = ProducerConsumerUtils(host=self.host, username=self.username, password=self.password)
            pcu.work_queues_producer(queue_name=dgt_queue_name, message=dgt_message)



if __name__ == '__main__':
    username = "root"
    password = "root"
    host = "192.168.29.129"
    exchange = "algorithm_OK"
    pt = TrackDataGroupProducerConsumerUtils(username=username, password=password, host=host)
    pt.subscribe_message(exchange=exchange)
