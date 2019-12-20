#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Created by LYleonard on 2019/12/18
# Describe: Publish/Subscribe--测试

import pika

class ConsumeTool:
    def __init__(self):
        credentials = pika.PlainCredentials(username="root", password="root")
        conn = pika.BlockingConnection(pika.ConnectionParameters(host="192.168.29.129", credentials=credentials))
        self.channel = conn.channel()

    def create_broker(self):
        # 创建broker
        self.channel.exchange_declare(exchange="one_step", exchange_type="fanout")
        # 声明一个队列，生产者和消费者都要声明一个相同的队列，用来防止万一某一方挂了，另一方能正常运行
        result = self.channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        # 把队列和中间人绑定
        self.channel.queue_bind(exchange='one_step', queue=queue_name)
        print(' [*] Waiting for logs. To exit press CTRL+C')
        self.channel.basic_consume(queue=queue_name, on_message_callback=self.callback, auto_ack=True)
        self.channel.start_consuming()

    def callback(self, ch, method, properties, body):
        print("One_step %r" % body)

if __name__ == '__main__':
    ct = ConsumeTool()
    ct.create_broker()