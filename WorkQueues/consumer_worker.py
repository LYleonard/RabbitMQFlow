#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Created by LYleonard on 2019/12/18
# Describe: Work Queues
import pika
import time
credentials = pika.PlainCredentials(username='root', password='root')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='192.168.29.129', credentials=credentials))
channel = connection.channel()


def work_queues_consumer():
    channel.queue_declare(queue='task_queue', durable=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')

    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body)
        time.sleep(body.count(b'.'))
        print(" [x] Done")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='task_queue', on_message_callback=callback)
    channel.start_consuming()


work_queues_consumer()