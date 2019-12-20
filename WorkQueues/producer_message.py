#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Created by LYleonard on 2019/12/18
# Describe: Work Queues

import pika
import sys

credentials = pika.PlainCredentials(username='root', password='root')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='192.168.29.129', credentials=credentials))
channel = connection.channel()

def work_queues_producer():
    channel.queue_declare(queue='AAAAA_test Queue', durable=True)
    message = ' '.join(sys.argv[1:]) or "Hello World!"
    # message = sys.argv[1:]
    channel.basic_publish(
        exchange='',
        routing_key='task_queue',
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ))
    print(" [x] Sent %r" % message)
    connection.close()

work_queues_producer()