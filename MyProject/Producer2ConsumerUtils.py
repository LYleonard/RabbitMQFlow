#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Created by LYleonard on 2019/12/18
# Describe: Publish/Subscribe模式
import time

import pika

class ProducerConsumerUtils:
    def __init__(self, username, password, host):
        self.username = username
        self.password = password
        self.host = host

    def build_connection(self, host, username, password):
        """
        建立连接
        :param host: 主机IP
        :param username: RabbitMQ用户名
        :param password: RabbitMQ用户密码
        :return: null
        """
        # 凭证信息
        credentials = pika.PlainCredentials(username=username, password=password)
        # 连接参数设置
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, credentials=credentials))
        # 管道
        try:
            channel = connection.channel()
            return channel, connection
        except Exception as e:
            print("Connection channel is not built！ Error：", e)

    def publish_message(self, exchange, message):
        """
        RabbitMQ Publish/Subscribe模式 - Publish, exchange_type fanout.
        :param exchange: exchange名
        :param message: 消息
        :return: null
        """
        # 创建broker
        channel, connection = self.build_connection(host=self.host, password=self.password, username=self.username)
        channel.exchange_declare(exchange=exchange, exchange_type="fanout")

        channel.basic_publish(exchange=exchange, routing_key="", body=message)
        print(" Publish/Subscribe publish_message ==> Algorithm first step is OK! Sent %r" % message)
        # 注意这里关闭了连接！！
        connection.close()

    def subscribe_message(self, exchange):
        """
        RabbitMQ Publish/Subscribe模式 - Subscribe, exchange_type fanout.
        :param exchange: exchange名
        :return: null
        """
        # 创建broker
        channel, connection = self.build_connection(host=self.host, password=self.password, username=self.username)
        global logger
        channel.exchange_declare(exchange=exchange, exchange_type="fanout")
        # 声明一个队列，生产者和消费者都要声明一个相同的队列，用来防止万一某一方挂了，另一方能正常运行
        result = channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        # 把队列和中间人绑定
        channel.queue_bind(exchange=exchange, queue=queue_name)

        print("Publish/Subscribe subscribe_message ==> Waiting for message. To exit press CTRL+C")
        channel.basic_consume(queue=queue_name, on_message_callback=self.callback, auto_ack=True)
        try:
            from venv import logger
            logger.info("starting receiving message...")
            channel.start_consuming()
        except KeyboardInterrupt as e:
            logger.info(e)
        finally:
            logger.info("Warm shutdown...")
            connection.close()
            logger.info("Warm shutdown...Done")

    def callback(self, ch, method, properties, body):
        """
        回调函数，在此调用各模块方法
        :param ch:
        :param method:
        :param properties:
        :param body: bytes类型的消息
        :return: null
        """
        print("Publish/Subscribe subscribe_message callback ==> Message: %s" % body)
        body_message = str(body, encoding="utf-8")
        if body_message == "algorithm_OK":
            print("===========algorithm_OK============")
            print("Message for subscribe1: ", body_message)
            print("===================================")
        pass

    def work_queues_producer(self, queue_name, message):
        """
        Work Queues 消息生产者方法
        :param queue_name: queue_name and routing_key
        :param message: 消息内容
        :return: null
        """
        channel, connection = self.build_connection(host=self.host, password=self.password, username=self.username)
        channel.queue_declare(queue=queue_name, durable=True)
        # message = sys.argv[1:]
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            ))
        print("Work Queues work_queues_producer %s Sent %r OK!" % (queue_name,message))
        # connection.close()
        return None

    def work_queues_consumer(self, queue_name):
        """
        Work Queues 消息消费者方法
        :param queue_name: queue_name and routing_key
        :return: null
        """
        channel, connection = self.build_connection(host=self.host, password=self.password, username=self.username)
        channel.queue_declare(queue=queue_name, durable=True)
        print('Work Queues work_queues_consumer ==> Waiting for messages. To exit press CTRL+C')

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=queue_name, on_message_callback=self.callback_work_queues_consumer)
        channel.start_consuming()

    def callback_work_queues_consumer(self, ch, method, properties, body):
        """
        Work Queues 消息消费者回调方法
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
        # body_message = str(body, encoding="utf-8")
        # if body_message == "OK":
        #     print("===========algorithm_OK============")
        #     print("Message for subscribe1: ", body_message)
        #     print("===================================")
        # pass

if __name__ == '__main__':
    username = "root"
    password = "root"
    host = "192.168.29.129"
    message = exchange = "algorithm_OK"
    pt = ProducerConsumerUtils(username=username, password=password, host=host)
    pt.publish_message(exchange=exchange, message=message)
