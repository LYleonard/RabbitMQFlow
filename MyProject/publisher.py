#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Created by LYleonard on 2019/12/18
# Describe:

from MyProject.Producer2ConsumerUtils import ProducerConsumerUtils

if __name__ == '__main__':
    ## 定义该阶段处理流程
    username = "root"
    password = "root"
    host = "192.168.29.129"
    message = exchange = "algorithm_OK"
    pt = ProducerConsumerUtils(username=username, password=password, host=host)
    pt.publish_message(exchange=exchange, message=message)