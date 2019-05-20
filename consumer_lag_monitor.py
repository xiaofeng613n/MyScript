#!/usr/bin/python
# coding:utf-8
import getopt
import sys
import json
import hashlib
import requests

import time
import datetime
import pytz

from kafka import SimpleClient, KafkaConsumer
from kafka.common import OffsetRequestPayload, TopicPartition

from kazoo.client import KazooClient,KazooState

import logging
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler("consumer_lag_monitor.log")
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
#console = logging.StreamHandler()
#console.setLevel(logging.INFO)
logger.addHandler(handler)
#logger.addHandler(console)


class KafkaHelper:

    def __init__(self, brokers, zk_host, zk_path):
        self.brokers = brokers
        self.client = SimpleClient(brokers)
        self.consumer = None
        self.zk_host = zk_host
        self.zk_path = zk_path
        self.zk = None

    #获取一个topic的offset值的和
    def get_topic_offset(self, topic):
        partitions = self.client.topic_partitions[topic]
        offset_requests = [OffsetRequestPayload(topic, p, -1, 1) for p in partitions.keys()]
        offsets_responses = self.client.send_offset_request(offset_requests)
        return sum([r.offsets[0] for r in offsets_responses])

    #获取一个topic特定group已经消费的offset值的和
    def get_group_offset(self, group_id, topic):
        if self.consumer is None:
            self.consumer = KafkaConsumer(bootstrap_servers=self.brokers, group_id=group_id)
        pts = [TopicPartition(topic=topic, partition=i) for i in self.consumer.partitions_for_topic(topic)]
        result = self.consumer._coordinator.fetch_committed_offsets(pts)
        return sum([r.offset for r in result.values()])

    def get_group_lag(self, group_id, topic):
        topic_offset = self.get_topic_offset(topic)
        # group_offset = self.get_group_offset(group_id, topic)
        group_offset = self.get_group_offset_zk(group_id, topic)
        lag = topic_offset - group_offset
        return lag

    def get_group_offset_zk(self, group_id, topic):
        if self.zk is None:
            self.zk = KazooClient(hosts=self.zk_host)
            self.zk.start()
        group_offset = 0
        for partition_id in [0, 1, 2]:
            data, stat = self.zk.get(self.zk_path + '/consumers/' + group_id + '/offsets/' + topic + '/' + str(partition_id))
            group_offset = group_offset + int(data)
        return group_offset

def send_alert(lag):
    #tz = pytz.timezone('Asia/Shanghai')  # 东八区
    t = datetime.datetime.fromtimestamp(int(time.time()), pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')

    url = rms_url + project_code
    data = {'point_code': point_code,
            'error_code': error_code,
            'notice_time': t,
            'content': content + str(lag),
            'level': 2,
            'is_test': 0}

    data_json = json.dumps(data)
    hl = hashlib.md5()
    hl.update((token + data_json).encode(encoding='utf-8'))
    payload = 'token=' + hl.hexdigest() + '&data=' + data_json
    headers = {
        'content-type': "application/x-www-form-urlencoded"
    }
    response = requests.request("POST", url, data=payload, headers=headers, timeout=5)
    logger.info('response:' + response.content)

def send_alert_elog():
    t = datetime.datetime.fromtimestamp(int(time.time()), pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')

    url = rms_url + project_code
    data = {'point_code': '',
            'error_code': '',
            'notice_time': t,
            'content': 'consumer_lag_monitor.py脚本异常',
            'level': 2,
            'is_test': 0}

    data_json = json.dumps(data)
    hl = hashlib.md5()
    hl.update(('QFucpVMHcFfo3OZXlCZm' + data_json).encode(encoding='utf-8'))
    payload = 'token=' + hl.hexdigest() + '&data=' + data_json
    headers = {
        'content-type': "application/x-www-form-urlencoded"
    }
    response = requests.request("POST", url, data=payload, headers=headers, timeout=5)
    logger.info('response:' + response.content)


def monitor(args):
    brokers = args[0]
    group_id = args[1]
    topic = args[2]

    zk_host = args[3]
    zk_path = args[4]

    kafka_helper = KafkaHelper(brokers=brokers, zk_host=zk_host, zk_path=zk_path)

    lag = kafka_helper.get_group_lag(group_id, topic)
    logger.info('lag:' + str(lag))
    while True:
        try:
            lag = kafka_helper.get_group_lag(group_id, topic)
            logger.info('lag:' + str(lag))
            if lag > max_lag:
                send_alert(lag)
        except Exception as info:
            logger.error(info)
            send_alert_elog()
        time.sleep(sleep_time)


def main():
    # parse command line options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hm", ["help", "m"])
    except getopt.error, msg:
        print msg
        print "for help use --help"
        sys.exit(2)
    # process options
    for o, a in opts:
        if o in ("-h", "--help"):
            print __doc__
            sys.exit(0)
        elif o in ("-m"):
            monitor(args)
        else:
            print 'unknown opt : ' + o
            sys.exit(0)

rms_url = ''
project_code = ''
point_code = ''
error_code = ''
token = ''
content = ''
sleep_time = 30
max_lag = 500000

#python2.7
#pip install kafka-python
#pip install pytz
#pip install hashlib
#pip install requests
#pip install kazoo

# test
# nohup python consumer_lag_monitor.py -m xx:9092 console-consumer-20631 app1 xx:2181 /kafka >output 2>&1 &
if __name__ == "__main__":
    main()
