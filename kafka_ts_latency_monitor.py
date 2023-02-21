import logging
import os
import time
from confluent_kafka import Consumer

import transport_pb2
from subscribe import Subscribe
from influx_worker import KafkaInfluxWorker

subscribe = Subscribe()

KAFKA_HOST = os.getenv('KAFKA_HOST', 'localhost:9092')
GROUP = 'iothub-monitor-latency'
TOPIC_PREFIX = 'tb_rule_engine.main'

logger = logging.getLogger('consumer')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
logger.addHandler(handler)
consumer = Consumer({
    'bootstrap.servers': KAFKA_HOST,
    'group.id': GROUP,
    'auto.offset.reset': 'latest'
}, logger=logger)

consumer.subscribe([f'{TOPIC_PREFIX}.{i}' for i in range(10)])

worker_kafka = KafkaInfluxWorker(subscribe)
worker_kafka.start()

if __name__ == '__main__':
    while True:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue
            ts_received = time.time() * 1000
            re = transport_pb2.ToRuleEngineMsg()
            re.ParseFromString(msg.value())
            tb_msg = transport_pb2.TbMsgProto()
            tb_msg.ParseFromString(re.tbMsg)
            if tb_msg.type == 'POST_TELEMETRY_REQUEST':
                (is_existed, device_id) = subscribe.check_if_subscribe_by_lmsb(tb_msg.entityIdLSB, tb_msg.entityIdMSB)
                if is_existed is True:
                    # filter and put data to worker
                    worker_kafka.enqueue(device_id, tb_msg, ts_received)
                    # TODO log
        except KeyboardInterrupt:
            break

    consumer.close()
