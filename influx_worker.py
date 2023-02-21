import sys
import os
import time
import uuid
import abc
import threading
from queue import Queue
from queue import Empty as QueueEmpty

from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.exceptions import InfluxDBError

from utils import logger
from subscribe import Subscribe
from transport_pb2 import TbMsgProto

INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", '')
INFLUX_ORG = os.getenv('INFLUX_ORG', '')
INFLUX_URL = os.getenv('INFLUX_URL', 'http://localhost:8086')
INFLUX_MEASUREMENT = os.getenv('INFLUX_MEASUREMENT', '')
INFLUX_BUCKET = os.getenv('INFLUX_BUCKET', '')
MONITOR_ENV = os.getenv('MONITOR_ENV', 'it-aks')
QUEUE_FETCHTIME = 60


class InfluxWorker():

    def __init__(self, subscribe: Subscribe):
        self.subscribe = subscribe
        self._queue = Queue()
        self._worker = None
        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN,
                                org=INFLUX_ORG)

        write_options = WriteOptions(batch_size=1000,
                                     flush_interval=1_000,
                                     jitter_interval=2_000,
                                     retry_interval=5_000,
                                     max_retries=5,
                                     max_retry_delay=30_000,
                                     exponential_base=2)
        self._write_api = client.write_api(write_options,
                                           success_callback=self._on_success,
                                           error_callback=self._on_error,
                                           retry_callback=self._on_retry)
        self.counter = 0

    def _on_success(self, conf: (str, str, str), data: str):
        logger.debug(f"Written batch: {conf}, data: {data}")

    def _on_error(self, conf: (str, str, str), data: str, exception: InfluxDBError):
        logger.info(f"Cannot write batch: {conf}, data: {data} due: {exception}")

    def _on_retry(self, conf: (str, str, str), data: str, exception: InfluxDBError):
        logger.info(f"Retryable error occurs for batch: {conf}, data: {data} retry: {exception}")

    def __del__(self):
        logger.info('__del__')
        self._worker.join()

    @abc.abstractmethod
    def enqueue():
        raise NotImplementedError()

    @abc.abstractmethod
    def _parse():
        raise NotImplementedError()

    def _write(self, point=Point(INFLUX_MEASUREMENT)):
        self._write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=[point])

    def _start_worker(self):
        while True:
            try:
                (device_id, data, ts_received) = self._queue.get(timeout=QUEUE_FETCHTIME)
                point = self._parse(device_id, data, ts_received)
                if point:
                    self._write(point)
                    if self.counter % 10000 == 0:
                        print(f'influx write counter: {self.counter}')
                    self.counter += 1
            except QueueEmpty:
                pass
            except Exception as ex:
                logger.error('influx worker exception: ' + str(ex))
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
                os._exit(1)
                break

    def start(self):
        logger.info('worker start')
        self._worker = threading.Thread(target=self._start_worker,
                                        daemon=True)
        self._worker.start()


class WsInfluxWorker(InfluxWorker):

    def __init__(self, subscribe: Subscribe):
        super().__init__(subscribe)

    def enqueue(self,
                device_id=str(uuid.uuid4()),
                ws_data=dict(),
                ts_received=int(time.time()*1000)):
        self._queue.put((device_id, ws_data, ts_received))

    def _parse(self, device_id=str(uuid.uuid4()),
               ws_data=dict(),
               ts_received=time.time()*1000) -> Point:
        return self.subscribe.ws_filter(device_id, ws_data, ts_received)


class KafkaInfluxWorker(InfluxWorker):

    def __init__(self, subscribe: Subscribe):
        super().__init__(subscribe)

    def enqueue(self,
                device_id=str(uuid.uuid4),
                tb_msg=TbMsgProto(),
                ts_received=int(time.time()*1000)):
        self._queue.put((device_id, tb_msg, ts_received))

    def _parse(self, device_id=str(uuid.uuid4),
               tb_msg=TbMsgProto(),
               ts_received=time.time()*1000) -> Point:
        return self.subscribe.kafka_filter(device_id, tb_msg, ts_received)
