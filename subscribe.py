import json
import os
import time
import uuid

from influxdb_client import Point
from transport_pb2 import TbMsgProto

from utils import logger, get_least_most_significant_bits

FILE_TB_WS_TOPIC = os.getenv('FILE_TB_WS_TOPIC', 'source.json')

INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", '')
INFLUX_ORG = os.getenv('INFLUX_ORG', '')
INFLUX_URL = os.getenv('INFLUX_URL', 'http://localhost:8086')
INFLUX_MEASUREMENT = os.getenv('INFLUX_MEASUREMENT', '')
INFLUX_BUCKET = os.getenv('INFLUX_BUCKET', '')
MONITOR_ENV = os.getenv('MONITOR_ENV', 'it-aks')


class Subscribe():
    '''
    * According to Thingsboard telemetry subscribe, if device_id contain an
    empty set, it means subscribe all, otherwise, you can subscribe keys
    you're concen. Here's a real time monitor device telemetry key
    Ref: https://thingsboard.io/docs/user-guide/telemetry/
    data structure:
    _realtime_monitor_data = {"device_id1": {"key1", "key2"}, "device_id2": {}
    '''
    _realtime_monitor_data = {}
    '''
    * To achieve Thingsboard websocket subscribe telemetry client subscribe id
    and device id mapping, here's a lookup table to map this relationship
    _subid2device_id_table = {"sub_id1":"device_id1", "sub_id2": "device_id2"}
    '''
    _subid2device_id_table = {}
    '''
    * To reconstruct Kafka TbMsgProto entityIdMSB, entityIdLSB in uuid to
    represent entityId.
    _lsbmsb2device_id_table = {<lsb1>: {<msb1>: "device_id1"}}
    '''
    _lsbmsb2device_id_table = {}

    @staticmethod
    def load_tb_ws_topic():
        with open(f'./{FILE_TB_WS_TOPIC}', 'r') as f:
            tb_ws_topic = f.read()
        return tb_ws_topic

    def __init__(self):
        src = json.loads(self.load_tb_ws_topic())
        for ts_sub_cmd in src.get('tsSubCmds'):
            device_id = ts_sub_cmd['entityId']
            self._subid2device_id_table[ts_sub_cmd["cmdId"]] = device_id
            keys = set()
            for key in ts_sub_cmd.get('keys', '').split(','):
                keys.add(key)
            self._realtime_monitor_data[device_id] = keys
            (lsb, msb) = get_least_most_significant_bits(device_id)
            if self._lsbmsb2device_id_table.get(lsb, None) is None:
                self._lsbmsb2device_id_table[lsb] = {}
            self._lsbmsb2device_id_table[lsb][msb] = device_id
        logger.debug(self._realtime_monitor_data)
        logger.debug(self._subid2device_id_table)
        logger.debug(self._lsbmsb2device_id_table)

    def get_deivce_list(self):
        return list(self._realtime_monitor_data.keys())

    def check_if_subscribe_by_lmsb(self, lsb: int, msb: int) -> (bool, str):
        if self._lsbmsb2device_id_table.get(lsb, None) is not None:
            device_id = self._lsbmsb2device_id_table[lsb].get(msb, '')
            if device_id != '':
                return True, device_id
        return False, ''

    def check_if_subscribe_by_sub_id(self, sub_id=int()) -> (bool, str):
        if self._subid2device_id_table.get(sub_id, None) is None:
            return False, ''
        device_id = self._subid2device_id_table.get(sub_id, '')
        return True, device_id

    def check_if_subscribe_by_device_id(self,
                                        device_id=str(uuid.uuid4()),
                                        key='') -> bool:
        if self._realtime_monitor_data.get(device_id, None) is None:
            return False
        elif len(self._realtime_monitor_data.get(device_id, set())) == 0:
            return True
        elif key in self._realtime_monitor_data.get(device_id):
            return True
        return False

    def ws_filter(self, device_id=str(uuid.uuid4()), ws_data=dict(),
                  ts_received=time.time()*1000) -> Point:
        '''
        # telemetry sample
        {
            "subscriptionId":1,
            "errorCode":0,
            "errorMsg":null,
            "data":{
                "batteryLevel":[[1652170331846,"80"]],
                "leakage":[[1652170331846,"true"]],
                "pulseCounter":[[1652170331846,"3106310"]]
            },
            "latestValues":{
                "pulseCounter":1652170331846,
                "leakage":1652170331846,
                "batteryLevel":1652170331846
            }
        }
        '''
        point = Point(INFLUX_MEASUREMENT).tag('device_id', device_id).tag('env', MONITOR_ENV).tag('client', 'ws')
        existed = False
        for key, value in ws_data.get('data', dict()).items():
            if self.check_if_subscribe_by_device_id(device_id, key):
                ts_transmitted = value[0][0]
                time_delta = float(ts_received - ts_transmitted) / 1000
                point.field(key, time_delta)
                existed = True
        if existed is True:
            point.time(int(ts_received * pow(10, 6)))
        else:
            point = None
        return point

    def kafka_filter(self, device_id=str(uuid.uuid4),
                     tb_msg=TbMsgProto(),
                     ts_received=int(time.time()*1000)) -> Point:
        '''
        # tbMsg sample
        id: "254725a5-a573-463b-90c1-2f43abcf8083"
        type: "POST_TELEMETRY_REQUEST"
        entityType: "DEVICE"
        entityIdMSB: -1111111111111111111
        entityIdLSB: -2222222222222222222
        metaData {
          data {
            key: "deviceName"
            value: "EV-ULTRA-METAL-241329"
          }
          data {
            key: "deviceType"
            value: "EV"
          }
          data {
            key: "ts"
            value: "1672580617186"
          }
        }
        data: "{\"mode\":{\"stop\"}}"
        ts: 1672580617261
        customerIdMSB: -3333333333333333333
        customerIdLSB: -4444444444444444444
        ctx {
        }
        '''
        point = Point(INFLUX_MEASUREMENT).tag('device_id', device_id).tag('env', MONITOR_ENV).tag('client', 'kafka')
        ts_transmitted = int(tb_msg.metaData.data.get('ts', 0))
        time_delta = float(ts_received - ts_transmitted) / 1000
        existed = False
        data = json.loads(tb_msg.data)
        for key, value in data.items():
            if self.check_if_subscribe_by_device_id(device_id, key):
                point.field(key, time_delta)
                existed = True
        if existed is True:
            point.time(int(ts_received * pow(10, 6)))
        else:
            point = None
        return point
