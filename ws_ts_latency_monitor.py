import time
import os
from queue import Queue
from typing import Dict, Tuple
import threading
import json

import websocket
import requests

from subscribe import Subscribe
from utils import logger
from influx_worker import WsInfluxWorker

THINGSBOARD_URL = os.getenv('THINGSBOARD_URL', 'http://localhost/api')
THINGSBOARD_URL_WS = os.getenv('THINGSBOARD_URL_WS', 'wss://localhost')
THINGSBOARD_ACCOUNT = os.getenv('THINGSBOARD_ACCOUNT', '')
THINGSBOARD_PASSWORD = os.getenv('THINGSBOARD_PASSWORD', '')
GAP = 1 * 1000
GAP_LAST = 2 * 1000


class TBWSClient(websocket.WebSocketApp):
    WS_REQ_NEW_CONNECT = 'ws_req_newconnect'

    def __init__(self, url, q_req_new_ws=Queue()):
        super().__init__(url,
                         on_open=self.ws_on_open,
                         on_close=self.ws_on_close,
                         on_error=self.ws_on_error,
                         on_message=self.ws_on_message,
                         on_ping=self.on_ping,
                         on_pong=self.on_pong,
                         )
        '''
        ref:
        1. https://websocket-client.readthedocs.io/en/latest/examples.html
        2. https://github.com/websocket-client/websocket-client/blob/master/websocket/_app.py
        '''
        self.ws_subid = 0
        self.ws_subid_mapping = {}
        self.q_req_new_ws = q_req_new_ws
        self.count = 0
        self.init = False
        self.last_ts = {} # example{'sub_id1': time.time(), 'sub_id2': time.time()}

    def on_ping(self, ws, message):
        logger.debug("Got a ping! A pong reply has already been automatically sent.")

    def on_pong(self, ws, message):
        logger.debug("Got a pong! No need to respond")

    def _submit_req_new_ws(self):
        # submit to this thread owner for a new websocket client request
        # in case of token invaid, other reason
        self.q_req_new_ws.put(TBWSClient.WS_REQ_NEW_CONNECT)

    def ws_on_error(self, ws, error=Exception('')):
        logger.info(f'websocket error {str(error)}')
        self._submit_req_new_ws()

    def ws_on_close(self, ws, close_status_code=0, close_msg=''):
        logger.info('websocket connection closed')
        self._submit_req_new_ws()

    def ws_on_open(self, ws):
        topic = Subscribe.load_tb_ws_topic()
        logger.info('websocket connection opened')
        logger.debug(f'subscribe topic: {topic}')
        # ws.send("This is a ping", websocket.ABNF.OPCODE_PING)
        ws.send(topic)

    def ws_on_message(self, ws, message=''):
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
        ts_received = time.time() * 1000
        data = json.loads(message)
        sub_id = int(data['subscriptionId'])
        # filter by subscribe key
        (is_existed, device_id) = subscribe.check_if_subscribe_by_sub_id(sub_id)
        if is_existed is True:
            # filter and put data to worker
            worker_ws.enqueue(device_id, data, ts_received)
            # TODO log


class TBRestClient():
    def __init__(self, username='', password=''):
        self.username = username
        self.password = password
        self.tb_token = ''
        message, status_code = self.login()
        assert message == ''
        assert status_code == 200
        self.ws = None
        self.thread_ws = None

        self.q_req_new_ws = Queue()
        self.q_req_new_ws.put(TBWSClient.WS_REQ_NEW_CONNECT)
        self.t_ws_manager = threading.Thread(target=self.ws_thread_manager,
                                             daemon=True)
        self.t_ws_manager.start()

    def ws_thread_manager(self):
        q_interval = 60
        while True:
            data = self.q_req_new_ws.get()
            # receive request from ws client child, to perform single ws instance
            if data == TBWSClient.WS_REQ_NEW_CONNECT:
                if isinstance(self.ws, websocket.WebSocketApp) is True:
                    self.ws.close()
                if isinstance(self.thread_ws, threading.Thread) is True:
                    self.thread_ws.join()
                self.thread_ws = threading.Thread(target=self.start_ws, daemon=True)
                self.thread_ws.start()
            time.sleep(q_interval)

    def start_ws(self):
        message, status_code = self.login()
        assert message == ''
        assert status_code == 200
        url = f'{THINGSBOARD_URL_WS}/api/ws/plugins/telemetry?token={self.tb_token}'
        self.ws = TBWSClient(url, self.q_req_new_ws)
        self.ws.run_forever()

    def get_auth_headers(self) -> Dict:
        return {
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate',
            'X-Authorization': f'Bearer {self.tb_token}'
        }

    def login(self) -> Tuple[str, int]:
        # login to IOT Hub
        data = {
            'username': self.username,
            'password': self.password
        }
        message, status_code = '', 200
        r = requests.post(f'{THINGSBOARD_URL}/auth/login', json=data)
        status_code = r.status_code
        if status_code == 200:
            self.tb_token = r.json()['token']
        else:
            message = r.json()['message']
            return message, status_code
        return message, status_code


def start(tb_client):
    message, status_code = tb_client.login()
    assert message == ''
    assert status_code == 200
    while True:
        time.sleep(60)


subscribe = Subscribe()
worker_ws = WsInfluxWorker(subscribe)
worker_ws.start()

if __name__ == '__main__':
    tb_client = TBRestClient(THINGSBOARD_ACCOUNT,
                             THINGSBOARD_PASSWORD)
    start(tb_client)
