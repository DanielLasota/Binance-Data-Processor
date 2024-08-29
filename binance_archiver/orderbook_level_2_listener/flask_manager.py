from flask import Flask, request
from flask_cors import CORS
import json
import logging
from threading import Thread, Event
from queue import Queue


class FlaskManager:
    def __init__(self):
        self.observers = []
        self.app = Flask(__name__)
        CORS(self.app)
        self.setup_routes()
        self.data_queue = Queue()
        self.data_updated = Event()
        self.stop_event = Event()
        log = logging.getLogger('werkzeug')
        log.setLevel(logging.ERROR)
        self.server_thread = None
        self.notify_cli = None

    def set_callback(self, callback):
        self.notify_cli = callback

    def setup_routes(self):
        @self.app.route('/post', methods=['POST'])
        def receive_data():
            data = request.get_json()
            print(f'>> {list(data.items())[0][1]}')
            self.notify_cli(data)
            return "Data received"

    def app_init(self):
        self.app.run(threaded=False, debug=False)

    def run(self):
        self.server_thread = Thread(target=self.app_init)
        self.server_thread.daemon = True
        self.server_thread.start()

'''
curl -X POST http://localhost:5000/post -H "Content-Type: application/json" -d '{"subscribe": ["StreamType.DifferenceDepth", "Market.SPOT", 'xrpusdt']}'
curl -X POST http://localhost:5000/post -H "Content-Type: application/json" -d "{\"key\": \"value\"}"
curl -X POST http://localhost:5000/post -H "Content-Type: application/json" -d "{"modify_subscription": {"type": "subscribe", "stream_type": "DifferenceDepth", "market": "SPOT", "asset": "xrpusdt"}}"
curl -X POST http://localhost:5000/post -H "Content-Type: application/json" -d "{\"modify_subscription\": {\"type\": \"subscribe\", \"stream_type\": \"DifferenceDepth\", \"market\": \"SPOT\", \"asset\": \"xrpusdt\"}}"
curl -X POST http://localhost:5000/post -H "Content-Type: application/json" -d "{\"modify_subscription\": {\"type\": \"subscribe\", \"stream_type\": \"DIFFERENCE_DEPTH\", \"market\": \"SPOT\", \"asset\": \"xrpusdt\"}}"
'''
