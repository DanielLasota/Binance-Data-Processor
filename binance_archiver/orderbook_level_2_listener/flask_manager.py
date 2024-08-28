from flask import Flask, request
from flask_cors import CORS
import json
import logging
from threading import Thread, Event
from queue import Queue

from binance_archiver.orderbook_level_2_listener.observer import Observer


class FlaskManager(Observer):
    def __init__(self):
        super().__init__()
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

    def update(self, message):
        self.data_queue.put(message)
        self.data_updated.set()

    def setup_routes(self):
        @self.app.route('/post', methods=['POST'])
        def receive_data():
            data = request.get_json()
            print(f'>> {list(data.items())[0][1]}')
            self.notify_observers(data)
            return "Data received"

        @self.app.route('/shutdown', methods=['POST'])
        def shutdown():
            terminate_func = request.environ.get('werkzeug.server.shutdown')
            if terminate_func is not None:
                terminate_func()
            return "Server shutting down..."

    def stream(self):
        while True:
            self.data_updated.wait()
            while not self.data_queue.empty():
                data = self.data_queue.get()
                yield f"data: {json.dumps(data)}\n\n"
            self.data_updated.clear()

    def app_init(self):
        self.app.run(threaded=False, debug=False)

    def run(self):
        self.server_thread = Thread(target=self.app_init)
        self.server_thread.daemon = True
        self.server_thread.start()

    def stop(self):
        if self.server_thread and self.server_thread.is_alive():
            import requests
            try:
                requests.post('http://localhost:5000/shutdown')
                self.server_thread.join()
                print('joined')
            except requests.exceptions.RequestException as e:
                print(f"Error during server shutdown: {e}")

'''
curl -X POST http://localhost:5000/post -H "Content-Type: application/json" -d '{"key": "value"}'
'''

