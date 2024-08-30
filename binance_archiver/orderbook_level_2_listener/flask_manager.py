from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from threading import Thread
import uvicorn
import requests


class FastAPIManager:
    def __init__(self):
        self.app = FastAPI()
        self.setup_routes()
        self.server_thread = None
        self.notify_cli = None
        self.server = None

        # Allow CORS for all domains
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    def set_callback(self, callback):
        self.notify_cli = callback

    def setup_routes(self):
        @self.app.post("/post")
        async def receive_data(request: Request):
            data = await request.json()
            print(f'>> {list(data.items())[0][1]}')
            if self.notify_cli:
                self.notify_cli(data)
            return {"message": "Data received"}

        @self.app.post("/shutdown")
        async def shutdown():
            if self.server:
                self.server.should_exit = True
            return {"message": "Server shutting down..."}

    def app_init(self):
        config = uvicorn.Config(self.app, host="127.0.0.1", port=5000, log_level="error")
        self.server = uvicorn.Server(config)
        self.server.run()

    def run(self):
        self.server_thread = Thread(target=self.app_init, name='fastapi_manager_thread')
        self.server_thread.daemon = True
        self.server_thread.start()

    def shutdown(self):
        try:
            requests.post('http://127.0.0.1:5000/shutdown')
        except requests.exceptions.ConnectionError:
            print("Server is already shut down.")
        if self.server_thread and self.server_thread.is_alive():
            self.server_thread.join(timeout=2)
            if self.server_thread.is_alive():
                print("Warning: server thread did not terminate properly.")


'''
curl -X POST http://localhost:5000/post -H "Content-Type: application/json" -d '{"subscribe": ["StreamType.DifferenceDepth", "Market.SPOT", 'xrpusdt']}'
curl -X POST http://localhost:5000/post -H "Content-Type: application/json" -d "{\"key\": \"value\"}"
curl -X POST http://localhost:5000/post -H "Content-Type: application/json" -d "{"modify_subscription": {"type": "subscribe", "stream_type": "DifferenceDepth", "market": "SPOT", "asset": "xrpusdt"}}"
curl -X POST http://localhost:5000/post -H "Content-Type: application/json" -d "{\"modify_subscription\": {\"type\": \"subscribe\", \"stream_type\": \"DifferenceDepth\", \"market\": \"SPOT\", \"asset\": \"xrpusdt\"}}"
curl -X POST http://localhost:5000/post -H "Content-Type: application/json" -d "{\"modify_subscription\": {\"type\": \"subscribe\", \"market\": \"SPOT\", \"asset\": \"xrpusdt\"}}"
'''
