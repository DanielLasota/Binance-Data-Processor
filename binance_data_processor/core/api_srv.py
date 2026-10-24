# import psutil
import uvicorn

from binance_data_processor.core.light_api import LightApi


class ApiSrv:

    __slots__ = (
        'callback',
        'app',
        'server'
    )

    def __init__(self, callback):
        self.callback = callback
        self.app = None
        self.server = None

    def run(self):
        #todo: rem
        # import os
        # p = psutil.Process(os.getpid())
        # print(f"[PSUTIL] name={p.name()} pid={p.pid} exe={p.exe()} {psutil.Process(p.pid).cpu_affinity()}")

        self.app = LightApi(self.callback)
        cfg = uvicorn.Config(self.app, host="127.0.0.1", port=5000, log_level="error")
        self.server = uvicorn.Server(cfg)
        self.server.run()

    def stop(self):
        if self.server:
            self.server.should_exit = True
