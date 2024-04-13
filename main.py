from daemon_manager.daemon_manager import DaemonManager

if __name__ == "__main__":
    manager = DaemonManager(
        env_path='',
        config_path='',
        dump_path=''
    )
    manager.run()
