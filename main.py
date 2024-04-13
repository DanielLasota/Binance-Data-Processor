from daemon_manager.daemon_manager import DaemonManager

if __name__ == "__main__":
    manager = DaemonManager(
        config_path='config.json',
        dump_path='C:/Users/daniellasota/testcik'
    )
    manager.run()
