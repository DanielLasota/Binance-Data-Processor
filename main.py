from daemon_manager.daemon_manager import DaemonManager

if __name__ == "__main__":
    manager = DaemonManager(
        env_path='C:/Users/daniellasota/bi_arch.env',
        config_path='config.json',
        dump_path=''
    )
    manager.run()
