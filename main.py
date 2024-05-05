from daemon_manager.daemon_manager import DaemonManager

if __name__ == "__main__":
    manager = DaemonManager(
        config_path='config.json',
        env_path='C:/Users/daniellasota/archer.env',
        dump_path='',
        should_csv_be_removed_after_zip=True,
        should_zip_be_removed_after_upload=False,
        should_zip_be_sent=True
    )

    manager.run()
