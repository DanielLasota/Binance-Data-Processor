from binance_archiver.orderbook_level_2_listener.archiver_daemon import ArchiverDaemon

__docstring__ = '''
Sample usage:

manager = DaemonManager(
    config=config,
    dump_path='dump',
    remove_csv_after_zip=True,
    remove_zip_after_upload=False,
    send_zip_to_blob=False,
    azure_blob_parameters_with_key=azure_blob_parameters_with_key,
    container_name=container_name
)

manager.run()

'''

__all__ = ['ArchiverDaemon']

__author__ = "Daniel Lasota <grossmann.root@gmail.com>"
__status__ = "development"
__version__ = "0.0.1"
__date__ = "12-09-2024"

