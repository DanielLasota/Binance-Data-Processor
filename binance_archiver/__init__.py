from binance_archiver.daemon_manager.DaemonManager import DaemonManager

__docstring__ = '''
Sample usage:

manager = DaemonManager(
    config=config,
    dump_path='temp',
    remove_csv_after_zip=True,
    remove_zip_after_upload=False,
    send_zip_to_blob=False,
    azure_blob_parameters_with_key=azure_blob_parameters_with_key,
    container_name=container_name
)

manager.run()

'''

__all__ = ['DaemonManager']

__author__ = "Daniel Lasota <grossmann.root@gmail.com>"
__status__ = "production"
__version__ = "2.1.3.7"
__date__ = "05 may 2024"
