import os
from dotenv import load_dotenv

from binance_archiver.scraper import download_csv_data


if __name__ == '__main__':

    # env_path = os.path.join(os.path.expanduser("~"), 'binance-archiver-1.env')
    # load_dotenv(env_path)
    load_dotenv('binance-archiver-2.env')

    backblaze_access_key_id = os.environ.get('BACKBLAZE_ACCESS_KEY_ID')
    backblaze_secret_access_key = os.environ.get('BACKBLAZE_SECRET_ACCESS_KEY')
    backblaze_endpoint_url = os.environ.get('BACKBLAZE_ENDPOINT_URL')
    backblaze_bucket_name = os.environ.get('BACKBLAZE_BUCKET_NAME')

    download_csv_data(
        # dump_path='C:/Users/defrg/binance_data_main/',
        start_date='25-02-2025',
        end_date='25-02-2025',
        backblaze_access_key_id=backblaze_access_key_id,
        backblaze_secret_access_key=backblaze_secret_access_key,
        backblaze_endpoint_url=backblaze_endpoint_url,
        backblaze_bucket_name=backblaze_bucket_name,
        pairs=['BTCUSDT', 'XRPUSDT', 'ETHUSDT'],
        markets=['SPOT'],
        stream_types=['DIFFERENCE_DEPTH_STREAM']
    )
