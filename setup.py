from setuptools import setup, find_packages

setup(
    name='binance_archiver',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'aiofiles',
        'websockets',
        'azure-storage-blob',
        'python-dotenv',
        'aiohttp',
        'requests',
        'websocket',
        'python-dotenv',
        'websockets',
        'requests',
        'websocket',
        'websocket-client'
    ],
    author="Daniel Lasota",
    author_email="grossmann.root@gmail.com",
    description="A package for archiving Binance data",
    keywords="binance archiver quant data ",
    url="http://youtube.com"
)