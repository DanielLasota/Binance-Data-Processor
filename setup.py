from setuptools import setup, find_packages
from binance_data_processor import __version__

setup(
    name='binance-data-processor',
    version=__version__,
    packages=find_packages(),
    install_requires=[
        'python-dotenv',
        'fastapi',
        'uvicorn',
        'websockets',
        'orjson',
        'requests',
        'numpy',
        'pandas',
        'alive-progress'
    ],
    extras_require={
        'dev': [
            'pytest',
            'objgraph',
            'pympler'
        ],
        'azure': [
            'azure-identity',
            'azure-storage-blob'
        ]
    },
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Daniel Lasota",
    author_email="grossmann.root@gmail.com",
    description="A package for archiving Binance data",
    keywords="binance data processor listener archiver quant data sprzedam opla",
    url="https://www.youtube.com/watch?v=dQw4w9WgXcQ",
    python_requires='>=3.11',
)

