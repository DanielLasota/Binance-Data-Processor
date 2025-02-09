from setuptools import setup, find_packages

setup(
    name='binance-archiver',
    version='0.0.2',
    packages=find_packages(),
    install_requires=[
        'azure-identity',
        'azure-keyvault',
        'azure-storage-blob',
        'python-dotenv',
        'boto3==1.35.2',
        'fastapi',
        'uvicorn',
        'websockets',
        'orjson',
        'alive-progress',
        'pytest',
        'objgraph',
        'pympler',
        'requests',
        'numpy',
        'pandas'
    ],
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Daniel Lasota",
    author_email="grossmann.root@gmail.com",
    description="A package for archiving Binance data",
    keywords="binance archiver quant data sprzedam opla",
    url="http://youtube.com",
    python_requires='>=3.11',
)
