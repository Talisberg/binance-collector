"""Setup configuration for binance-collector"""
from setuptools import setup, find_packages
from pathlib import Path

# Read README
readme_file = Path(__file__).parent / 'README.md'
long_description = readme_file.read_text() if readme_file.exists() else ''

setup(
    name='binance-collector',
    version='0.1.0',
    description='Fast, schema-validated data ingestion for Binance cryptocurrency markets',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Open Source Contributors',
    url='https://github.com/yourusername/binance-collector',
    packages=find_packages(),
    python_requires='>=3.8',
    install_requires=[
        'pandas>=2.0.0',
        'pyarrow>=14.0.0',
        'requests>=2.31.0',
        'pydantic>=2.0.0',
        'python-binance>=1.0.19',
        'pyyaml>=6.0',
    ],
    extras_require={
        'dev': [
            'pytest>=7.0.0',
            'pytest-cov>=4.0.0',
            'black>=23.0.0',
            'flake8>=6.0.0',
        ]
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Financial and Insurance Industry',
        'Topic :: Office/Business :: Financial :: Investment',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
    keywords='binance cryptocurrency trading data market-data orderbook trades',
)
