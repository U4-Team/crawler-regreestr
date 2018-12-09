import os
from setuptools import setup, find_packages


setup(
    name='crawler_regreestr',
    version='0.0.1',
    description='Crawler RegReestr',
    entry_points={
        'console_scripts': [
            'crawler_regreestr=crawler_regreestr.cli:cli',
        ],
    },
    long_description=open(os.path.join(os.path.dirname(__file__), 'README.md')).read(),
    packages=find_packages('src'),
    package_dir={'': 'src'},
    extras_require={
        'test': [
            'coverage==4.5.2',
            'pycodestyle==2.4.0',
            'pylint==2.2.2',
            'pytest==4.0.1',
            'pytest-cov==2.6.0',
            'pytest-mock==1.10.0',
            'pylint-quotes==0.1.9',
            'pytest-sugar==0.9.2',
            'diff-cover==1.0.5',
        ],
    },
)
