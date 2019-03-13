from setuptools import setup, find_packages

setup(
    version='0.1.0',
    name='datapunt_parkeerrechten',
    description='Backup scripts for NPR Parkeerrechten data',
    url='https://github.com/Amsterdam/parkeerrechten',
    author='Amsterdam Datapunt',
    author_email='datapunt@amsterdam.nl',
    license='Mozilla Public License Version 2.0',
    classifiers=[
        'License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
    ],

    package_dir={'': 'src'},
    packages=find_packages('src'),

    entry_points={
        'console_scripts': [
            'run_import = parkeerrechten.run_import:main',
            'dump_database = parkeerrechten.dump_database:main',
            'restore_database = parkeerrechten.restore_database:main'
        ],
    },
    install_requires=[
        'SQLAlchemy',
        'cython',
        'pymssql==2.1.1',  # requires FreeTDS (non-python dependency)
        'psycopg2-binary',

        'python_swiftclient',
        'python-keystoneclient',
    ],
    extras_require={
        'test': [
            'pytest',
            'pytest-flake8',
        ],
    }

)
