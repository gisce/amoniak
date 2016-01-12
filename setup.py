from setuptools import setup

INSTALL_REQUIRES = [
    'empowering',
    'erppeek',
    'pymongo<3',
    'rq',
    'modeldict',
    'times',
    'raven',
    'click'
]

setup(
    name='amoniak',
    version='0.9.2',
    packages=['amoniak', 'amoniak.caching'],
    url='http://gisce.net',
    license='MIT',
    install_requires=INSTALL_REQUIRES,
    entry_points="""
        [console_scripts]
        amoniak=amoniak.runner:amoniak
    """,
    author='GISCE-TI, S.L.',
    author_email='ti@gisce.net',
    description='AMON Tools for GISCE-ERP'
)
