import os
import subprocess


try:
    try:
        VERSION = subprocess.check_output([
            'git', 'describe', '--tags'
        ]).strip()
    except subprocess.CalledProcessError as e:
        VERSION = __import__('pkg_resources') \
            .get_distribution(__name__).version
except Exception as e:
    VERSION = 'unknown'

from .amon import *
