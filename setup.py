import os

from setuptools import setup

# Ask pbr to not generate AUTHORS file if environment variable does not require it
if not os.environ.get('SKIP_GENERATE_AUTHORS'):
    os.environ['SKIP_GENERATE_AUTHORS'] = '1'

# Ask pbr to not generate ChangeLog file if environment variable does not require it
if not os.environ.get('SKIP_WRITE_GIT_CHANGELOG'):
    os.environ['SKIP_WRITE_GIT_CHANGELOG'] = '1'

setup(
    setup_requires=['pbr>=1.9', 'setuptools>=17.1'],
    pbr=True,
)