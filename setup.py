from setuptools import setup
from os import path

PACKAGENAME = 'filesync'
ENTRY_POINT = "filesync"
DESCRIPTION = "A file sync lib"


this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md')) as f:
    long_description = f.read()


setup(
    name=PACKAGENAME,
    packages=[PACKAGENAME],
    version_config={
        "version_format": "{tag}.dev{sha}",
        "starting_version": "0.0.1"
    },
    setup_requires=['better-setuptools-git-version'],
    description=DESCRIPTION,
    long_description=long_description,
    long_description_content_type='text/markdown',
    license='MIT',
    author='Gregor Roth',
    author_email='gregor.roth@web.de',
    url='https://github.com/grro/filesync',
    entry_points={
        'console_scripts': [
            ENTRY_POINT + '=' + PACKAGENAME + ':main'
        ]
    },
    keywords=[
        'file', 'synchronization', 'webdav'
    ],
    install_requires=[
        'dateparser==1.0.0', 'webdavclient3==3.14.5', 'requests==2.27.1'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha"
    ],
)
