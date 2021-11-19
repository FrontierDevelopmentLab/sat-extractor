#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import io
import shlex
from glob import glob
from os.path import basename
from os.path import dirname
from os.path import join
from os.path import splitext
from subprocess import check_call

from setuptools import find_packages
from setuptools import setup
from setuptools.command.develop import develop


class PostDevelopCommand(develop):
    def run(self):
        try:
            check_call(shlex.split("pre-commit install"))
        except Exception as e:
            print(f"Unable to run 'pre-commit install' with exception {e}")
        develop.run(self)


def read(*names, **kwargs):
    with io.open(
        join(dirname(__file__), *names),
        encoding=kwargs.get("encoding", "utf8"),
    ) as fh:
        return fh.read()


setup(
    name="satextractor",
    version="0.2.0",
    license="BSD-2-Clause",
    description="SatExtractor. Extract everything from everywhere.",
    url="https://github.com/FrontierDevelopmentLab/sat-extractor",
    packages=find_packages("src"),
    package_dir={"": "src"},
    py_modules=[splitext(basename(path))[0] for path in glob("src/*.py")],
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        # complete classifier list: http://pypi.python.org/pypi?%3Aaction=list_classifiers
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: Unix",
        "Operating System :: POSIX",
        "Operating System :: Microsoft :: Windows",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: Implementation :: CPython",
        # uncomment if you test on these interpreters:
        # 'Programming Language :: Python :: Implementation :: IronPython',
        # 'Programming Language :: Python :: Implementation :: Jython',
        # 'Programming Language :: Python :: Implementation :: Stackless',
        "Topic :: Utilities",
    ],
    project_urls={
        "Documentation": "https://sat-extractor.readthedocs.io/",
        "Changelog": "https://sat-extractor.readthedocs.io/en/latest/changelog.html",
        "Issue Tracker": "https://github.com/FrontierDevelopmentLab/sat-extractor/issues",
    },
    keywords=[
        # eg: 'keyword1', 'keyword2', 'keyword3',
    ],
    python_requires=">=3.8",
    install_requires=[
        "loguru~=0.5.3",
        "joblib~=1.1.0",
        "numpy~=1.21.4",
        "pandas~=1.3.4",
        "shapely~=1.8.0",
        "gunicorn~=20.1.0",
        "pyproj~=3.2.1",
        "geopandas~=0.10.2",
        "rasterio~=1.2.10",
        "zarr~=2.10.2",
        "tqdm~=4.62.3",
        "google-api-python-client~=2.29.0",
        "google-cloud-storage~=1.42.3",
        "google-cloud-functions~=1.3.1",
        "google-cloud-pubsub~=2.8.0",
        "google-cloud-bigquery~=2.30.1",
        "hydra-core~=1.1.1",
        "gcsfs~=2021.11.0",
        "pystac[validation]~=1.1.0",
        "sentinelhub~=3.4.1",
        "pyarrow~=6.0.0",
    ],
    extras_require={
        "dash": [
            "dash~=2.0.0",
            "dash-auth~=1.4.1",
            "dash-bootstrap-components~=1.0.0",
        ],
        "pre-commit": [
            "pre-commit",
        ],
        "test": [
            "pytest",
            "tox",
        ],
    },
    entry_points={
        "console_scripts": [
            "satextractor = satextractor.cli:main",
        ],
    },
    cmdclass={"develop": PostDevelopCommand},
)
