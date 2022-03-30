from setuptools import setup
from setuptools import find_packages

setup(
    name="noiz",
    packages=find_packages('src'),
    package_dir={'': 'src'},
    include_package_data=True,
    author="Damian Kula",
    author_email="dkula@unistra.fr",
    version="0.5.20220330",
    entry_points={"console_scripts": ["noiz=noiz.cli:cli"]},
    install_requires=[
        "flask",
        "utm",
        "obspy",
        "flask_migrate",
        "flask-sqlalchemy",
        "environs",
    ],
)
