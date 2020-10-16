from setuptools import setup

setup(
    name="noiz",
    packages=["src/noiz"],
    include_package_data=True,
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
