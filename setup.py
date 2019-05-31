from setuptools import setup

setup(
    name='noiz',
    packages=['noiz'],
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'noiz=noiz.cli:cli',
        ],
    },
    install_requires=[
        'flask',
    ],

)