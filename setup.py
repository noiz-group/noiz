from setuptools import setup

setup(
    name='noiz',
    packages=['noiz'],
    include_package_data=True,
    # entry_points={
    #     'flask.commands': [
    #         'noiz=noiz.cli:myowncustomshit',
    #     ],
    # },
    install_requires=[
        'flask',
    ],

)