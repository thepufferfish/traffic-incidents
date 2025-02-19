from setuptools import find_packages, setup

setup(
    name="traffic_fatalities",
    packages=find_packages(exclude=["traffic_fatalities_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "matplotlib",
        "osmnx",
        "networkx",
        'thefuzz'
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
