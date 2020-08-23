from setuptools import setup, find_packages

setup(
    name="frame-tasks",
    packages=find_packages(exclude=("test",)),
    author="Sankho Turjo Sarkar",
    install_requires=[
        "pandas>1.0",
        "click",
        "mypy",
        "simpleai",
        "flask",
        "palettable",
        "celery",
        "murmurhash3",
    ],
    version="0.1.0",
)
