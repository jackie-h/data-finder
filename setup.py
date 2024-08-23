from setuptools import setup

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
   name='datafinder',
   version='0.1',
   description='Generated typed finders',
   author='Jackie Haynes',
   packages=['datafinder'],  #same as name
   install_requires=requirements, #external packages as dependencies
)