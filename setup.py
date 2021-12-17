from setuptools import setup
from pathlib import Path

# read the contents of your README file
here = Path(__file__).parent
long_description = (here / 'README.md').read_text().strip()
requirements = (here / 'requirements.txt').read_text().strip().split('\n')

setup(
    name='patents',
    version='0.9',
    description='USPTO patent parser',
    url='http://github.com/iamlemec/patents',
    author='Doug Hanley',
    author_email='thesecretaryofwar@gmail.com',
    license='MIT',
    packages=['patents'],
    zip_safe=False,
    install_requires=requirements,
    long_description=long_description,
    long_description_content_type='text/markdown',
    package_data={
        'patents': ['meta/*_files.txt'],
    },
)
