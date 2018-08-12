from setuptools import setup, find_packages
VERSION = '0.0.1'

setup(
    name='livestreaming-tools',
    version=VERSION,
    author='Holden Karau',
    author_email='holden@pigscanfly.ca',
    # Copy the shell script into somewhere likely to be in the users path
    packages=find_packages(),
    install_requires=[
        'google-api-python-client',
        'google-auth',
        'google-auth-oauthlib',
        'google-auth-httplib2',
        'python-twitch-client',
        'pytz',
        'bufferapp',
        'BeautifulSoup4',
    ],
    test_requires=[
        'nose==1.3.7',
        'coverage>3.7.0',
        'unittest2>=1.0.0',
    ],
)
