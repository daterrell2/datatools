from setuptools import setup

setup(name='datatools',
      version='0.1',
      description='Tools for moving data between databases, files, sources',
      url='https://github.com/daterrell2/datatools.git',
      author='David Terrell',
      author_email='daterrell2@gmail.com',
      license='',
      packages=['datatools'],
      install_requires=['SQLAlchemy', ],
      setup_requires=['pytest-runner', ],
      tests_require=['pytest', ],
      zip_safe=False)
