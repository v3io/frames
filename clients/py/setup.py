# Copyright 2018 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


def version():
    with open('v3io_frames.py') as fp:
        for line in fp:
            if '__version__' in line:
                _, version = line.split('=')
                return version.replace("'", '').strip()


def parse_deps():
    deps = []
    in_deps = False
    with open('Pipfile') as fp:
        for line in fp:
            if not line.strip():
                continue
            elif in_deps and '[' in line:
                return deps
            elif line.strip() == '[packages]':
                in_deps = True
                continue
            elif not in_deps:
                continue
            else:
                dep, version = [str.strip(val) for val in line.split('=', 1)]
                version = version[1:-1]  # Trim ""
                deps.append('{}{}'.format(dep, version))


setup(
    name='v3io_frames',
    version=version(),
    description='Client for v3io streaming data',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Miki Tebeka',
    author_email='miki@353solutions.com',
    license='MIT',
    url='https://github.com/v3io/frames',
    py_modules=['v3io_frames'],
    install_requires=parse_deps(),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: MacOS',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Software Development :: Libraries',
    ],
    tests_require=['pytest', 'flake8'],
)
