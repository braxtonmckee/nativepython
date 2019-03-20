#   Copyright 2018 Braxton Mckee
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import setuptools
import pkg_resources
from distutils.command.build_ext import build_ext
from distutils.extension import Extension


class NumpyBuildExtension(build_ext):
    """
    Used for when numpy headers are needed during build.
    Ensures that numpy will be installed before attempting
    to include any of its libraries
    """

    def run(self):
        self.include_dirs.append(
            pkg_resources.resource_filename('numpy', 'core/include'))
        build_ext.run(self)


extra_compile_args = [
    '-O2',
    '-fstack-protector-strong',
    '-Wformat',
    '-Wdate-time',
    '-Werror=format-security',
    '-std=c++14',
    '-Wno-sign-compare',
    '-Wno-narrowing',
    '-Wno-unused-'
]

ext_modules = [
    Extension(
        'object_database._types',
        sources=[
            'object_database/all.cpp',
        ],
        define_macros=[
            ("_FORTIFY_SOURCE", 2)
        ],
        extra_compile_args=extra_compile_args
    ),
    Extension(
        'typed_python._types',
        sources=[
            'typed_python/all.cpp',
        ],
        define_macros=[
            ("_FORTIFY_SOURCE", 2)
        ],
        extra_compile_args=extra_compile_args
    )
]

INSTALL_REQUIRES = [line.strip() for line in open('requirements.txt')]

setuptools.setup(
    name='nativepython',
    version='0.0.1',
    description='Tools for generating machine code using python.',
    author='Braxton Mckee',
    author_email='braxton.mckee@gmail.com',
    url='https://github.com/braxtonmckee/nativepython',
    packages=setuptools.find_packages(),
    cmdclass={'build_ext': NumpyBuildExtension},
    ext_modules=ext_modules,
    setup_requires=[
        'numpy'
    ],
    install_requires=INSTALL_REQUIRES,
    classifiers=[
        "Programming Language :: Python :: 3"
    ],
    entry_points={
        'console_scripts': [
            'object_database_webtest=object_database.frontends.object_database_webtest:main',
            'object_database_service_manager=object_database.frontends.service_manager:main',
        ]
    },
    include_package_data=True,
    zip_safe=False
)
