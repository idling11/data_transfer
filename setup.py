import os
import platform
import subprocess

import setuptools

# Package metadata.

name = "data_transfer"
description = "Move data and provide data comparison through object storage"

# Should be one of:
# 'Development Status :: 3 - Alpha'
# 'Development Status :: 4 - Beta'
# 'Development Status :: 5 - Production/Stable'
release_status = "Development Status :: 3 - Alpha"
dependencies = [
    'schedule >= 1.1.0',
    'clickzetta-connector >= 0.8.60',
    'docopt >= 0.6.2',
    'pyyaml >= 6.0',
    'openpyxl >= 3.1.2',
    'tabulate >= 0.8.9',
    'pymysql >= 1.0.2',
    'sqlparse >= 0.4.1',
    'pymysql-pool >= 0.4.3',
    'psycopg2 >= 2.9.3',
    "cz-data-diff >= 0.0.2",
    "cz-data-diff[mysql,clickzetta,postgresql] >= 0.0.2",
    "pyodps >= 0.11.4.post0",
]
extras = {

}

all_extras = []

for extra in extras:
    all_extras.extend(extras[extra])

extras["all"] = all_extras

# Setup boilerplate below this line.

package_root = os.path.abspath(os.path.dirname(__file__))

version = {}
with open(os.path.join(package_root, "migration/version.py")) as fp:
    exec(fp.read(), version)
version = version["__version__"]

packages = setuptools.find_packages(exclude=["test", "test.*", "scripts", "scripts.*"])

setuptools.setup(
    name=name,
    version=version,
    description=description,
    url='https://www.yunqi.tech/',
    author="mocun",
    author_email="hanmiao.li@clickzetta.com",
    platforms="Posix; MacOS X;",
    packages=packages,
    install_requires=dependencies,
    extras_require=extras,
    python_requires=">=3.6",
    entry_points={
        'console_scripts': ['cz-migration=migration.migration:main'],
    },
    include_package_data=True,
    zip_safe=False,
)
