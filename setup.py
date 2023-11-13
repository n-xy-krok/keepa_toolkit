from setuptools import setup, find_packages

with open("requirements.txt") as f:
	install_requires = f.read().strip().split("\n")

# get version from __version__ variable in keepa_toolkit_v2/__init__.py
from keepa_toolkit_v2 import __version__ as version

setup(
	name="keepa_toolkit_v2",
	version=version,
	description="Keepa Toolkit Frappe App Reworked",
	author="N_XY",
	author_email="kubliy.n@ikrok.net",
	packages=find_packages(),
	zip_safe=False,
	include_package_data=True,
	install_requires=install_requires
)
