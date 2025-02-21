import glob
import subprocess

from setuptools import find_packages, setup


def get_version():
    return f"1.0+{subprocess.check_output(['git', 'rev-parse', 'HEAD']).strip().decode('utf-8')}"


try:
    with open("requirements.txt") as f:
        install_requires = [x.strip() for x in f.readlines()]
except OSError:
    install_requires = []

setup(
    name="LIMS2DB",
    version=get_version(),
    author="Maya Brandi, Denis Moreno",
    author_email="maya.brandi@scilifelab.se, denis.moreno@scilifelab.se",
    description="Fetching data from Genologics LIMS and pushing it to statusdb",
    install_requires=install_requires,
    packages=find_packages(),
    scripts=glob.glob("scripts/*.py"),
)
