from setuptools import setup, find_packages

with open("README.md", 'r') as f:
    long_description = f.read()

def parse_requirements(filename):
    with open(filename, 'r') as f:
        return [line.strip() for line in f if line.strip() and not line.startswith('#')]

setup(
    name='crocolaketools',
    version='1.0.1',
    description='Package to generate and load CrocoLake',
    license="GNU GPLv3",
    long_description=long_description,
    author='Enrico Milanese',
    author_email='enrico.milanese@whoi.edu',
    packages=find_packages(),
    install_requires=parse_requirements('requirements.txt'),
    entry_points={
        'console_scripts': [
            'crocolaketools = scripts.main:main',
            'argo2argoqc_phy = scripts.argo2argoqc_phy:main',
            'argo2argoqc_bgc = scripts.argo2argoqc_bgc:main',
            'glodap2parquet = scripts.glodap2parquet:main',
            'spray2parquet = scripts.spray2parquet:main',
            'argogdac2parquet = scripts.argogdac2parquet:argogdac2parquet',
            'merge_crocolake = scripts.merge_crocolake:main',
            'download_demo_data = scripts.download_demo_data:main',
        ],
    },
    include_package_data=True,
    package_data={
        "crocolaketools": [
            "config/config.yaml",
            "config/config_cluster.yaml",
            "config/generate_crocolake_symlinks.sh",
        ]
    }
)
