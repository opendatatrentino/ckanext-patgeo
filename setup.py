from setuptools import setup, find_packages

version = '0.1'

setup(
    name='ckanext-patgeo',
    version=version,
    description="GEO PAT",
    long_description="",
    classifiers=[],
    keywords='',
    author='dev@spaziodati.eu',
    author_email='dev@spaziodati.eu',
    url='',
    license='WTFPL',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['ckanext', 'ckanext.patgeo'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        # -*- Extra requirements: -*-
    ],
    entry_points="""
    [ckan.plugins]
    patgeo_harvester=ckanext.patgeo.harvesters:PatGeoHarvester
    """,
)
