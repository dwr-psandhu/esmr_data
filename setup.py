from setuptools import setup
import versioneer

requirements = [
    # package requirements go here
]

setup(
    name='esmr_data',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Electronic Self Monitoring Data",
    license="MIT",
    author="Nicky Sandhu",
    author_email='psandhu@water.ca.gov',
    url='https://github.com/dwr-psandhu/esmr_data',
    packages=['esmr_data'],
    entry_points={
        'console_scripts': [
            'esmr_data=esmr_data.cli:cli'
        ]
    },
    install_requires=requirements,
    keywords='esmr_data',
    classifiers=[
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ]
)
