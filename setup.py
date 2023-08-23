import setuptools
from version import VERSION


with open("README.md", "r") as fh:
    long_description = fh.read()

DESCRIPTION = """
1. API Testing Package
Repository Name: pepper-fusion-api
Description: A comprehensive package for API testing, including tools for automation, validation, and integration.
2. Data Compare Testing Package
Repository Name: pepper-fusion-data-compare
Description: Tools and libraries for comparing data across various sources, with customizable rules and reporting features.
3. Machine Learning & Data Science Testing Package
Repository Name: pepper-fusion-ml-ds
Description: A robust package for validating and testing machine learning models, data preprocessing, and experimentation.
4. Predictive Analytics Package
Repository Name: pepper-fusion-predictive
Description: Advanced tools for predictive analytics, including forecasting, visualization, and integration with BI tools.
"""[1:-1]

CLASSIFIERS = """
Programming Language :: Python :: 3
License :: OSI Approved :: MIT License
Operating System :: OS Independent
Topic :: Software Development :: Testing
Development Status :: 5 - Production/Stable
"""[1:-1]

setuptools.setup(
    name="pepper_fusion", # Updated to match your project name
    version=VERSION,
    author="Ranjith Ashok", # Update with your name or team's name
    author_email="ranjithashok2@gmail.com", # Update with your email
    description="Pepper Fusion - A Comprehensive Testing Framework", # Updated description
    long_description=DESCRIPTION,
    long_description_content_type="text/markdown",
    url='https://github.com/ranjithashokgit/pepper-fusion/', # Updated URL to match your GitHub repository
    keywords='API Testing, Data Compare Testing, Machine Learning, Data Science Testing, Predictive Analytics', # Updated keywords
    license='MIT',
    packages=setuptools.find_packages(),
    platforms='any',
    classifiers=CLASSIFIERS.splitlines(),
    python_requires='>=3.6',
    install_requires=[
        'pandas',
        'numpy',
        'matplotlib',
        'xlsxwriter',
        'openpyxl',
        'sqlalchemy',
        'cx-Oracle',
        'requests',
        'robotframework',
        'robotframework-requests',
        'robotframework-requestspro',
        'robotframework-seleniumlibrary',
        'jinja2',
        'pyyaml',
        'lxml',
        'ipython',
        'jupyterlab',
        'notebook',
        'natsort',
        'jupyterlab_robotmode',
        'plotly',
        'fsspec',
        'twine',
        'wheel',
    ],
)
