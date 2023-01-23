Building the Docs
#################

Make sure that you have all the necessary dependencies by installing the documentation dependencies, use the following command::

    pip install '.[docs]'

The documentation was build with Sphinx with the following command (as executed in the GitHub project root folder)::

    sphinx-build -b html docs/ build/
