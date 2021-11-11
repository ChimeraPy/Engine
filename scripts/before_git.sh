# NOTE: Run this script in the root git repo!

# Update the PlantUML diagrams to pngs
echo $'\nUPDATING PLANTUML DIAGRAMS'
python scripts/convert_puml_to_png.py

# Running tests
echo $'\nRUNNING TESTS'
python -m unittest discover tests/

# Checking type hinting with mypy
echo $'\nMYPY'
mypy pymmdt/

# Checking if the documentation is valid
echo $'\nPYDOCSTYLE'
pydocstyle pymmdt/

# Update the API docs
echo $'\nLAZYDOCS GENERATION'
rm docs/api-docs/*.md
lazydocs --output-path="./docs/api-docs" --src-base-url="https://github.com/edavalosanaya/PyMMDT/blob/main/" pymmdt/

# You can also use twine instead
# https://twine.readthedocs.io/en/latest/contributing.html
