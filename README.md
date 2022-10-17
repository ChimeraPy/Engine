![logo](docs/_static/chimerapy_logo_with_name.png)

# ChimeraPy: A Distributed Computing Framework for Multimodal Data Collection

[![Coverage Status](https://coveralls.io/repos/github/oele-isis-vanderbilt/ChimeraPy/badge.svg?branch=main)](https://coveralls.io/github/oele-isis-vanderbilt/ChimeraPy?branch=main) ![](https://img.shields.io/github/workflow/status/oele-isis-vanderbilt/ChimeraPy/Test) ![](https://img.shields.io/github/license/oele-isis-vanderbilt/ChimeraPy) ![](https://img.shields.io/badge/style-black-black)

[Documentation](https://oele-isis-vanderbilt.github.io/ChimeraPy) | [PyPI](https://pypi.org/project/chimerapy/)

We propose the development of ChimeraPy, a distributed computing framework for multimodal data collection. We focus on four key features that are important for multimodal learning analytics purposes: free and open-source code; distributed computing capabilities; the use of python as a programming language; and time-alignment capabilities.

## Installation

You can install the package through PIPY with the following command.

```
pip install chimerapy
```

Additionally, you can install the package through GitHub instead.

```
git clone https://github.com/oele-isis-vanderbilt/ChimeraPy
cd ChimeraPy
pip install .
```

## Development

To create a development environment, use instead the following command:

```
pip install '.[test]'
```

With this command, the necessary dependencies for testing will be automatically
installed.

To execute the test, use the following command (located within the root
GitHub repository folder).

```
pytest
```

In this project, we use the `pre-commit` library to verify every commit
follows certain standards. This dependency is automatically included in
the test dependency installation. [Here](https://pre-commit.com) is
information in how its used and advantages.

## License

ChimeraPy uses the GNU GENERAL PUBLIC LICENSE, as found in [LICENSE](https://oele-isis-vanderbilt/ChimeraPy/blob/main/LICENSE) file.
