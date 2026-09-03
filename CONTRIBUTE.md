# Contributing to CrocoLakeTools

Thank you for considering contributing to CrocoLakeTools! Your contributions help us improve and grow this project. To ensure a smooth collaboration, please follow the guidelines outlined in this document.

## Table of Contents

1. [Introduction](#introduction)
2. [How to Contribute](#how-to-contribute)
3. [Submitting Issues](#submitting-issues)
4. [Submitting Pull Requests](#submitting-pull-requests)
5. [Coding Standards](#coding-standards)
7. [Developing Converters](#developing-converters)
8. [Testing](#testing)
9. [Code of Conduct](#code-of-conduct)
10. [Communication](#communication)
11. [Databases of Interest](#databases-of-interest)

## Introduction

CrocoLakeTools is a Python package designed to generate CrocoLake, a database of oceanographic observations developed and maintained under the NSF-sponsored project CROCODILE. We welcome contributions from the community to help enhance and expand the functionality of this package.

## How to Contribute

To contribute to CrocoLakeTools, please follow these steps:

1. **Fork the Repository**: Fork the [CrocoLakeTools repository](https://github.com/boom-lab/crocolaketools-public.git) to your own GitHub account.
2. **Create a Branch**: Create a new branch for your feature. The recommended workflow is the [git workflow](https://nvie.com/posts/a-successful-git-branching-model/), so you'll probably want to branch off of `develop`.
3. **Make Changes**: Make your changes in the new branch.
4. **Commit Changes**: Commit your changes with clear and descriptive commit messages.
5. **Push to Fork**: Push your changes to your forked repository.
6. **Open a Pull Request**: Open a pull request (PR) to merge your changes into the `develop` branch of the original repository.

## Submitting Issues

If you encounter any issues or have feature requests, please submit them through the [GitHub Issues page](https://github.com/boom-lab/crocolaketools-public/issues). Provide as much detail as possible to help us understand and address the issue.

## Submitting Pull Requests

When submitting a pull request, please ensure the following:

- Your PR is submitted from a forked repository.
- Your PR targets the `develop` branch.
- Your PR has been assigned for review to a member of the CrocoLake team (currently @enrico-mi).

## Coding Standards

Please adhere to the following coding standards:

- Follow the PEP 8 style guide for Python code.
- Write clear and concise commit messages.
- Comment your code where necessary to explain complex logic.

## Developing Converters

When developing new converter modules, please follow these guidelines:

- **Module Placement**: Each new converter should be placed in the `converter` modules folder.
- **Usage Scripts**: Each new converter should be accompanied by a script in the `scripts` folder demonstrating how to use it.
- **Starting Point**: To develop a new converter module, refer to the existing converters in the `converter` folder as a starting point (e.g., `converterGLODAP` for .csv files, `converterSprayGlider` for netCDF files).
- **Shared Utilities**: Shared utilities like dictionaries used to map and rename variables should be added to `crocolaketools/db_names.py` and `crocolaketools/db_params.py`.
- **Discussion**: Before developing a new converter, it is recommended to discuss its necessity and implementation with a member of the CrocoLake team.

## Testing

CrocoLakeTools deals with converting files from one format to Parquet. Each new format should be testable on a small sample file. Ensure that your code includes tests for any new functionality or bug fixes. See the `tests` folder for examples of testing modules.

Set up the environment as described in the README's [Installation](README.md#installation) section, then run `pytest tests/test_golden.py`. Tests use the small fixtures committed under `tests/fixtures/`, so no data download is needed. If a change alters converter logic, and thus converter output, `tests/test_golden.py` will fail: regenerate with `pytest tests/test_golden.py --update-golden` and explain the resulting JSON diff in your pull request.

## Communication

For any questions or discussions, please use the [GitHub Discussions page](https://github.com/boom-lab/crocolaketools/discussions) or the issue tracker.

## Databases of Interest

(work in progress: this section will be populated with a list of databases and links.)

Thank you for contributing to CrocoLakeTools!
