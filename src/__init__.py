'''
This __init__.py file is essential for making this directory (`/src/`) a Python package.

By adding this file, Python will recognize `/src/` as a package, allowing its modules and sub-packages
to be imported into other Python scripts or Jupyter notebooks within this project. 

For example, with this file in place, you can import functions from `utils.py` in a script under the `/eda/`
sub-directory like this:

    from src.utils import some_function

Or, if you just want to import the `utils` module itself:

    from src import utils

This setup allows for a more modular and organized codebase, which is particularly useful as the project grows.
It ensures that all utility functions, classes, and variables defined in `utils.py` are accessible from any part
of the project, enhancing reusability and maintainability.
'''