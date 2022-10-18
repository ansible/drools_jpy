# Drools JPY Package

This python package allows you to directly call Drools classes (implemented in Java) using JPY from your Python code.
Needs
   * Java 11+
   * Maven 3.8.1+
   * Environment variable **JAVA_HOME** should be set appropriately

# Setup and Testing

```
  python3 -m venv venv
  source venv/bin/activate
  python3 -m pip install --upgrade pip
  python3 -m pip install --upgrade build
  python3 -m pip install -e '.[dev]'
  tox
  python3 -m build
```

# Check code coverage

```
   coverage run -m pytest
   coverage html
   open htmlcov/index.html
```

# Format and lint the code before creating a PR

```
   black .
   flake8 .
   isort .		
```

# To publish the package to PyPi

```
python3 -m pip install --upgrade build
rm -rf dist
python3 -m build
python3 -m pip install --upgrade twine
python3 -m twine upload dist/*

```
