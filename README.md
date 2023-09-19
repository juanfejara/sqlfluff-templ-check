# Force dbt ref plugin

This plugin force dbt ref to be used in sql FROM instead of hardcoded tables or views.

## Intall

To install this plugin, you must have sqlfluff, importlib_resources and regex packages.

Add the rule SD01 to the rules in .sqlfluff, ej:

'''yml
rules = SD01
'''

'''sh
python -m setup install
'''

## Test

'''sh
pytest -s
'''

This interface is supported from version `0.4.0` of
SQLFluff onwards.
