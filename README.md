# Force dbt ref plugin

This plugin requires the dbt `ref` or `source` macros to be used in sql `FROM` instead of hardcoded tables or views.

## Install

To install this plugin, you must have sqlfluff packages.

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
