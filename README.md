# Force dbt ref plugin

This plugin requires the dbt `ref` or `source` macros to be used in sql `FROM` instead of hardcoded tables or views.

## Install

To install this plugin, you must have sqlfluff packages.

Add the rule SD01 to the rules in .sqlfluff, for example:

'''yml
rules = SD01
'''

After add the rule, you must install the plugin into your environtment running the setup.py:

'''sh
python -m setup install
'''

This will copy the egg into the path of the site-package used by python:
sqlfluff_plugin_force_dbt_ref-1.0.0-py3.7.egg

## Test

'''sh
pytest -s
'''
