"""An example of a custom rule implemented through the plugin system.

This uses the rules API supported from 0.4.0 onwards.
"""

from sqlfluff.core.plugin import hookimpl
from sqlfluff.core.rules import BaseRule
from typing import List, Type


@hookimpl
def get_rules() -> List[Type[BaseRule]]:
    """Get plugin rules."""
    from sqlfluff_force_dbt_ref.rules import Rule_SD01

    return [Rule_SD01]


@hookimpl
def get_configs_info() -> dict:
    """Get rule config validations and descriptions."""
    return {}
