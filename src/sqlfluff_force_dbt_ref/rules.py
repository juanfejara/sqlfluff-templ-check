"""Rule that requires dbt ref/source macro in sql FROM.
"""

from dataclasses import dataclass

import re
import sqlfluff
from packaging import version
from sqlfluff.core.parser.segments import BaseSegment
from sqlfluff.core.rules import BaseRule, EvalResultType, LintResult, RuleContext
from sqlfluff.core.rules.crawlers import SegmentSeekerCrawler

if version.parse(sqlfluff.__version__) >= version.parse("2.3.1"):
    from sqlfluff.utils.analysis.query import Query
elif version.parse(sqlfluff.__version__) >= version.parse("2.0.0"):
    from sqlfluff.utils.analysis.select_crawler import Query

from typing import List


@dataclass
class SD01Query(Query):
    """Query subclass with custom SD01 info."""

    pass


class Rule_SD01(BaseRule):
    """Rule force dbt 'ref' or 'source' to avoid hard coding tables or views.

    **Anti-pattern**

    Using hard coded table or view in FROM or JOIN.

    .. code-block:: sql

        SELECT *
        FROM foo
        ORDER BY
            bar,
            baz

    **Best practice**

    Use ref to reference a table or view.

    .. code-block:: sql

        SELECT *
        FROM {{ ref('foo') }}
        ORDER BY bar
    """

    groups = ("all",)
    crawl_behaviour = SegmentSeekerCrawler(
        {"from_clause", "with_compound_statement"}, allow_recurse=False
    )
    _dialects_requiring_alias_for_values_clause = [
        "snowflake",
    ]
    is_fix_compatible = False

    @classmethod
    def _find_raw_at_src_idx(cls, segment: BaseSegment, src_idx: int):
        """Recursively search to find a raw segment for a position in the source.

        NOTE: This assumes it's not being called on a `raw`.

        In the case that there are multiple potential targets, we will find the
        first.
        """
        assert segment.segments
        for seg in segment.segments:
            if not seg.pos_marker:  # pragma: no cover
                continue
            src_slice = seg.pos_marker.source_slice
            # If it's before, skip onward.
            if src_slice.stop <= src_idx:
                continue
            # Is the current segment raw?
            if seg.is_raw():
                return seg
            # Otherwise recurse
            return cls._find_raw_at_src_idx(seg, src_idx)

    def _eval(self, context: RuleContext) -> EvalResultType:
        flag_review_next = False
        result: List[LintResult] = []
        aliases: List[str] = []
        if context.segment.is_type("with_compound_statement"):
            query = SD01Query.from_segment(context.segment, dialect=context.dialect)
            for child in query.children:
                aliases.append(child.cte_name_segment.raw.lower())

        if not context.templated_file:
            return result

        for raw_slice in context.templated_file.raw_sliced:
            # In case of the slice finish with from or join, must search in the next slice
            if re.search(r"from\s*$", raw_slice.raw.lower()) or re.search(
                r"join\s*$", raw_slice.raw.lower()
            ):
                flag_review_next = True
            # In case of the slice have from ro join but don't finish whith them
            elif re.search(r"\s+from\s+", raw_slice.raw.lower()) or re.search(
                r"\s+join\s+", raw_slice.raw.lower()
            ):
                # Exception when use sql function extract
                # If the 'from' is followed by a hardcoded table or view
                if not re.search(r"\s+extract\(\s*.+\s+from\s+", raw_slice.raw.lower()) and not (
                    aliases and any(alias in raw_slice.raw.lower() for alias in aliases)
                ):
                    raw_seg = self._find_raw_at_src_idx(context.segment, raw_slice.source_idx)
                    result.append(
                        LintResult(
                            anchor=raw_seg,
                            description=f"Hard code table or view "
                            f"`{raw_slice.raw.lower()}` not allowed.",
                        )
                    )
            elif flag_review_next:
                # In case of begin with a new query, it begins with "( select"
                if re.search(r"^\s*\(\s*select", raw_slice.raw.lower()):
                    if not (
                        re.search(r"from\s*$", raw_slice.raw.lower())
                        or re.search(r"join\s*$", raw_slice.raw.lower())
                    ):
                        flag_review_next = False
                # In case of a template must have a ref or a source
                elif not (
                    re.search(r"^{{\sref\('.+'\)\s}}$", raw_slice.raw.lower())
                    or re.search(
                        r"^{{\s*source\(\s*'.+'\s*,\s*'.+'\s*\)\s*}}$", raw_slice.raw.lower()
                    )
                ):
                    raw_seg = self._find_raw_at_src_idx(context.segment, raw_slice.source_idx)
                    result.append(
                        LintResult(
                            anchor=raw_seg,
                            description=f"Must use ref or source in tamplate, "
                            f"`{raw_slice.raw}` not allowed.",
                        )
                    )
                else:
                    flag_review_next = False
        return result
