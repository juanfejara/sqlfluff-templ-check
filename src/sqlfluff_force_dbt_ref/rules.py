"""Rule that requires dbt ref/source macro in sql FROM.
"""

from dataclasses import dataclass, field

from sqlfluff.core.parser.segments import BaseSegment
from sqlfluff.core.rules import BaseRule, EvalResultType, LintResult, RuleContext
from sqlfluff.core.rules.crawlers import SegmentSeekerCrawler
from sqlfluff.utils.analysis.query import Query
from sqlfluff.utils.functional import FunctionalContext, sp
from typing import List


@dataclass
class SD01Query(Query):
    """Query subclass with custom SD01 info."""

    aliases: List[str] = field(default_factory=list)


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
        {"select_statement", "with_compound_statement"}, allow_recurse=False
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

    def _fill_aliases(self, context: RuleContext, query: SD01Query):
        for child in query.children:
            query.aliases.append(child.cte_name_segment.raw.lower())

    def _eval_select(
        self, context: RuleContext, query: SD01Query, from_clause, result: List[LintResult] = []
    ) -> EvalResultType:
        table_expression = (
            from_clause.children(sp.is_type("from_expression"))
            .children(sp.is_type("from_expression_element"))
            .children(sp.is_type("table_expression"))
        )
        identifier = table_expression.children(sp.is_type("table_reference")).children(
            sp.is_type("identifier")
        )
        bracketed = table_expression.children(sp.is_type("bracketed"))
        if (
            bool(identifier)
            and not identifier.get().is_templated
            and identifier.get().raw not in query.aliases
        ):
            idx = identifier.raw_segments[0].get_start_point_marker().source_slice.start
            raw_seg = self._find_raw_at_src_idx(context.segment, idx)
            result.append(
                LintResult(
                    anchor=raw_seg,
                    description=f"Hard code table or view " f"`{raw_seg.raw}` not allowed.",
                )
            )
        # Recursive iteration over anidated queries
        elif bool(bracketed):
            from_clause_bracketed = (
                bracketed.select()
                .children(sp.is_type("select_statement"))
                .children(sp.is_type("from_clause"))
            )
            self._eval_select(context, query, from_clause_bracketed, result)
        join = (
            from_clause.children(sp.is_type("from_expression"))
            .children(sp.is_type("join_clause"))
            .children(sp.is_type("from_expression_element"))
            .children(sp.is_type("table_expression"))
            .children(sp.is_type("table_reference"))
            .children(sp.is_type("identifier"))
        )
        if bool(join) and not join.get().is_templated and join.get().raw not in query.aliases:
            idx = join.raw_segments[0].get_start_point_marker().source_slice.start
            raw_seg = self._find_raw_at_src_idx(context.segment, idx)
            result.append(
                LintResult(
                    anchor=raw_seg,
                    description=f"Hard code join " f"`{raw_seg.raw}` not allowed.",
                )
            )
        return result

    def _eval(self, context: RuleContext) -> EvalResultType:
        result: List[LintResult] = []
        query = SD01Query.from_segment(context.segment, dialect=context.dialect)
        if context.segment.is_type("with_compound_statement"):
            self._fill_aliases(context, query)
            children = FunctionalContext(context).segment.children(sp.is_type("select_statement"))
            from_clause = children.children(sp.is_type("from_clause")).first()
        elif context.segment.is_type("select_statement"):
            children = FunctionalContext(context).segment.children()
            from_clause = children.select(sp.is_type("from_clause")).first()
        self._eval_select(context, query, from_clause, result)
        return result
