"""Rule that requires dbt ref/source macro in sql FROM.
"""

from dataclasses import dataclass, field

from sqlfluff.core.parser.segments import BaseSegment
from sqlfluff.core.rules import BaseRule, EvalResultType, LintResult, RuleContext
from sqlfluff.core.rules.crawlers import SegmentSeekerCrawler
from sqlfluff.dialects.dialect_ansi import BracketedSegment, IdentifierSegment
from sqlfluff.utils.analysis.query import Query
from sqlfluff.utils.functional import FunctionalContext, Segments, sp
from typing import List, cast


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
        """Find aliases of with cluase"""
        for child in query.children:
            query.aliases.append(child.cte_name_segment.raw.lower())

    def _get_identifier(self, table_expression):
        """Get table identifier inside a table expression"""
        identifier_segments = (
            Segments(table_expression)
            .children(sp.is_type("table_reference"))
            .children(sp.is_type("identifier"))
        )
        if bool(identifier_segments) and identifier_segments.get().is_type("identifier"):
            return identifier_segments
        else:
            return cast(IdentifierSegment, identifier_segments.get())

    def _get_bracketed(self, table_expression):
        """Get the query nested inside brackets on a table expression"""
        bracketed_segments = Segments(table_expression).children(sp.is_type("bracketed"))
        if bool(bracketed_segments) and bracketed_segments.get().is_type("bracketed"):
            return bracketed_segments
        else:
            return cast(BracketedSegment, bracketed_segments.get())

    def _eval_from_clause(
        self, context: RuleContext, query: SD01Query, from_clause, result: List[LintResult]
    ) -> EvalResultType:
        """Evaluate the tables, views or nested queries inside a from clause if exists
        not allowed references"""
        table_expressions = (
            from_clause.children(sp.is_type("from_expression"))
            .children(sp.is_type("from_expression_element"))
            .children(sp.is_type("table_expression"))
        )
        # Iterate over the for tables
        for table_expression in table_expressions:
            identifier = self._get_identifier(table_expression)
            bracketed = self._get_bracketed(table_expression)
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
            # Recursive iteration over nested queries
            elif bool(bracketed):
                from_clause_bracketed = (
                    bracketed.select()
                    .children(sp.is_type("select_statement"))
                    .children(sp.is_type("from_clause"))
                )
                self._eval_clauses(context, query, from_clause_bracketed, result)
        return result

    def _eval_join_clauses(
        self, context: RuleContext, query: SD01Query, from_clause, result: List[LintResult] = []
    ) -> EvalResultType:
        """Evaluate the tables, views or nested queries inside a join clause if exists
        not allowed references"""
        # Extract joins in the select
        joins = (
            from_clause.children(sp.is_type("from_expression"))
            .children(sp.is_type("join_clause"))
            .children(sp.is_type("from_expression_element"))
            .children(sp.is_type("table_expression"))
            .children(sp.is_type("table_reference"))
            .children(sp.is_type("identifier"))
        )
        # Iterate over the join clauses
        for join in joins:
            if bool(join) and not join.is_templated and join.raw not in query.aliases:
                idx = join.raw_segments[0].get_start_point_marker().source_slice.start
                raw_seg = self._find_raw_at_src_idx(context.segment, idx)
                result.append(
                    LintResult(
                        anchor=raw_seg,
                        description=f"Hard code join " f"`{raw_seg.raw}` not allowed.",
                    )
                )
        return result

    def _eval_clauses(
        self, context: RuleContext, query: SD01Query, from_clause, result: List[LintResult] = []
    ) -> EvalResultType:
        """Recursively evaluation to find not allowed tables or views reference."""
        self._eval_from_clause(context, query, from_clause, result)
        self._eval_join_clauses(context, query, from_clause, result)
        return result

    def _eval(self, context: RuleContext) -> EvalResultType:
        """Evaluate to find not allowed tables or views reference, only allow templated
        references."""
        result: List[LintResult] = []
        query = SD01Query.from_segment(context.segment, dialect=context.dialect)
        if context.segment.is_type("with_compound_statement"):
            self._fill_aliases(context, query)
            children = FunctionalContext(context).segment.children(sp.is_type("select_statement"))
            from_clause = children.children(sp.is_type("from_clause")).first()
        elif context.segment.is_type("select_statement"):
            children = FunctionalContext(context).segment.children()
            from_clause = children.select(sp.is_type("from_clause")).first()
        self._eval_clauses(context, query, from_clause, result)
        return result
