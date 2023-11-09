"""Rule that requires dbt ref/source macro in sql FROM.
"""

from sqlfluff.core.parser.segments import BaseSegment
from sqlfluff.core.rules import BaseRule, EvalResultType, LintResult, RuleContext
from sqlfluff.core.rules.crawlers import SegmentSeekerCrawler
from sqlfluff.dialects.dialect_ansi import (
    BracketedSegment,
    IdentifierSegment,
    TableReferenceSegment,
)
from sqlfluff.utils.functional import FunctionalContext, Segments
from sqlfluff.utils.functional import segment_predicates as sp
from typing import List, cast


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

    with_aliases: List[str] = []
    groups = ("all",)
    crawl_behaviour = SegmentSeekerCrawler(
        {"select_statement", "with_compound_statement"}, allow_recurse=False
    )
    _dialects_requiring_alias_for_values_clause = [
        "snowflake",
    ]
    is_fix_compatible = False

    def _eval(self, context: RuleContext) -> EvalResultType:
        """Evaluate to find not allowed tables or views reference, only allow templated
        references."""
        result: List[LintResult] = []
        if context.segment.is_type("with_compound_statement"):
            from_clause = FunctionalContext(context).segment
        # In case of segment of type select_statement
        else:
            children = FunctionalContext(context).segment.children()
            from_clause = children.select(sp.is_type("from_clause")).first()
        self._eval_clauses(context, from_clause, result)
        return result

    def _eval_clauses(
        self, context: RuleContext, clause, result: List[LintResult] = []
    ) -> EvalResultType:
        """Recursively evaluation to find not allowed tables or views reference."""
        self._eval_cte_clause(context, clause, result)
        if clause.get().is_type("with_compound_statement"):
            clause = clause.children(sp.is_type("select_statement")).children(
                sp.is_type("from_clause")
            )
        self._eval_from_clause(context, clause, result)
        self._eval_join_clauses(context, clause, result)
        return result

    def _eval_cte_clause(
        self, context: RuleContext, from_clause, result: List[LintResult] = []
    ) -> EvalResultType:
        for cte in from_clause.children(sp.is_type("common_table_expression")).iterate_segments():
            self.with_aliases.append(cte.children(sp.is_type("identifier"))[0].raw.lower())
            bracketed = cte.children(sp.is_type("bracketed"))
            from_clause_bracketed = self._get_clause_in_bracketed(bracketed)
            self._eval_clauses(context, from_clause_bracketed, result)
        return result

    def _eval_join_clauses(
        self, context: RuleContext, clause, result: List[LintResult] = []
    ) -> EvalResultType:
        """Evaluate the tables, views or nested queries inside a join clause if exists
        not allowed references"""
        joins = clause.children(sp.is_type("from_expression")).children(sp.is_type("join_clause"))
        # Loop over joins clauses
        for join in joins:
            join_table_expression = (
                Segments(join)
                .children(sp.is_type("from_expression_element"))
                .children(sp.is_type("table_expression"))
            )
            bracketed = join_table_expression.children(sp.is_type("bracketed"))
            table_reference = cast(
                TableReferenceSegment,
                join_table_expression.children(sp.is_type("table_reference")).get(),
            )
            alias = table_reference.raw if bool(table_reference) else ""
            # In case join like: left join (select id from tbl) as tb on tb.id = othertb.id
            # Iterate over the select
            if bool(bracketed):
                from_clause_bracketed = self._get_clause_in_bracketed(bracketed)
                self._eval_clauses(context, from_clause_bracketed, result)
            # Else there not need iteration like in : left join tbl as tb on tb.id = othertb.id
            elif bool(join) and not join.is_templated and alias not in self.with_aliases:
                idx = join.raw_segments[0].get_start_point_marker().source_slice.start
                raw_seg = self._find_raw_at_src_idx(context.segment, idx)
                result.append(
                    LintResult(
                        anchor=raw_seg,
                        description=f"Hard coded join " f"`{raw_seg.raw}` is not allowed.",
                    )
                )
        # In case nested query inside join eval recursively
        return result

    def _eval_from_clause(
        self, context: RuleContext, clause, result: List[LintResult]
    ) -> EvalResultType:
        """Evaluate the tables, views or nested queries inside a from clause if exists
        not allowed references"""
        table_expressions = (
            clause.children(sp.is_type("from_expression"))
            .children(sp.is_type("from_expression_element"))
            .children(sp.is_type("table_expression"))
        )
        # Loop over for tables
        for table_expression in table_expressions:
            identifier = self._get_identifier(table_expression)
            bracketed = self._get_bracketed(table_expression)
            if (
                bool(identifier)
                and not identifier.get().is_templated
                and identifier.get().raw not in self.with_aliases
            ):
                idx = identifier.raw_segments[0].get_start_point_marker().source_slice.start
                raw_seg = self._find_raw_at_src_idx(context.segment, idx)
                result.append(
                    LintResult(
                        anchor=raw_seg,
                        description=f"Hard codes tables/views " f"`{raw_seg.raw}` are not allowed.",
                    )
                )
            # Recursive iteration over nested queries
            elif bool(bracketed):
                clause = bracketed.children(
                    sp.is_type("with_compound_statement")
                ) or bracketed.select().children(sp.is_type("select_statement")).children(
                    sp.is_type("from_clause")
                )
                self._eval_clauses(context, clause, result)
        return result

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

    def _get_clause_in_bracketed(self, bracketed):
        if bracketed.children(sp.is_type("select_statement")):
            return (
                bracketed.children(sp.is_type("select_statement"))
                .children(sp.is_type("from_clause"))
                .first()
            )
        else:
            return bracketed.children(sp.is_type("with_compound_statement"))

    def _get_bracketed(self, table_expression):
        """Get the query nested inside brackets on a table expression"""
        bracketed_segments = Segments(table_expression).children(sp.is_type("bracketed"))
        if bool(bracketed_segments) and bracketed_segments.get().is_type("bracketed"):
            return bracketed_segments
        else:
            return cast(BracketedSegment, bracketed_segments.get())

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
