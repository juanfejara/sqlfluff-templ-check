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
        self._eval_clauses(context, FunctionalContext(context).segment, result)
        return result

    def _eval_clauses(
        self, context: RuleContext, clause, result: List[LintResult] = []
    ) -> EvalResultType:
        """Recursively evaluation to find not allowed tables or views reference."""
        if not hasattr(clause, "get") or not callable(clause.get):
            clause = Segments(cast(BaseSegment, clause))
        if clause.get() is None:
            return result
        if clause.get().is_type("select_statement"):
            self._eval_select_statement(context, clause, result)
        elif clause.get().is_type("with_compound_statement"):
            cte_clauses = clause.children(sp.is_type("common_table_expression"))
            self._eval_cte_clause(context, cte_clauses, result)
            self._eval_select_statement(
                context, clause.children(sp.is_type("select_statement")), result
            )
        elif clause.get().is_type("set_expression"):
            self._eval_set_clause(context, clause, result)
        return result

    def _eval_select_statement(
        self, context: RuleContext, clause: Segments, result: List[LintResult] = []
    ) -> EvalResultType:
        """Evaluate the select statement, and for every nested expression call"""
        for element in clause.children(sp.is_type("select_clause")).children(
            sp.is_type("select_clause_element")
        ):
            if element.is_expandable:
                bracketed = element.segments[0]
                expression = self._get_bracketed(bracketed).children(sp.is_type("expression"))
                self._eval_clauses(context, Segments(expression.get().segments[0]), result)
        from_clause = clause.children(sp.is_type("from_clause"))
        self._eval_from_clause(context, from_clause, result)
        self._eval_join_clauses(context, from_clause, result)
        return result

    def _eval_set_clause(
        self, context: RuleContext, set_clauses: Segments, result: List[LintResult] = []
    ) -> EvalResultType:
        """Evalueate set expressions, and for every nested expression call
        recorsively the evaluation"""
        for sel in set_clauses.children(sp.is_type("select_statement")):
            self._eval_clauses(context, sel, result)
        return result

    def _eval_cte_clause(
        self, context: RuleContext, cte_clauses: Segments, result: List[LintResult] = []
    ) -> EvalResultType:
        """Evalueate common table expressions, and for every nested expression call
        recorsively the evaluation"""
        for cte in cte_clauses.iterate_segments():
            self.with_aliases.append(cte.children(sp.is_type("identifier"))[0].raw.lower())
            bracketed = cte.children(sp.is_type("bracketed"))
            for clause in self._get_clauses_in_bracketed(bracketed):
                self._eval_clauses(context, clause, result)
        return result

    def _eval_join_clauses(
        self, context: RuleContext, clause: Segments, result: List[LintResult] = []
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
            if join.is_expandable:
                bracketed = join_table_expression.children(sp.is_type("bracketed"))
                self._eval_clauses(
                    context, bracketed.children(sp.is_type("select_statement")), result
                )
                alias_expression = (
                    Segments(join)
                    .children(sp.is_type("from_expression_element"))
                    .children(sp.is_type("alias_expression"))
                )
                if alias_expression:
                    identfifier = alias_expression.children(sp.is_type("identifier")).get()
                    if identfifier:
                        self.with_aliases.append(identfifier.raw.lower())
            else:
                table_reference = cast(
                    TableReferenceSegment,
                    join_table_expression.children(sp.is_type("table_reference")).get(),
                )
                alias_ref = table_reference.raw if bool(table_reference) else ""
                if bool(join) and not join.is_templated and alias_ref not in self.with_aliases:
                    idx = join.raw_segments[0].get_start_point_marker().source_slice.start
                    raw_seg = self._find_raw_at_src_idx(context.segment, idx)
                    result.append(
                        LintResult(
                            anchor=raw_seg,
                            description=f"Hard coded join " f"`{raw_seg.raw}` is not allowed.",
                        )
                    )
        return result

    def _get_statement(self, segment: Segments) -> Segments:
        """Get the statement inside a bracketed segment or a select statement"""
        return self._get_clauses_in_bracketed(Segments(cast(BaseSegment, segment)).children())

    def _eval_from_clause(
        self, context: RuleContext, clause: Segments, result: List[LintResult]
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
            if table_expression.is_expandable:
                statement = self._get_statement(cast(Segments, table_expression))
                self._eval_clauses(context, statement, result)
            else:
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
                            description=f"Hard codes tables/views "
                            f"`{raw_seg.raw}` are not allowed.",
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

    def _get_clauses_in_bracketed(self, bracketed: Segments) -> Segments:
        """Get the clauses inside a bracketed segment"""
        if bracketed.children(sp.is_type("select_statement")):
            return bracketed.children(sp.is_type("select_statement"))
        elif bracketed.children(sp.is_type("set_expression")):
            return bracketed.children(sp.is_type("set_expression"))
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
