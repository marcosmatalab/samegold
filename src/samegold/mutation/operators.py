"""Mechanical mutation operators for data code.

Mutants are GENERATED, not planted. The distinction is the whole point: a set of bugs
chosen by the person who wrote the gate is a set of bugs the gate was designed to catch,
and its catch rate says nothing. Generated mutants include ones the author never imagined,
including equivalent ones, and the equivalent ones have to be enumerated and explained
rather than quietly dropped.

Two families, because this project has two implementations:

  * SQL mutants over the DuckDB reference (sqlglot AST), and over the Spark SQL that the
    declarative pipeline runs;
  * Python mutants over the business rules and the transformation code (Python AST).

And a third, hand-written on purpose and labelled as such: SPECIFICATION mutants. Those
change what the pipeline is *supposed* to do (which month a return is imputed to, how long
the return window is, what the deduplication key is). They cannot be generated, because a
generator has no idea what the contract means, and they are the only mutants able to
falsify the project's claim that its witnesses are partly independent: a specification
mutant that every witness survives is a blind spot the README has to name.
"""

from __future__ import annotations

import ast
import copy
from dataclasses import dataclass
from typing import Any, Literal

import sqlglot
from sqlglot import exp

MutantKind = Literal["sql", "python", "spec"]


@dataclass(frozen=True, slots=True)
class Mutant:
    mutant_id: str
    kind: MutantKind
    operator: str
    location: str
    original: str
    mutated: str
    source: str
    equivalent_reason: str | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "mutant_id": self.mutant_id,
            "kind": self.kind,
            "operator": self.operator,
            "location": self.location,
            "original": self.original,
            "mutated": self.mutated,
            "equivalent_reason": self.equivalent_reason,
        }


# --------------------------------------------------------------------------- SQL

_CMP_SWAP: dict[type[exp.Expression], type[exp.Expression]] = {
    exp.LTE: exp.LT,
    exp.LT: exp.LTE,
    exp.GTE: exp.GT,
    exp.GT: exp.GTE,
    exp.EQ: exp.NEQ,
}

_JOIN_SWAP = {"LEFT": "INNER", "FULL": "LEFT", "INNER": "LEFT", "": "LEFT"}


def mutate_sql(sql: str, dialect: str = "duckdb") -> list[Mutant]:
    """Every single-edit mutation of a SQL statement, in a stable order.

    Stability matters: mutant ids are used in evidence and in the README, so walking the
    tree in a different order between runs would renumber the survivors.
    """
    tree = sqlglot.parse_one(sql, read=dialect)
    mutants: list[Mutant] = []
    nodes = list(tree.walk())

    def emit(operator: str, index: int, mutate: Any, location: str, before: str) -> None:
        clone = tree.copy()
        target = list(clone.walk())[index]
        replacement = mutate(target)
        if replacement is None:
            return
        target.replace(replacement)
        after = replacement.sql(dialect=dialect)
        mutants.append(
            Mutant(
                mutant_id=f"SQL-{len(mutants) + 1:03d}",
                kind="sql",
                operator=operator,
                location=location,
                original=before[:120],
                mutated=after[:120],
                source=clone.sql(dialect=dialect, pretty=True),
            )
        )

    for index, node in enumerate(nodes):
        cls = type(node)
        if cls in _CMP_SWAP:
            emit(
                f"cmp:{cls.__name__}->{_CMP_SWAP[cls].__name__}",
                index,
                lambda n, c=_CMP_SWAP[cls]: c(this=n.this.copy(), expression=n.expression.copy()),
                f"comparison#{index}",
                node.sql(dialect=dialect),
            )
        elif isinstance(node, exp.Interval):
            emit(
                "interval:+1",
                index,
                _bump_interval,
                f"interval#{index}",
                node.sql(dialect=dialect),
            )
        elif isinstance(node, exp.Join):
            emit(
                "join:kind-swap",
                index,
                _swap_join,
                f"join#{index}",
                node.sql(dialect=dialect)[:120],
            )
        elif isinstance(node, exp.Sum):
            emit(
                "agg:sum->max",
                index,
                lambda n: exp.Max(this=n.this.copy()),
                f"agg#{index}",
                node.sql(dialect=dialect),
            )
        elif isinstance(node, exp.Coalesce):
            emit(
                "coalesce:drop-default",
                index,
                lambda n: n.this.copy(),
                f"coalesce#{index}",
                node.sql(dialect=dialect),
            )
        elif isinstance(node, exp.Ordered):
            emit(
                "order:flip",
                index,
                _flip_order,
                f"ordered#{index}",
                node.sql(dialect=dialect),
            )
    return mutants


def _bump_interval(node: exp.Expression) -> exp.Expression | None:
    """INTERVAL 45 DAY -> INTERVAL 46 DAY.

    sqlglot parses the count of an interval as a *string* literal, not a numeric one
    (``Literal(this='45', is_string=True)``), so the obvious ``is_number`` check silently
    generates nothing. Getting this wrong would have removed the whole family of
    window-boundary mutants, which are the ones that matter most in this domain.
    """
    clone = node.copy()
    literal = clone.this
    if not isinstance(literal, exp.Literal):
        return None
    text = str(literal.this).strip()
    head = text.split(" ")[0]
    if not head.isdigit():
        return None
    literal.set("this", text.replace(head, str(int(head) + 1), 1))
    return clone


def _swap_join(node: exp.Expression) -> exp.Expression | None:
    clone = node.copy()
    side = (clone.args.get("side") or "").upper()
    kind = (clone.args.get("kind") or "").upper()
    current = side or kind
    new = _JOIN_SWAP.get(current)
    if new is None:
        return None
    if new == "INNER":
        clone.set("side", None)
        clone.set("kind", "INNER")
    else:
        clone.set("side", new)
        clone.set("kind", None)
    return clone


def _flip_order(node: exp.Expression) -> exp.Expression:
    clone = node.copy()
    clone.set("desc", not bool(clone.args.get("desc")))
    return clone


# ------------------------------------------------------------------------ Python

_PY_CMP_SWAP: dict[type[ast.cmpop], type[ast.cmpop]] = {
    ast.LtE: ast.Lt,
    ast.Lt: ast.LtE,
    ast.GtE: ast.Gt,
    ast.Gt: ast.GtE,
    ast.Eq: ast.NotEq,
}

# Method calls whose removal is a realistic data bug rather than a syntax error.
_REMOVABLE_CALLS = frozenset(
    {"dropDuplicates", "distinct", "withWatermark", "dropna", "filter", "where", "cache"}
)


class _Mutation(ast.NodeTransformer):
    def __init__(self, target: int, action: str) -> None:
        self.counter = 0
        self.target = target
        self.action = action
        self.applied: tuple[str, str] | None = None

    def visit_Compare(self, node: ast.Compare) -> ast.AST:
        self.generic_visit(node)
        if self.action != "cmp" or len(node.ops) != 1:
            return node
        op = type(node.ops[0])
        if op not in _PY_CMP_SWAP:
            return node
        self.counter += 1
        if self.counter - 1 == self.target:
            before = ast.unparse(node)
            node.ops = [_PY_CMP_SWAP[op]()]
            self.applied = (before, ast.unparse(node))
        return node

    def visit_Constant(self, node: ast.Constant) -> ast.AST:
        if (
            self.action != "const"
            or not isinstance(node.value, int)
            or isinstance(node.value, bool)
        ):
            return node
        self.counter += 1
        if self.counter - 1 == self.target:
            before = ast.unparse(node)
            node = ast.Constant(value=node.value + 1)
            self.applied = (before, ast.unparse(node))
        return node

    def visit_Call(self, node: ast.Call) -> ast.AST:
        self.generic_visit(node)
        if self.action != "drop_call":
            return node
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr in _REMOVABLE_CALLS:
            self.counter += 1
            if self.counter - 1 == self.target:
                before = ast.unparse(node)
                self.applied = (before, ast.unparse(func.value))
                return func.value
        return node


def mutate_python(source: str, module_name: str = "<module>") -> list[Mutant]:
    tree = ast.parse(source)
    mutants: list[Mutant] = []
    for action in ("cmp", "const", "drop_call"):
        index = 0
        while True:
            transformer = _Mutation(index, action)
            clone = transformer.visit(copy.deepcopy(tree))
            if transformer.applied is None:
                break
            ast.fix_missing_locations(clone)
            before, after = transformer.applied
            mutants.append(
                Mutant(
                    mutant_id=f"PY-{len(mutants) + 1:03d}",
                    kind="python",
                    operator=action,
                    location=f"{module_name}#{action}[{index}]",
                    original=before[:120],
                    mutated=after[:120],
                    source=ast.unparse(clone),
                )
            )
            index += 1
    return mutants
