"""The consumption layer: what a human actually looks at.

A close nobody can see is a close nobody trusts. This package renders the gold tables into a
single self-contained HTML page - no server, no JavaScript framework, no network - and a
freshness monitor with the alert rule the contract implies.
"""

from samegold.serve.freshness import FreshnessBreach, evaluate_freshness
from samegold.serve.report import render_report

__all__ = ["FreshnessBreach", "evaluate_freshness", "render_report"]
