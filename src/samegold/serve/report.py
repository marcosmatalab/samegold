"""One self-contained HTML page: the close, its versions, and what moved after signature.

Self-contained means what it says: no server, no CDN, no JavaScript. It opens from the file
system, which is the only way a report survives being emailed to somebody in finance.

Why this exists at all: a hiring manager who reviewed the design said the project would read
as a laboratory without a consumption layer, and he was right. A number nobody looks at is not
a deliverable. The page is generated from the same versioned gold table the claims digest, so
it cannot drift from them.
"""

from __future__ import annotations

import datetime as dt
import html
from collections.abc import Sequence
from typing import Any

from samegold.domain.money import euros

_STYLE = """
:root { color-scheme: light dark; }
body { font: 15px/1.55 -apple-system, "Segoe UI", system-ui, sans-serif; margin: 0 auto;
       max-width: 60rem; padding: 2.5rem 1.5rem 4rem; }
h1 { font-size: 1.6rem; margin: 0 0 .25rem; }
p.lede { color: #555; margin: 0 0 2rem; }
table { border-collapse: collapse; width: 100%; margin: 0 0 2rem;
        font-variant-numeric: tabular-nums; }
th, td { text-align: right; padding: .45rem .6rem;
         border-bottom: 1px solid rgba(128,128,128,.28); }
th:first-child, td:first-child { text-align: left; }
thead th { font-size: .72rem; letter-spacing: .08em; text-transform: uppercase; color: #666; }
tr.restated td { background: rgba(200, 120, 0, .10); }
td.negative { color: #a3352a; }
.note { font-size: .85rem; color: #666; border-left: 3px solid rgba(128,128,128,.4);
        padding-left: .9rem; margin: 0 0 1.6rem; }
@media (prefers-color-scheme: dark) { p.lede, thead th, .note { color: #aaa; } }
"""


def render_report(versions: Sequence[dict[str, Any]], generated_at: dt.datetime) -> str:
    """Render the close report. ``versions`` is gold.revenue_by_month, every version of it."""
    by_month: dict[str, list[dict[str, Any]]] = {}
    for row in versions:
        by_month.setdefault(str(row["accounting_month"]), []).append(dict(row))

    rows_html: list[str] = []
    # A month "moved" if it has more than one version, not if its NET moved. Those are not
    # the same question and the page used to ask the second while highlighting rows by the
    # first: a restatement where gross and returns both rose by 500 cents leaves net
    # unchanged, so the table showed an orange restated row with a change of 0,00 while the
    # sentence above it said "0 of 1 months moved after they were signed off". The figure in
    # the lede now counts the same thing the highlighting marks.
    restated_months = 0
    for month in sorted(by_month):
        series = sorted(by_month[month], key=lambda r: int(r["close_version"]))
        first = series[0]
        if len(series) > 1:
            restated_months += 1
        for row in series:
            restated = int(row["close_version"]) > 0
            classes = ' class="restated"' if restated else ""
            change = int(row["net_cents"]) - int(first["net_cents"])
            rows_html.append(
                f"<tr{classes}><td>{html.escape(month)}</td>"
                f"<td>{row['close_version']}</td>"
                f"<td>{euros(int(row['gross_cents']))}</td>"
                f"<td>{euros(int(row['returns_cents']))}</td>"
                f"<td>{euros(int(row['net_cents']))}</td>"
                f'<td class="{"negative" if change < 0 else ""}">'
                f"{'' if not restated else euros(change)}</td>"
                f"<td>{html.escape(str(row['restatement_reason']))}</td>"
                f"<td>{html.escape(str(row['restated_at'])[:10])}</td></tr>"
            )

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>samegold - monthly close</title><style>{_STYLE}</style></head>
<body>
<h1>Monthly close</h1>
<p class="lede">Generated {html.escape(generated_at.isoformat(timespec="seconds"))} from
gold.revenue_by_month. Every version is shown: a close is never rewritten, and
{restated_months} of {len(by_month)} months were restated after they were signed off.</p>
<p class="note">Highlighted rows are restatements: a return arrived up to 45 days after the
sale and is imputed to the month of the sale, so a month that had already been closed changed.
The change column is measured against version 0, the figure finance signed.</p>
<table>
<thead><tr><th>Month</th><th>Version</th><th>Gross (EUR)</th><th>Returns (EUR)</th>
<th>Net (EUR)</th><th>Change vs. v0</th><th>Reason</th><th>As of</th></tr></thead>
<tbody>
{chr(10).join(rows_html)}
</tbody></table>
</body></html>
"""
