"""How this project writes money, in one place.

Cents are the only representation the contract allows, and there is exactly one function
that turns them into something a person reads. That is not tidiness, it is a bug this
repository has now had twice:

  * the close report divided by 100 as a float, in a codebase whose contract says "money is
    integer cents everywhere; there is no float in this pipeline" and whose argument for
    cents-as-BIGINT is that it removes an entire class of rounding differences. At
    1 234 567 890 123 456 789 cents it printed a figure ending 568,00 for one ending 567,89;
  * the same figure appeared as `662,481.62` on the report and `662 481,62` in the evidence
    and the post-mortem: one number, two conventions, on a page documented as generated from
    the same table the other two are.

The fix for the second was to correct the report. That left `samegold demo` - the thing a
reader runs before anything else - still dividing by 100 and still printing en-US, which is
how a fix in one of two places gets called a fix. There is one function now.
"""

from __future__ import annotations


def euros(cents: int) -> str:
    """67269342 -> "672 693,42". Integer arithmetic, Spanish convention, no sign.

    The thousands separator is a plain ASCII space and the decimal separator is a comma,
    matching `evidence/runs/*.json`, the rendered post-mortem and the close report. A
    non-breaking space would look identical in a rendered document and would make any
    comparison against it compare two things a reader cannot tell apart.
    """
    whole, fraction = divmod(abs(int(cents)), 100)
    return f"{whole:,}".replace(",", " ") + f",{fraction:02d}"


def signed_euros(cents: int) -> str:
    """The same, with an explicit sign, for a delta that is meaningless without one."""
    return f"{'+' if int(cents) >= 0 else '-'}{euros(cents)}"
