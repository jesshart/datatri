"""datatri — triage data instead of failing on it.

    import datatri as dti
    r = dti.triage(frame, checks)
    r.healthy, r.sick, r.report

Healthy flows on. Sick goes to the doctor with a diagnosis. The run never breaks.
"""

from datatri.check import Check, healthy, in_range, in_set, matches, not_null, sick, unique
from datatri.conserve import Conservation, ConserveResult, conserve, safe_join
from datatri.schema import SchemaResult, check_schema
from datatri.surface import Surface
from datatri.triage import TriageBlocked, TriageResult, rollup, triage

__all__ = [
    "Check",
    "Conservation",
    "ConserveResult",
    "SchemaResult",
    "Surface",
    "TriageBlocked",
    "TriageResult",
    "check_schema",
    "conserve",
    "healthy",
    "in_range",
    "in_set",
    "matches",
    "not_null",
    "rollup",
    "safe_join",
    "sick",
    "triage",
    "unique",
]
