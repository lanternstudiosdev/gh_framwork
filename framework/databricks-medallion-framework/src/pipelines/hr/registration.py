"""Back-compat shim.

The registration planner moved to the subject-agnostic module
``pipelines.registration``. This shim preserves the old import path
(``pipelines.hr.registration``) used by tests, the config linter, and any
existing references. Import from ``pipelines.registration`` in new code.
"""

from __future__ import annotations

from pipelines.registration import (  # noqa: F401
    BronzeRegistration,
    LoadKind,
    SilverRegistration,
    filter_entities_by_scope,
    plan_bronze_registrations,
    plan_silver_registrations,
    registration_plan_summary,
    validate_registration_plan,
)

__all__ = [
    "BronzeRegistration",
    "LoadKind",
    "SilverRegistration",
    "filter_entities_by_scope",
    "plan_bronze_registrations",
    "plan_silver_registrations",
    "registration_plan_summary",
    "validate_registration_plan",
]
