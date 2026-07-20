# Declarative pipelines: Bronze, Silver, Gold

## Short answer

| Layer | HR (production subject) | Samples |
|-------|-------------------------|---------|
| **Bronze** | Yes — Connect-first `hr_workday_bronze` | Yes |
| **Silver** | Yes — `hr_workday_silver` | Yes |
| **Gold** | **Not** in DABs yet | Yes |

**HR production path:** Bronze + Silver declarative pipelines.  
Gold is optional and not auto-applied.

**Other subjects:** `sales` (Dynamics 365) and `refdata` (SQL) are scaffolded with Bronze + Silver pipelines that reuse the same generic entry modules (`src/pipelines/bronze_entry.py` / `silver_entry.py`); populate their `config/entities/*.yaml` before enabling schedules.

Config Apply only loads **metadata**; it does not run pipelines.

---

## Primary HR orchestration (Connect)

```text
[Lakeflow Connect → __src]
        │
        ├─► [hr_workday_bronze]              schema: bronze            (restricted=false)
        │         └─► [hr_workday_silver]    schema: silver
        │
        └─► [hr_workday_bronze_restricted]   schema: bronze_restricted (restricted=true)
                  └─► [hr_workday_silver_restricted]  schema: silver_restricted
```

Workflow: `hr_workday_orchestration` (standard and restricted layers in parallel).

Pipeline config `restricted_scope: "true"|"false"` filters entities by the `restricted` flag.

### Dynamic registration validation (no cluster required)

Pipelines build a **registration plan** first (`src/pipelines/registration.py`) — pure Python, no DLT — then apply `@dlt.table` with default-arg closures. The plan is subject-agnostic; the generic entry modules `src/pipelines/bronze_entry.py` and `src/pipelines/silver_entry.py` run it for whichever `subject_area_key` the pipeline configures.

| Check | Where |
|-------|--------|
| Unit tests | `tests/test_pipeline_registration.py` |
| CI lint | `scripts/lint_framework_config.py` |

This does **not** replace a live DBR smoke run, but catches naming, ownership (`__src`), and restricted-split regressions in PR CI.

---

## Fallback HR orchestration (API / Volume)

```text
[api_extract] → [hr_workday_bronze] → [archive_landing]
                      └──────────────→ [hr_workday_silver]
```

Workflow: `hr_workday_api_fallback_orchestration` (only when entities use `api_extract`).

---

## Layer responsibilities

| Layer | Role |
|-------|------|
| Bronze | Source fidelity + technical columns; from Connect table or Volume Auto Loader |
| Silver | Dedupe, PKs, policies, expectations |
| Gold | Marts / aggregates (not wired for HR yet) |

---

## Summary

- Modular pipelines per layer — not one automatic B+S+G for every entity.  
- Workday: Connect → Bronze → Silver.  
- Gold: add later as a separate pipeline + orchestration task.
