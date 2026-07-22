# Design Review & Implementation Plan
## Databricks Medallion Ingestion Framework

**Status (current):** HR-first path with **Workday → Lakeflow Connect** default; UC Volume retained as API/file fallback.

| Area | State |
|------|--------|
| Control catalog DDL | `sql/control/` |
| Catalog DDL (control + subject data catalogs) | `sql/control/00_create_catalogs.sql` |
| Schema + volume DDL | `sql/control/01_create_schemas.sql` |
| Config (sources, 13 HR entities, overlays) | `config/` |
| Volume helpers + config merge | `src/lib/volumes.py`, `config_merge.py` |
| Config Apply, API extract, archive | `src/jobs/` |
| HR Bronze + Silver pipelines | `src/pipelines/hr/` |
| DABs orchestration | `bundles/databricks.yml` → `hr_workday_orchestration` |
| Samples (volume-aligned + DDL) | `samples/` |
| Unit tests (paths/merge) | `tests/test_volumes.py` |

Control catalog: **`edw_platform_control_{env}`**  
HR catalog: **`edw_hr_{env}`**

---

### 1. Workspace bootstrap (next operational step)

**Still required in a live workspace**

- Run `sql/control/00` → `01` → `02` (fix storage LOCATION URLs).
- Create secret scope `kv-hr-{env}` with Workday secret **names** from config.
- `databricks bundle deploy` + `apply_control_config`.
- Smoke: extract (or seed volume) → bronze → archive → silver.

---

### 2. Reprocess Dispatcher

**Current state**

- `reprocess_dispatcher.py` queries approved requests, forces watermarks, triggers workflows.
- HR workflow naming: `hr_workday_orchestration` / `reprocess_hr_workday`.
- Optional `pipeline_assets` lookup with convention fallback.

**Remaining**

- Populate `pipeline_assets` from config.
- Failed-status updates when trigger fails.
- Scheduled dispatcher (5–15 min) vs GH-only approval transition.
- Surface completion back to PR/issue.

---

### 3. Observability (Log Analytics)

**Remaining**

- `src/lib/observability.py` with correlation_id (GitHub run + commit).
- Structured events: pipeline, quality, reprocess.
- Sample KQL / workbook.

---

### 4. CI robustness

**Current:** unit tests + YAML load; soft smoke on HR bronze.

**Remaining:** contract schema validation (jsonschema/Pydantic), `bundle validate`, hard-fail smoke, golden expectation tests.

---

### 5. Bronze operational maturity

- DLQ / rescued data for Auto Loader.
- OPTIMIZE/ZORDER/VACUUM policy driven from entity metadata.
- Late-arriving data path vs main watermark.

---

### 6. Environment promotion & multi-subject scale

- Formalize overlays for qat/prod (`hr.qat.yaml`, `workday.prod.yaml`).
- `sales` (Dynamics 365) and `refdata` (SQL) subjects are scaffolded via the generic
  entry modules (`bronze_entry.py` / `silver_entry.py`); remaining work is real entity
  lists / connections + full production paths.
- Onboarding checklist for new subject areas.

---

### 7. Security & governance

- UC grants automation from tags/policies.
- Key rotation for column-policy keys.
- Contract → UC comments publication.

---

### Suggested phases

**Phase 1 — Done in repo**  
Scaffolding, UC Volumes, HR config + pipelines, control DDL, samples.

**Phase 2 — Next**  
Live workspace proof, hardened reprocess, observability, CI hard smoke, Bronze ops.

**Phase 3**  
Multi-subject self-service, grants automation, advanced monitoring.

### Risks

- Dynamic DLT registration needs validation against your DBR/Lakeflow version.
- Reprocess blast radius — keep approval for full historical reloads.
- Secret name drift — treat scope/key names as reviewed Git config.

This plan is living documentation; update as decisions ship.
