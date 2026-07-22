# Control plane SQL (platform metadata)

Executable DDL for the **shared platform control catalog** and the **HR data catalog skeleton**.

## Layout

| File | Purpose |
|------|---------|
| `00_create_catalogs.sql` | Create catalogs: `edw_platform_control_{env}` + subject data catalogs (e.g. `edw_hr_{env}`). **Admin-owned** — a metastore admin may run this outside the DAB deployment. Also where UC connections live. |
| `01_create_schemas.sql` | Create the `control` schema + subject medallion schemas (`bronze`/`silver`/`gold` + `*_restricted`) + `files.landing` / `published` external volumes |
| `02_control_tables.sql` | All control tables + helper views |
| `03_workspace_proof_checks.sql` | Post-deploy verification queries (see [docs/07 workspace proof](../../docs/07-workspace-proof-runbook.md)) |

> **Why catalogs are separate:** Unity Catalog catalogs (and connections) are
> typically owned by a metastore admin and may be provisioned outside DAB
> deployments. `00_create_catalogs.sql` isolates that DDL so an admin can own
> catalogs while engineers / the DAB own schemas, volumes, and control tables.
> Both `00` and `01` are written so adding a new subject area (Sales, RefData)
> is a copy-paste of the HR block.

## Catalog naming

| Env | Control catalog | HR data catalog |
|-----|-----------------|-----------------|
| dev | `edw_platform_control_dev` | `edw_hr_dev` |
| qat | `edw_platform_control_qat` | `edw_hr_qat` |
| prod | `edw_platform_control_prod` | `edw_hr_prod` |

Scripts default to **dev**. You do **not** need a SQL Warehouse to run them:

- **From VS Code (no warehouse):** `python scripts/run_control_sql.py --env dev`
  runs 00–02 on serverless compute. The SQL files are **templates**: the runner
  fills in `{env}` (from `--env`) and `{storage_account}` (from
  [`config/environments.yaml`](../../config/environments.yaml)). Pass `--files`
  to run a subset — e.g. skip `00_create_catalogs.sql` if an admin already
  created the catalogs. See [scripts/README.md](../../scripts/README.md).
- **In a notebook / SQL editor:** print paste-ready SQL with
  `python scripts/run_control_sql.py --env dev --dry-run` and paste it in.

The **storage account** for each environment lives in one place —
`config/environments.yaml` (`environments.<env>.storage_account`). Container
names follow `edw-{subject}-{env}`. The `qat` and `prod` account names in that
file are placeholders — confirm the real ADLS Gen2 account names before running
against those environments.

## Run order

1. Ensure Unity Catalog metastore, storage credential, and external location for HR storage exist.
2. Run `00_create_catalogs.sql` (metastore admin, or the runner) to create the control + subject catalogs.
3. Run `01_create_schemas.sql` to create schemas + volumes (storage account comes from `config/environments.yaml`).
4. Run `02_control_tables.sql`.
5. Deploy the DAB bundle and run **`apply_control_config`** to load Git YAML into control tables  
   (entities, sources, **pipeline_assets**, reprocess requests, …).
6. Follow [docs/07-workspace-proof-runbook.md](../../docs/07-workspace-proof-runbook.md) for live proof.
7. Use `03_workspace_proof_checks.sql` after apply + orchestration.

```bash
# After DDL + secrets are in place:
databricks bundle deploy --target dev_personal
databricks bundle run apply_control_config --target dev_personal
```

## Design notes

- **Control is a separate catalog** from subject data (`edw_hr_*`) so multi-domain GitOps stays single-plane.
- **JSON STRING columns** hold free-form maps (API params, Auto Loader options, connection blobs). See `src/lib/metadata.py`.
- **Workday default ingest** is Lakeflow Connect (no Volume).  
- **Landing paths** (API/file fallback only) are UC Volumes: `/Volumes/edw_hr_{env}/files/landing/raw/{source_key}/{entity_name}/`.
- **Runtime tables**: `watermark_state` is updated by pipelines/dispatcher, not bulk-replaced by config apply.

## Samples

Parallel sample-oriented DDL and notes live under [`../../samples/sql/control/`](../../samples/sql/control/).
