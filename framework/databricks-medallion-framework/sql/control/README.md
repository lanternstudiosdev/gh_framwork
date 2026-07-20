# Control plane SQL (platform metadata)

Executable DDL for the **shared platform control catalog** and the **HR data catalog skeleton**.

## Layout

| File | Purpose |
|------|---------|
| `00_create_catalog_and_schema.sql` | Create `edw_platform_control_{env}` + `control` schema |
| `01_control_tables.sql` | All control tables + helper views |
| `02_hr_data_catalog_skeleton.sql` | `edw_hr_dev` medallion schemas + `files.landing` / `published` volumes |
| `03_workspace_proof_checks.sql` | Post-deploy verification queries (option 8) |

## Catalog naming

| Env | Control catalog | HR data catalog |
|-----|-----------------|-----------------|
| dev | `edw_platform_control_dev` | `edw_hr_dev` |
| qat | `edw_platform_control_qat` | `edw_hr_qat` |
| prod | `edw_platform_control_prod` | `edw_hr_prod` |

Scripts default to **dev**. For other envs, search-replace catalog names (and storage LOCATION URLs in `02_`).

## Run order

1. Ensure Unity Catalog metastore, storage credential, and external location for HR storage exist.
2. Run `00_create_catalog_and_schema.sql`.
3. Run `01_control_tables.sql`.
4. Run `02_hr_data_catalog_skeleton.sql` (adjust `abfss://` LOCATION values for your account).
5. Deploy the DAB bundle and run **`apply_control_config`** to load Git YAML into control tables  
   (entities, sources, **pipeline_assets**, reprocess requests, …).
6. Follow [docs/08-workspace-proof-runbook.md](../../docs/08-workspace-proof-runbook.md) for live proof.
7. Use `03_workspace_proof_checks.sql` after apply + orchestration.

```bash
# After DDL + secrets are in place:
databricks bundle deploy --target dev
databricks bundle run apply_control_config --target dev
```

## Design notes

- **Control is a separate catalog** from subject data (`edw_hr_*`) so multi-domain GitOps stays single-plane.
- **JSON STRING columns** hold free-form maps (API params, Auto Loader options, connection blobs). See `src/lib/metadata.py`.
- **Workday default ingest** is Lakeflow Connect (no Volume).  
- **Landing paths** (API/file fallback only) are UC Volumes: `/Volumes/edw_hr_{env}/files/landing/raw/{source_key}/{entity_name}/`.
- **Runtime tables**: `watermark_state` is updated by pipelines/dispatcher, not bulk-replaced by config apply.

## Samples

Parallel sample-oriented DDL and notes live under [`../../samples/sql/control/`](../../samples/sql/control/).
