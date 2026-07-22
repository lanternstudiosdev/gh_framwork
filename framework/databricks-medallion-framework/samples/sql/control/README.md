# Samples — control SQL

DDL for running medallion **samples** against the same control model as production.

## Files

| File | Purpose |
|------|---------|
| `00_create_catalogs.sql` | Control catalog + HR data catalog (admin-owned; may run outside DAB) |
| `01_create_schemas.sql` | `control` schema + `edw_hr_dev` medallion schemas + `files.landing` / `published` volumes |
| `02_control_tables.sql` | Same table shapes as `sql/control/02_control_tables.sql` |

## How samples use control

1. Run `00` → `01` → `02` (storage account comes from `config/environments.yaml`).
2. Apply **sample** YAML into control tables (set `config_root` to `samples/config` when running apply), **or** apply production `config/` if you are testing full HR entities.
3. Copy seed/generated files into the volume:

```text
/Volumes/edw_hr_dev/files/landing/raw/workday/employees/
/Volumes/edw_hr_dev/files/landing/raw/workday/departments/
```

4. Run sample Bronze/Silver under `samples/pipelines/` (or framework HR pipelines).

See the main [samples README](../../README.md).
