# Samples — control SQL

DDL for running medallion **samples** against the same control model as production.

## Files

| File | Purpose |
|------|---------|
| `00_create_catalog_and_schema.sql` | Control catalog + `control` schema |
| `01_control_tables.sql` | Same table shapes as `sql/control/01_control_tables.sql` |
| `02_hr_data_catalog_skeleton.sql` | `edw_hr_dev` + `files.landing` volume |

## How samples use control

1. Run `00` → `01` → `02` (edit storage LOCATION URLs).
2. Apply **sample** YAML into control tables (set `config_root` to `samples/config` when running apply), **or** apply production `config/` if you are testing full HR entities.
3. Copy seed/generated files into the volume:

```text
/Volumes/edw_hr_dev/files/landing/raw/workday/employees/
/Volumes/edw_hr_dev/files/landing/raw/workday/departments/
```

4. Run sample Bronze/Silver under `samples/pipelines/` (or framework HR pipelines).

See the main [samples README](../../README.md).
