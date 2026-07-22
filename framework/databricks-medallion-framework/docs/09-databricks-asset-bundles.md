# Declarative Automation Bundles (DABs) — a beginner's guide

> **New to Databricks or VS Code?** Start here. This guide explains how this
> framework is deployed, assuming **no prior Databricks experience**.

**Declarative Automation Bundles** (DABs) — *formerly called Databricks Asset
Bundles* — are Databricks' built-in **infrastructure-as-code** tool. Instead of
clicking around the Databricks web UI to create jobs and pipelines, you describe
everything in YAML files that live next to your code in Git. One command then
creates or updates all of it in the workspace.

Think of a bundle as **"the whole project as code"**: the pipelines, the jobs,
which environment they go to, and how they're wired together.

- Official overview: <https://learn.microsoft.com/azure/databricks/dev-tools/bundles/>

---

## Why we use bundles here

| Without bundles | With bundles (this framework) |
|-----------------|-------------------------------|
| Create jobs/pipelines by hand in the UI | Defined once in [`bundles/databricks.yml`](../bundles/databricks.yml) |
| Hard to reproduce across dev/qat/prod | One file, four **targets** (environments) |
| No review/history of infra changes | Infra changes go through Git pull requests |
| Manual, error-prone deploys | `databricks bundle deploy` does it all |

---

## What you need installed (one time)

1. **Databricks CLI** (v0.218.0 or newer) — the tool that reads the bundle and
   talks to your workspace.
   Install: <https://learn.microsoft.com/azure/databricks/dev-tools/cli/install>

   ```bash
   databricks --version
   ```

2. **Authenticate** the CLI to your workspace (OAuth is recommended):

   ```bash
   databricks auth login --host https://<your-workspace>.azuredatabricks.net
   ```

3. *(Optional but recommended)* the **Databricks extension for VS Code**, which
   lets you deploy and run bundles from the editor:
   <https://learn.microsoft.com/azure/databricks/dev-tools/vscode-ext/>

---

## Anatomy of our `bundles/databricks.yml`

Open [`bundles/databricks.yml`](../bundles/databricks.yml) alongside this guide.
It has five top-level sections:

```yaml
bundle:        # the bundle's name
sync:          # which local folders get uploaded to the workspace
artifacts:     # things to build before deploy (here: the Python wheel)
variables:     # values that change per environment (catalog names, etc.)
targets:       # the deployable environments (dev_personal, dev_shared, qat, prod)
resources:     # the actual Databricks objects: pipelines + jobs
```

- **`bundle`** — just a name (`medallion-ingestion-framework`). Used to build
  deployment paths in the workspace.
- **`sync`** — the local `src/`, `config/`, `sql/` folders are uploaded so jobs
  and pipelines can find the code and YAML config.
- **`artifacts`** — builds the shared Python **wheel** (`lib`, `pipelines`,
  `jobs`) that every job/pipeline depends on.
- **`variables`** — named values (e.g. `hr_catalog`) that each target overrides,
  so the same resource definition works in dev, qat, and prod.
- **`targets`** — see below.
- **`resources`** — the jobs and Lakeflow pipelines. Each references a variable
  like `${var.hr_catalog}` so it lands in the right catalog per environment.

Full configuration reference:
<https://learn.microsoft.com/azure/databricks/dev-tools/bundles/settings>

---

## Targets = environments you can deploy to

A **target** is a named environment. You choose one with `--target <name>`.
This framework defines four:

| Target | Purpose | Deploys to | Data catalogs |
|--------|---------|-----------|----------------|
| **`dev_personal`** *(default)* | **Your own** sandbox for day-to-day work | Your personal workspace folder (`/Workspace/Users/<you>/…`), resources prefixed `[dev <you>]` so engineers never collide | `edw_*_dev` |
| **`dev_shared`** | Shared team + **CI** sandbox | A shared workspace folder (`/Workspace/Shared/…`); everyone sees one identical set of resources | `edw_*_dev` |
| **`qat`** | Quality Assurance / Test | QAT workspace paths | `edw_*_qat` |
| **`prod`** | Production | Production paths, no dev conveniences | `edw_*_prod` |

**Key idea:** `dev_personal` and `dev_shared` read/write the **same** `edw_*_dev`
data — they differ only in *where the jobs/pipelines are deployed*:

- Use **`dev_personal`** while developing on your own (it's the default, so a
  plain `databricks bundle deploy` uses it).
- Use **`dev_shared`** for shared demos and from CI/CD (CI runs as a service
  principal, not a person, so it needs the shared, non-personal target).

The `dev_*` targets use `mode: development` (schedules paused, dev-friendly);
`prod` uses `mode: production`. See deployment modes:
<https://learn.microsoft.com/azure/databricks/dev-tools/bundles/deployment-modes>

---

## The bundle lifecycle: validate → deploy → run

Run these from the `bundles/` directory. The three commands you'll use daily:

```bash
cd framework/databricks-medallion-framework/bundles

# 1. VALIDATE — check the YAML is correct (no changes made)
databricks bundle validate --target dev_personal

# 2. DEPLOY — build the wheel, upload code, create/update jobs + pipelines
databricks bundle deploy --target dev_personal

# 3. RUN — trigger a specific job or pipeline
databricks bundle run apply_control_config --target dev_personal
```

`databricks bundle run` accepts any job/pipeline name from the `resources`
section, e.g. `hr_workday_orchestration`, `hr_workday_bronze`.

Command reference:
<https://learn.microsoft.com/azure/databricks/dev-tools/cli/bundle-commands>

---

## What actually gets created

From our `resources` section, a deploy creates:

- **Lakeflow pipelines** — `hr_workday_bronze`, `hr_workday_silver`,
  `sales_bronze`, `refdata_bronze`, … (the medallion transforms).
- **Jobs** — `apply_control_config` (loads Git YAML into the control tables),
  `hr_workday_orchestration` (runs the HR path end-to-end),
  `reprocess_orchestrator`, and more.

Every apply run is recorded in
`edw_platform_control_{env}.control.config_deployments`, including a
**`dab_target`** column so you can see exactly which target
(`dev_personal` / `dev_shared` / `qat` / `prod`) produced each deployment.

---

## Typical workflows

**A developer iterating locally**

```bash
databricks bundle deploy --target dev_personal
databricks bundle run hr_workday_orchestration --target dev_personal
```

**Promoting to QAT / prod** (usually via CI, see
[`.github/workflows/deploy.yml`](../.github/workflows/deploy.yml)):

```bash
databricks bundle deploy --target qat
databricks bundle deploy --target prod
```

**Tearing down your personal sandbox** when finished:

```bash
databricks bundle destroy --target dev_personal
```

---

## How this fits the rest of the framework

1. **DDL first** — create the control catalog + tables
   (no SQL Warehouse needed: `python scripts/run_control_sql.py --env dev`; see
   [scripts/README.md](../scripts/README.md)).
2. **Deploy the bundle** — `databricks bundle deploy --target dev_personal`.
3. **Apply config** — `databricks bundle run apply_control_config …` loads the
   YAML under `config/` into the control tables (see
   [06-control-catalog-and-metadata.md](06-control-catalog-and-metadata.md)).
4. **Run pipelines** — bronze → silver, etc.

For the full hands-on sequence, follow
[07-workspace-proof-runbook.md](07-workspace-proof-runbook.md).

---

## Official documentation

- What are Declarative Automation Bundles: <https://learn.microsoft.com/azure/databricks/dev-tools/bundles/>
- Configuration reference: <https://learn.microsoft.com/azure/databricks/dev-tools/bundles/settings>
- Deployment modes (dev/prod): <https://learn.microsoft.com/azure/databricks/dev-tools/bundles/deployment-modes>
- CLI bundle commands: <https://learn.microsoft.com/azure/databricks/dev-tools/cli/bundle-commands>
- VS Code extension: <https://learn.microsoft.com/azure/databricks/dev-tools/vscode-ext/>
- Databricks Connect (run code from VS Code): <https://learn.microsoft.com/azure/databricks/dev-tools/databricks-connect/python/>
