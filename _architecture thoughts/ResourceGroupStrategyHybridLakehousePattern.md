I am an azure data architect and need to plan a resource group strategy for a databricks and fabric hybrid architecture with key vault, storage, data factory. Recommend some best practices to ise in each subscription, one for dev, qa, prod


**Resource group (RG) strategy for your Databricks + Microsoft Fabric hybrid data platform (with Azure Data Factory, ADLS Gen2 storage, and Key Vault) in separate Dev/QA/Prod subscriptions.**

Your setup already follows Cloud Adoption Framework (CAF) landing zone best practices by using **one dedicated Azure subscription per environment** (Dev, QA, Prod). This delivers strong isolation for billing, Azure Policy, RBAC, quotas, and security boundaries.

Within each subscription, the key principle is to **group resources by shared lifecycle, management needs, and security boundary**. Resources that you deploy/update/delete together belong in the same RG. Separate concerns (storage vs. compute vs. security) to enable independent scaling, locking, private endpoints, and least-privilege RBAC.

### Recommended RG Structure (identical across Dev/QA/Prod subs for IaC consistency)
Use this pattern in **every** subscription. It works extremely well for Databricks + Fabric hybrid architectures (ADLS as the common lake, Fabric capacities + shortcuts/mirroring for integration, ADF or Fabric Data Factory for orchestration, Key Vault for secrets).



Resource Group Name (example)	Purpose / Resources Included	Why Separate? (Lifecycle & Best Practice)
rg-dp-networking	VNet, subnets, NSGs, Private DNS zones, Private Endpoints (for storage, Key Vault, ADF, Databricks)	Networking is foundational and often peered or hub-spoke. Separate so you can apply network policies/locks independently.
rg-dp-storage	ADLS Gen2 storage account(s) (bronze/silver/gold containers or one account with hierarchical namespaces), lifecycle management policies	Long-lived data. Apply resource locks (CanNotDelete in Prod), backup policies, and stricter network/firewall rules. Storage is the single source of truth for both Databricks and Fabric.
rg-dp-compute	Azure Databricks workspace(s), Fabric Capacity (F-series SKU)	Compute scales independently. Databricks auto-creates a managed RG for clusters — keep the workspace here. Fabric capacity is an Azure resource you create here.
rg-dp-orchestration	Azure Data Factory (or Fabric Data Factory linked services)	Pipelines and integration runtimes have their own lifecycle and linked services (to KV, storage, Databricks).
rg-dp-security	Azure Key Vault(s), managed identities (for ADF → Databricks/Storage, Databricks → Storage)	Security boundary. One KV per environment (best practice from Microsoft). Reduce blast radius. Use secret scopes in Databricks.

From <https://grok.com/c/bfe660bf-075f-4aed-8cf8-278d90ea5710?rid=0cfbe566-651a-4941-be21-9ca632abe1db> 



**Naming convention** (CAF-aligned and Azure-friendly):
- `rg-<platform>-<component>` (no environment suffix needed inside the RG name because the **subscription already represents the environment**).
- Use lowercase + hyphens.
- Add tags on every RG and resource: `Environment` (dev/qa/prod), `Owner`, `CostCenter`, `DataClassification`, `Project` = "DataPlatform".

### Environment-Specific Nuances (Dev vs QA vs Prod)
- **Dev subscription**:
  - Smaller SKUs (e.g., Databricks Standard, Fabric F2/F4, ADF small integration runtime).
  - Allow public endpoints initially for faster iteration (disable in QA/Prod).
  - No production data — use synthetic or masked datasets.
  - More permissive Azure Policy (e.g., allow public storage for dev only).

- **QA subscription**:
  - Production-like sizing and configuration.
  - Mirror Prod networking and private endpoints.
  - Use for performance, integration, and user-acceptance testing.

- **Prod subscription**:
  - Highest availability: Geo-redundant storage (GRS/RA-GRS), zone-redundant Databricks/Fabric, auto-scaling.
  - Strict policies: private endpoints only, resource locks on storage and Key Vault, diagnostic settings to Log Analytics.
  - Separate Fabric capacity sized for production workloads (or multiple capacities if isolation is needed).

### Additional Best Practices Tailored to Your Hybrid Architecture
1. **Hybrid Databricks + Fabric integration**:
   - Use **ADLS Gen2 as the primary lake** (not OneLake-only) for seamless cross-platform access.
   - In Fabric, use **shortcuts** to Databricks-managed tables or ADLS paths.
   - Enable **trusted workspace access** on storage (add Fabric workspace and Databricks via ARM/Bicep) so both platforms can bypass firewall rules securely.
   - Databricks → Fabric mirroring (preview) or Fabric Data Factory activities calling Databricks notebooks/jobs are common patterns.

2. **Security & Access**:
   - **Managed identities everywhere** (ADF uses system-assigned MI to access KV, storage, Databricks; Databricks uses its own MI).
   - Key Vault: One per environment (in `rg-dp-security`). Link ADF linked services and Databricks secret scopes to it.
   - Private endpoints + Private DNS for all PaaS services in Prod/QA.
   - Azure RBAC at RG level (e.g., Data Engineers get Contributor on compute RG only).

3. **Deployment & Governance**:
   - **Infrastructure as Code** (Bicep or Terraform) — parameterize everything and deploy the exact same structure to Dev → QA → Prod via Azure DevOps/GitHub Actions.
   - Azure Policy initiatives per subscription (e.g., require private endpoints, tags, approved regions).
   - Use Management Groups above the subscriptions for common policies (Platform, Workloads, etc.).

4. **Cost & Operations**:
   - Tags enable accurate FinOps reporting.
   - Fabric capacity and Databricks clusters auto-pause where possible.
   - Monitor with Azure Monitor + Fabric capacity metrics.

5. **Anti-patterns to avoid**:
   - Putting everything in one RG → hard to lock, delete, or assign granular RBAC.
   - Mixing environments in one subscription/RG.
   - Storing secrets in ADF or Databricks directly (always use Key Vault).

This structure scales cleanly whether you have one central data platform or multiple data products later (you can add more RGs or even more subscriptions by domain). It aligns with current Microsoft guidance for Azure Databricks Well-Architected Framework, Fabric capacity management, and CAF landing zones.

If you share more details (e.g., VNet injection requirements, number of data products, or whether you prefer Fabric Data Factory vs. Azure Data Factory), I can refine the Bicep templates or provide a sample naming/tag policy. Happy to help you build the IaC starter kit!
