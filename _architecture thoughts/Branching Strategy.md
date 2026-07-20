**Branching Strategy for Databricks (GitHub + Terraform + DABs): dev/qa/main**

Your proposed model — **dev branch → Dev workspace**, **qa branch → QA workspace**, **main branch → Prod workspace** — is a perfectly valid and common enterprise pattern (a full GitFlow with dedicated long-lived environment branches). It works extremely well with your separate per-environment Databricks workspaces, Terraform, and Databricks Asset Bundles (DABs).

### How it works in practice
- **dev** branch = integration branch for Dev workspace. Developers create `feature/*` branches off `dev`, PR → merge to `dev` → GitHub Actions automatically runs `databricks bundle deploy --target dev`.
- **qa** branch = QA/staging. Merge `dev` → `qa` (via PR) → deploy to QA workspace.
- **main** branch = Production source of truth. Merge `qa` → `main` (release PR with approvals) → deploy to Prod workspace.
- **Hotfixes**: Still created off `main`, merged back to `main` (immediate Prod fix), then cherry-picked or merged down to `qa` and `dev`.

**Terraform + DABs integration**:
- Use environment-specific targets in your `databricks.yml` file (or Terraform variables).
- GitHub Actions workflows can use branch-name conditions or a matrix to deploy to the correct target workspace.

### Comparison to the simpler QA-branch + main=Prod model we discussed earlier



Aspect	dev/qa/main (your new proposal)	qa + main only (previous recommendation)	Winner for most teams
Environment mapping	Perfect 1:1 (each branch = one workspace)	qa = QA, main = Prod (Dev uses feature branches or personal clones)	dev/qa/main if you want strict isolation
Hotfix simplicity	Excellent (always off main)	Excellent	Tie
Merge overhead	Higher (2 promotion steps: dev→qa→main)	Lower (only qa→main)	qa + main
Risk of branch drift	Medium (more long-lived branches)	Lower	qa + main
CI/CD complexity	More pipelines/workflows to maintain	Simpler	qa + main
Developer experience	Very clear (“I work on dev branch for Dev env”)	Slightly less obvious for Dev	dev/qa/main
Audit / compliance	Excellent (main always = Prod)	Excellent	Tie

From <https://grok.com/c/bfe660bf-075f-4aed-8cf8-278d90ea5710?rid=0cfbe566-651a-4941-be21-9ca632abe1db> 



**Recommendation**:  
Go with **dev/qa/main** if your team is large, has multiple parallel workstreams, or if business/audit teams want a crystal-clear “this branch = this environment” mapping. It’s cleaner for your hybrid Databricks + Fabric setup with separate subscriptions.  

Stick with the simpler **qa + main** (and treat Dev as feature-branch driven) if your team is smaller or you want to minimize merge hell. Both are far better than using `main` as QA.  

Either way, use **protected branches + required PR reviews** + automated DAB + Terraform validation in GitHub Actions.

### Fabric Capacities + Workspace-Level Private Link: Impact on Power BI Mobile Reporting

**Short answer**: No — **users will NOT be able to access Power BI reports on mobile (iOS/Android) over public internet or mobile data once you privatize the workspace**.

Here’s exactly what happens when you enable **workspace-level Private Link + “Block public internet access”** (the full privatization step):

- The workspace (and all Power BI items inside it — reports, dashboards, apps, semantic models) becomes accessible **only** through the approved private endpoints in your hub/spoke VNets.
- The Fabric portal (`app.fabric.microsoft.com`) and Power BI service endpoints return “Access Restricted” or simply fail to load for anyone outside your private network.
- Power BI Mobile apps rely on the exact same public service endpoints as the web experience. They do **not** have built-in private-link support like Power BI Desktop does.

**Result for end users** (business users, executives, field teams):
- Mobile app will fail to connect or show errors when the capacity/workspace is privatized.
- Same applies to embedded Power BI in custom mobile apps.

**How to make mobile Power BI work with privatized Fabric** (if mobile is business-critical):
1. **Corporate VPN / Always-On VPN** (most common enterprise solution) — Mobile devices must connect to your corporate network (via Microsoft Intune + Global Secure Access or third-party VPN) so they resolve the private DNS (`privatelink.fabric.microsoft.com`) and route traffic through your spoke VNet.
2. **Provide guidance + device management** — Users get instructions to “Connect to VPN before opening Power BI app”.
3. **Test thoroughly** — Microsoft has demo videos showing mobile devices connected via private endpoints, but they assume the device is on the corporate network.

**Trade-off summary** (important for your architecture decision):
- Full privatization = maximum security (great for compliance) but breaks seamless mobile access for non-VPN users.
- If mobile reporting is a hard requirement for many users → consider keeping some workspaces/capacities public (or use tenant-level private link selectively) or accept VPN as the UX cost.
- Workspace-level private link is still the **best option** for your multi-environment + business-segmented setup because you can privatize only the Prod capacity and leave Dev/QA more open during development.

This is all current as of April 2026 (workspace-level Private Link is GA).

If you share more about your user base (how many mobile Power BI consumers vs. internal analysts) or want sample GitHub Actions YAML / Terraform for the dev/qa/main flow + Fabric Private Link service, I can generate them right away!
