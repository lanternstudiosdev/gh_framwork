Provide detailed steps to configure hub subscription changes and networking. Provide detailed steps to configure spoke subscriptions, resource groups and resources. A nice checklist including naming patterns following CAF. Include the workspace/capacity level private links noting if multiple workspaces can share the same privatelink


**Hub-and-Spoke Private Networking Configuration for Databricks + Fabric Hybrid (Fully Private)**

This guide provides **exact, production-ready steps** for your Azure Databricks (VNet-injected private clusters) + Microsoft Fabric (workspace-level Private Link) + ADF (managed VNet) architecture. Everything stays 100% private using hub-and-spoke topology.

- **Hub subscription** = centralized networking (transit VNet + Private DNS).
- **Spoke subscriptions** (Dev/QA/Prod) = isolated environments.
- All traffic uses Microsoft backbone + your VNets (no public endpoints).
- Private DNS zones live **only in the Hub** for central management.

**Important Fabric Private Link Note**:  
Each Fabric workspace has a **one-to-one** relationship with its own Private Link Service. Multiple workspaces **cannot share the same Private Link Service**. However, one workspace’s Private Link Service **can accept multiple private endpoints** (from different VNets). This is ideal for your segmented, multi-environment setup. Use workspace-level (not tenant-level) for granularity.

### CAF-Aligned Naming Conventions (Used Throughout)
Follow Cloud Adoption Framework (CAF) standards for clarity and governance:


---

### 1. Hub Subscription Configuration (Connectivity Subscription)
Do this **once** in your central Hub subscription (under Platform management group).

1. **Create Hub resources**:
   - Resource Group: `rg-hub-networking`
   - Resource Group: `rg-hub-dns` (for Private DNS only)

2. **Deploy Transit VNet** (in `rg-hub-networking`):
   - Name: `vnet-hub-eastus-transit-001` (adjust region)
   - Address space: e.g., `10.0.0.0/16`
   - Subnets (minimum):
     - `snet-private-endpoints` (/24 or larger — reserve ≥10 IPs per Fabric/Databricks endpoint)
     - `AzureFirewallSubnet` (if using Azure Firewall)
     - `GatewaySubnet` (for ExpressRoute/VPN)
     - `AzureBastionSubnet` (optional for testing)
   - Enable DDoS protection and Network Watcher.

3. **Create Private DNS Zones** (in `rg-hub-dns`):
   - `privatelink.azuredatabricks.net` (Databricks front-end + browser auth)
   - `privatelink.fabric.microsoft.com` (Fabric workspaces)
   - `privatelink.blob.core.windows.net`, `privatelink.dfs.core.windows.net` (ADLS)
   - `privatelink.vaultcore.azure.net` (Key Vault)
   - `privatelink.datafactory.azure.net` (ADF)
   - Link **every** zone to the Hub Transit VNet.
   - (Optional but recommended) Deploy Azure DNS Private Resolver for on-prem/conditional forwarding.

4. **(Later, after spokes)** Create Databricks front-end private endpoints **here** in the Hub Transit VNet (see Spoke steps).

5. **Peer spoke VNets** to Hub VNet (bidirectional, with "Use remote gateways" if needed).

---

### 2. Spoke Subscription Configuration (Repeat for Dev, QA, Prod)
Do these steps **identically** in each spoke subscription (`sub-dp-dev-eastus`, etc.).

#### Phase A: Networking & Resource Groups
1. Create the five RGs (CAF pattern):
   - `rg-dp-networking`
   - `rg-dp-storage`
   - `rg-dp-compute`
   - `rg-dp-orchestration`
   - `rg-dp-security`

2. Deploy Spoke VNet (in `rg-dp-networking`):
   - Name: `vnet-spoke-eastus-dp-dev-001`
   - Address space: non-overlapping (e.g., `10.10.0.0/16` for Dev)
   - Subnets for Databricks VNet injection:
     - `snet-databricks-host-dev` (/26 minimum)
     - `snet-databricks-container-dev` (/26 minimum — delegate to `Microsoft.Databricks/workspaces`)
   - Add `snet-private-endpoints` subnet (/24+ for PEs).
   - Peer to Hub Transit VNet.

3. **NSGs & Routes**:
   - Apply NSGs with required Databricks rules (after March 2026).
   - Add User-Defined Routes (UDRs) pointing egress to Hub Azure Firewall/NAT Gateway.

#### Phase B: Core Resources
4. **Storage** (`rg-dp-storage`):
   - Create ADLS Gen2 account (`stdpdevlake001`).
   - Enable hierarchical namespace, disable public access.
   - Create private endpoints (in spoke VNet) → approve.

5. **Key Vault** (`rg-dp-security`):
   - Create `kv-dp-dev`.
   - Disable public access → create private endpoint.
   - Add managed identities for ADF/Databricks.

6. **Azure Data Factory** (`rg-dp-orchestration`):
   - Create `adf-dp-dev`.
   - Enable **Managed Virtual Network** on the Azure IR.
   - Create managed private endpoints (inside ADF) to: ADLS, KV, Databricks workspace, Fabric (if needed).
   - Approve pending requests on target resources.

7. **Azure Databricks Workspace** (`rg-dp-compute`):
   - Create Premium workspace with **VNet injection**.
   - Select spoke VNet + host/container subnets.
   - Enable **Secure Cluster Connectivity (No Public IP)**.
   - Stop all compute before next step.

8. **Databricks Front-End Private Link (Inbound)**:
   - In Databricks workspace → Networking → Private endpoint connections.
   - Create private endpoint **in Hub Transit VNet** (`snet-private-endpoints`).
     - Target sub-resource: `databricks_ui_api`.
   - (Recommended) Create dedicated “browser authentication” workspace in Hub for resilience → add its `browser_authentication` private endpoint in Hub VNet.
   - Private DNS zone auto-creates A records in Hub.

9. **Fabric Capacity & Workspace Private Link** (`rg-dp-compute` or dedicated):
   - Create Fabric capacity (F-SKU) and workspace in Fabric portal.
   - **Create Private Link Service** (in Azure, same subscription):
     - Use the official ARM template (Microsoft.Fabric/privateLinkServicesForFabric):
       ```json
       {
         "type": "Microsoft.Fabric/privateLinkServicesForFabric",
         "name": "pls-fabric-dp-dev",
         "properties": {
           "tenantId": "<your-tenant-id>",
           "workspaceId": "<your-workspace-id>"
         }
       }
       ```
     - Deploy via Custom Deployment in `rg-dp-networking` or `rg-dp-compute`.
   - Create private endpoint **in spoke VNet** (`snet-private-endpoints`) pointing to the Private Link Service.
     - Target sub-resource: `workspace`.
   - In Fabric workspace settings → **Block public internet access**.
   - Link `privatelink.fabric.microsoft.com` DNS zone (already in Hub) to spoke VNet.
   - Repeat per workspace (Dev/QA/Prod + business domains).

10. **Outbound from Fabric**:
    - In Fabric workspace admin → Managed Private Endpoints → create to ADLS, KV, etc.

11. **Test**:
    - From a test VM in Hub or spoke (via VPN/ExpressRoute/Bastion): access Databricks URL and Fabric workspace.
    - Verify DNS resolution with `nslookup`.

---

### Final Checklist (Copy-Paste Ready)
- [ ] Hub subscription & Transit VNet created
- [ ] All Private DNS zones in `rg-hub-dns` + linked to Hub VNet
- [ ] Spoke VNets peered to Hub
- [ ] Databricks workspaces VNet-injected + Secure Cluster Connectivity enabled
- [ ] Databricks front-end PEs in Hub Transit VNet
- [ ] ADF managed VNet + managed private endpoints approved
- [ ] ADLS & KV private endpoints + firewall deny-public
- [ ] **Fabric**: One Private Link Service **per workspace** (ARM template) + private endpoint in spoke VNet + public access blocked
- [ ] NSGs, UDRs, NAT Gateway/Firewall configured
- [ ] IaC (Terraform) for everything + GitHub Actions (dev → qa → main branching)
- [ ] Tags, Azure Policy, resource locks (Prod storage/KV)
- [ ] Mobile Power BI: Requires corporate VPN (as discussed)

This setup is fully aligned with Microsoft’s 2026 guidance for Databricks hybrid private connect, Fabric workspace-level Private Link, and CAF hub-spoke landing zones.

If you want the **Terraform modules** (VNet injection, ARM for Fabric PLS, private endpoints, peering), GitHub Actions YAML for your dev/qa/main branching, or Bicep equivalents, just say the word and I’ll generate the complete code snippets tailored to your region/CIDR!
