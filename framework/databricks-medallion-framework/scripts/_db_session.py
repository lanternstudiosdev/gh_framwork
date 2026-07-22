"""
Shared helper: open a Spark session via **Databricks Connect** so the SQL /
inspection scripts in this folder run straight from VS Code (or any terminal)
**without a SQL Warehouse**.

Databricks Connect is a thin client: your `spark.sql(...)` calls execute on
remote Databricks **serverless** compute, and results stream back to your laptop.
No local Java/Spark install is required.

One-time setup
--------------
1. Install the client (matching your workspace's Databricks Runtime major version):

       pip install "databricks-connect>=15.4"

2. Authenticate the Databricks CLI to your workspace (OAuth U2M is recommended):

       databricks auth login --host https://<your-workspace>.azuredatabricks.net

   Alternatively set env vars DATABRICKS_HOST + DATABRICKS_TOKEN, or select a
   named profile with DATABRICKS_CONFIG_PROFILE=<profile-name>.

Docs
----
- Databricks Connect (Python):
  https://learn.microsoft.com/azure/databricks/dev-tools/databricks-connect/python/
- Serverless compute:
  https://learn.microsoft.com/azure/databricks/compute/serverless/
- Authentication:
  https://learn.microsoft.com/azure/databricks/dev-tools/auth/
"""

from __future__ import annotations

import sys


def get_spark(serverless: bool = True):
    """Return a Databricks Connect Spark session.

    ``serverless=True`` uses Databricks **serverless** compute (no cluster or
    warehouse to start). Set it to False to attach to the cluster configured in
    your default profile (``DATABRICKS_CLUSTER_ID``) instead.

    Exits with a friendly message if the client is missing or auth fails, so a
    new engineer immediately knows what to fix.
    """
    try:
        from databricks.connect import DatabricksSession
    except ImportError:
        sys.exit(
            "ERROR: databricks-connect is not installed.\n"
            '  pip install "databricks-connect>=15.4"\n'
            "Then authenticate:\n"
            "  databricks auth login --host https://<your-workspace>.azuredatabricks.net\n"
            "Docs: https://learn.microsoft.com/azure/databricks/dev-tools/databricks-connect/python/"
        )

    try:
        builder = DatabricksSession.builder
        if serverless:
            builder = builder.serverless(True)
        # Auth resolves automatically from: env vars (DATABRICKS_HOST/TOKEN),
        # DATABRICKS_CONFIG_PROFILE, or the CLI default profile.
        return builder.getOrCreate()
    except Exception as e:  # pragma: no cover - network/auth dependent
        sys.exit(
            f"ERROR: could not start a Databricks Connect session: {e}\n"
            "Check that you are authenticated (databricks auth login) and that\n"
            "serverless compute is enabled for your workspace.\n"
            "Docs: https://learn.microsoft.com/azure/databricks/dev-tools/databricks-connect/python/"
        )
