"""
security.py

Implements metadata-driven physical column-level security transforms
(encryption, hashing, masking) as required by the framework.

These transforms are applied in Silver (and optionally Gold) so the protected
data is physically stored that way.

Key design points:
- Policies come from platform_control.control.column_policies (sparse).
- Encryption keys are resolved from Azure Key Vault via Databricks secret scope
  (recommended) or direct references.
- Hashing is deterministic (good for joins) but irreversible.
- We still recommend Unity Catalog dynamic masking / row filters as an
  additional defense-in-depth layer for consumers who have different access rights.
"""

from typing import List, Dict, Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import json

try:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils()
except Exception:
    dbutils = None


def _get_secret(secret_ref: str) -> Optional[str]:
    """
    Resolve a secret.
    Expected format in column_policies: "hr-pii-encryption-key"
    This should be the name of a secret in a Databricks secret scope that is
    backed by Azure Key Vault.
    """
    if not secret_ref:
        return None

    scope = "keyvault-secrets"  # Convention: create a scope named this way backed by AKV
    if dbutils is not None:
        try:
            return dbutils.secrets.get(scope=scope, key=secret_ref)
        except Exception as e:
            # Fail loudly: emitting a placeholder key would silently produce wrong
            # ciphertext / hashes for protected columns.
            raise RuntimeError(
                f"Could not retrieve secret '{secret_ref}' from scope '{scope}': {e}"
            ) from e

    # No dbutils available. Unit tests monkeypatch _get_secret, so reaching here in a
    # real pipeline run means secrets are genuinely unavailable — fail rather than
    # encrypt/hash with a fake key.
    raise RuntimeError(
        f"Cannot resolve secret '{secret_ref}': dbutils / secret scope unavailable"
    )


def _apply_encryption(df: DataFrame, column: str, key: str) -> DataFrame:
    """Apply AES encryption (GCM mode recommended for modern Databricks)."""
    if not key:
        print(f"WARNING: No key provided for encryption of column {column}. Leaving as-is.")
        return df

    # Using aes_encrypt with a key. In production you may want to use a key derived
    # from the secret + a per-column salt for better security.
    return df.withColumn(
        column,
        F.expr(f"aes_encrypt({column}, unbase64('{key}'), 'GCM')")
    )


def _apply_hash(df: DataFrame, column: str) -> DataFrame:
    """Deterministic SHA-256 hash. Useful when you still need to join on the value."""
    return df.withColumn(column, F.sha2(F.col(column).cast("string"), 256))


def _apply_mask(df: DataFrame, column: str) -> DataFrame:
    """Simple masking. Replace with more sophisticated UDF or regex as needed."""
    return df.withColumn(
        column,
        F.regexp_replace(F.col(column).cast("string"), ".", "*")
    )


def apply_column_policies(
    df: DataFrame,
    policies: List[Dict[str, Any]],
    control_catalog: Optional[str] = None
) -> DataFrame:
    """
    Apply all column policies from the metadata to the DataFrame.

    policies: list of dicts coming from get_column_policies()
    """
    result_df = df

    for policy in policies:
        col_name = policy["column_name"]
        policy_type = policy.get("policy_type", "tag_only")
        key_ref = policy.get("encryption_key_vault_ref")
        classification = policy.get("classification", "unknown")

        print(f"Applying policy '{policy_type}' to column '{col_name}' (classification={classification})")

        if policy_type == "encrypt":
            key = _get_secret(key_ref)
            result_df = _apply_encryption(result_df, col_name, key)
        elif policy_type == "hash":
            result_df = _apply_hash(result_df, col_name)
        elif policy_type == "mask":
            result_df = _apply_mask(result_df, col_name)
        elif policy_type == "redact":
            result_df = result_df.withColumn(col_name, F.lit("[REDACTED]"))
        elif policy_type == "tag_only":
            # No physical change. The classification is recorded in metadata
            # and can be used to create UC masks or for documentation.
            pass
        else:
            print(f"Unknown policy_type '{policy_type}' for column {col_name}")

    return result_df
