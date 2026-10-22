"""
dags/nammamart_etl_jitendra.py
--------------------------------
NammaMart Quick Commerce End-to-End ETL Pipeline
Author: Jitendra

Pipeline Architecture:
  Extract (5 parallel tasks):
    - extract_customers  ──┐
    - extract_products   ──┤
    - extract_stores     ──┼──► validate_data ──┬──► quarantine_failed (quarantine failed rows)
    - extract_orders     ──┤                    │
    - extract_payments   ──┘                    └──► transform ──► load_to_warehouse ──┬──► reconcile_payments
                                                                                       └──► report_metrics

Step 1: Extract, Validate across 6 DQ Dimensions & Quarantine
  - Completeness (missing keys, DELIVERED delivery_minutes, missing category, missing zone)
  - Uniqueness (duplicate order_id, payment_id, customer_id, product_id, store_id)
  - Validity (order_status, payment_mode, 6-digit pincode, discount_pct range, timestamp format)
  - Accuracy (quantity > 0, order_amount formula, delivery_minutes <= 120, mrp > 0, cost_price <= mrp, capacity > 0)
  - Consistency (referential integrity with masters, payment-to-order amount match, loyalty tier casing)
  - Timeliness (no future timestamps/signups, staleness monitoring)
  - Quarantine: Writes failed rows to include/quarantine/jitendra/namamart_failed_20261020.csv with failure_reason.
  - Scorecard: Audits all checks in dq_results scorecard table.

Step 2: Transform (Clean / Passed rows only)
  - Standardise values (trim whitespace, fix casing, map 'upi' and 'UPI ' to 'UPI').
  - Deduplicate customers and products (drop duplicate customer_id and product_id).
  - Mask PII email and phone (e.g., pr***@example.com, 98******21).
  - Join orders with customers, products, and stores.
  - Derive order_date, net_revenue, gross_margin, and within_15_min.

Step 3: Load into DuckDB (Star Schema), Reconcile & Report
  - Idempotent load into DuckDB warehouse:
      * Fact table: etl_fact_orders
      * Dimension tables: etl_dim_customer, etl_dim_product, etl_dim_store
  - Payment reconciliation: compare clean orders with payments.csv and write etl_payment_reconciliation
    (one row per mismatch with reason).
  - Executive reporting: generate etl_daily_zone_revenue and etl_category_margin and print them in log.
"""

import io
import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

# Optional Airflow / Pendulum imports with graceful fallback for standalone execution
try:
    import pendulum
except ImportError:
    pendulum = None

try:
    from airflow.decorators import dag, task
    from airflow.models import Variable
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

    class DummyTaskResult(dict):
        def __rshift__(self, other):
            return other
        def __lshift__(self, other):
            return other

    def dag(*args, **kwargs):
        def decorator(f):
            def wrapper(*a, **kw):
                return f(*a, **kw)
            wrapper.is_dag = True
            wrapper.dag_id = kwargs.get("dag_id", "nammamart_etl_jitendra")
            wrapper.tags = kwargs.get("tags", [])
            wrapper.default_args = kwargs.get("default_args", {})
            return wrapper
        if len(args) == 1 and callable(args[0]) and not kwargs:
            return decorator(args[0])
        return decorator

    def task(*args, **kwargs):
        def decorator(f):
            def wrapper(*a, **kw):
                res = f(*a, **kw)
                if isinstance(res, dict):
                    return DummyTaskResult(res)
                return res
            wrapper.__name__ = f.__name__
            return wrapper
        if len(args) == 1 and callable(args[0]) and not kwargs:
            return decorator(args[0])
        return decorator

try:
    import duckdb
except ImportError:
    duckdb = None

# Configure logging
logger = logging.getLogger("nammamart_etl_jitendra")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Path Resolution: Portable across Docker (/usr/local/airflow) & Local Windows
# ---------------------------------------------------------------------------
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
if AIRFLOW_HOME and os.path.exists(os.path.join(AIRFLOW_HOME, "include", "data", "nammamart")):
    BASE_DIR = AIRFLOW_HOME
elif os.path.exists("/usr/local/airflow/include/data/nammamart"):
    BASE_DIR = "/usr/local/airflow"
elif os.path.exists(r"C:\worktrees\assignment\IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model\include\data\nammamart"):
    BASE_DIR = r"C:\worktrees\assignment\IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model"
else:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    BASE_DIR = os.path.abspath(os.path.join(current_dir, ".."))

DATA_DIR = os.path.join(BASE_DIR, "include", "data", "nammamart")
QUARANTINE_DIR = os.path.join(BASE_DIR, "include", "quarantine", "jitendra")
TRANSFORMED_DIR = os.path.join(BASE_DIR, "include", "transformed", "jitendra")
RECON_DIR = os.path.join(BASE_DIR, "include", "reconciliation", "jitendra")
REPORTS_DIR = os.path.join(BASE_DIR, "include", "reports", "jitendra")
WAREHOUSE_DB = os.path.join(BASE_DIR, "include", "warehouse_jitendra.duckdb")

# Benchmark Reference Date for dataset / run (NammaMart run date: 2026-10-20)
REFERENCE_DATE = "2026-10-20"
REFERENCE_TIMESTAMP = "2026-10-20 23:59:59"


def to_df(obj) -> pd.DataFrame:
    """Helper to convert serialized JSON string, dict, or existing DataFrame to DataFrame."""
    if isinstance(obj, pd.DataFrame):
        return obj.copy()
    if isinstance(obj, dict):
        if "data" in obj and "columns" in obj:
            return pd.read_json(io.StringIO(json.dumps(obj)), orient="split")
        return pd.DataFrame(obj)
    if isinstance(obj, str):
        if os.path.exists(obj):
            return pd.read_csv(obj)
        try:
            return pd.read_json(io.StringIO(obj), orient="split")
        except Exception:
            try:
                return pd.read_json(io.StringIO(obj))
            except Exception:
                return pd.read_json(io.StringIO(obj), orient="records")
    return pd.DataFrame(obj)


# ---------------------------------------------------------------------------
# Transformation Helpers: Standardisation, Deduplication, Masking, and Metrics
# ---------------------------------------------------------------------------
def standardize_payment_mode(val) -> str:
    """Standardise payment mode values: trim, fix casing, map upi and UPI to UPI."""
    if pd.isna(val):
        return val
    s = str(val).strip()
    s_upper = s.upper()
    if s_upper == "UPI":
        return "UPI"
    if s_upper in ("CARD", "CREDIT CARD", "DEBIT CARD"):
        return "Card"
    if s_upper in ("CASH ON DELIVERY", "COD", "CASHH", "CASH"):
        return "Cash on Delivery"
    if s_upper == "WALLET":
        return "Wallet"
    return s


def mask_email(val) -> str:
    """
    Mask customer email address for PII protection.
    Example: priya.hegde199@example.com -> pr***@example.com
    """
    if pd.isna(val) or not str(val).strip():
        return ""
    val_str = str(val).strip()
    if "@" not in val_str:
        return "***@***"
    user, domain = val_str.split("@", 1)
    masked_user = (user[:2] if len(user) >= 2 else user[:1]) + "***"
    return f"{masked_user}@{domain.lower()}"


def mask_phone(val) -> str:
    """
    Mask customer phone number for PII protection.
    Fixed 6-asterisk mask preserving first 2 and last 2 digits.
    Example: 9774349021 -> 97******21
    """
    if pd.isna(val) or not str(val).strip():
        return ""
    digits = re.sub(r"\D", "", str(val).strip())
    if len(digits) >= 4:
        return digits[:2] + "******" + digits[-2:]
    elif len(digits) > 0:
        return digits[:1] + "******" + digits[-1:]
    return ""


def trim_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Trim leading and trailing whitespace across all string/object columns."""
    df = df.copy()
    string_cols = [c for c in df.columns if pd.api.types.is_string_dtype(df[c]) or df[c].dtype == object]
    for col in string_cols:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
    return df


def standardize_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Standardise orders: trim, fix casing on payment_mode, order_status, and channel."""
    df = trim_text_columns(df)
    if "payment_mode" in df.columns:
        df["payment_mode"] = df["payment_mode"].apply(standardize_payment_mode)
    if "order_status" in df.columns:
        df["order_status"] = df["order_status"].astype(str).str.strip().str.upper()
    if "channel" in df.columns:
        df["channel"] = df["channel"].astype(str).str.strip().str.capitalize()
    return df


def standardize_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Standardise customers: trim, fix loyalty_tier casing and city casing."""
    df = trim_text_columns(df)
    if "loyalty_tier" in df.columns:
        tier_map = {
            "bronze": "Bronze",
            "silver": "Silver",
            "gold": "Gold",
            "platinum": "Platinum",
        }
        df["loyalty_tier"] = df["loyalty_tier"].astype(str).str.strip().apply(
            lambda x: tier_map.get(x.lower(), x.title())
        )
    if "city" in df.columns:
        df["city"] = df["city"].astype(str).str.strip().str.title()
    return df


def standardize_products(df: pd.DataFrame) -> pd.DataFrame:
    """Standardise products: trim text columns."""
    return trim_text_columns(df)


def standardize_stores(df: pd.DataFrame) -> pd.DataFrame:
    """Standardise stores: trim text columns and fix zone casing."""
    df = trim_text_columns(df)
    if "zone" in df.columns:
        df["zone"] = df["zone"].astype(str).str.strip().str.title()
    return df


def transform_data(
    orders_df: pd.DataFrame,
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame,
    stores_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Executes the complete Transformation logic:
      1. Standardise values (trim whitespace, fix casing, map upi and UPI to UPI).
      2. Deduplicate customers and products (keep first occurrence per ID).
      3. Mask email and phone (e.g., pr***@example.com, 98******21).
      4. Join orders with customers, products, and stores.
      5. Derive order_date, net_revenue, gross_margin, and within_15_min.
    Returns:
      (merged_orders, clean_customers, clean_products, clean_stores)
    """
    orders_std = standardize_orders(orders_df)
    cust_std = standardize_customers(customers_df)
    prod_std = standardize_products(products_df)
    store_std = standardize_stores(stores_df)

    clean_cust = cust_std.drop_duplicates(subset=["customer_id"], keep="first").copy()
    clean_prod = prod_std.drop_duplicates(subset=["product_id"], keep="first").copy()
    clean_store = store_std.drop_duplicates(subset=["store_id"], keep="first").copy()

    clean_cust["email"] = clean_cust["email"].apply(mask_email)
    clean_cust["phone"] = clean_cust["phone"].apply(mask_phone)

    merged = orders_std.merge(clean_cust, on="customer_id", how="left")
    merged = merged.merge(clean_prod, on="product_id", how="left")
    merged = merged.merge(clean_store, on="store_id", how="left", suffixes=("_customer", "_store"))

    merged["customer_area"] = merged["area_customer"]
    merged["customer_pincode"] = merged["pincode_customer"]
    merged["store_area"] = merged["area_store"]
    merged["store_pincode"] = merged["pincode_store"]

    merged["order_date"] = pd.to_datetime(merged["order_ts"], errors="coerce").dt.strftime("%Y-%m-%d")
    merged["net_revenue"] = merged["order_amount"].round(2)
    merged["gross_margin"] = (merged["net_revenue"] - (merged["quantity"] * merged["cost_price"])).round(2)
    merged["within_15_min"] = (merged["order_status"] == "DELIVERED") & (merged["delivery_minutes"] <= 15)

    return merged, clean_cust, clean_prod, clean_store


# ---------------------------------------------------------------------------
# Validation Engine: 6 Dimensions as defined in data_extract_rule.docx
# ---------------------------------------------------------------------------
def validate_customers(df: pd.DataFrame, run_id: str, checked_at: str):
    """Validate customers.csv across the 6 dimensions."""
    df = df.copy()
    n_rows = len(df)
    reasons = [[] for _ in range(n_rows)]
    checks = []

    # 1. Completeness: required keys and city
    mask_c1 = df["customer_id"].isnull() | (df["customer_id"].astype(str).str.strip() == "")
    for i in df[mask_c1].index:
        reasons[i].append("Completeness: missing customer_id")
    checks.append({
        "check_name": "customers_completeness_customer_id",
        "dimension": "Completeness",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_c1.sum())
    })

    mask_c2 = df["city"].isnull() | (df["city"].astype(str).str.strip() == "")
    for i in df[mask_c2].index:
        reasons[i].append("Completeness: city is null or empty")
    checks.append({
        "check_name": "customers_completeness_city",
        "dimension": "Completeness",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_c2.sum())
    })

    # 2. Uniqueness: one row per customer_id
    mask_u1 = df["customer_id"].duplicated(keep=False)
    for i in df[mask_u1].index:
        reasons[i].append(f"Uniqueness: duplicate customer_id ({df.loc[i, 'customer_id']})")
    checks.append({
        "check_name": "customers_uniqueness_customer_id",
        "dimension": "Uniqueness",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_u1.sum())
    })

    # 3. Validity: pincode 6 digits (Critical), email format (Warning), phone 10 digits (Warning)
    mask_v1 = ~df["pincode"].astype(str).str.match(r"^\d{6}$")
    for i in df[mask_v1].index:
        reasons[i].append(f"Validity: invalid pincode format ({df.loc[i, 'pincode']})")
    checks.append({
        "check_name": "customers_validity_pincode_6digits",
        "dimension": "Validity",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_v1.sum())
    })

    email_clean = df["email"].astype(str).str.strip()
    mask_w_email = df["email"].isnull() | (email_clean == "") | (email_clean.str.upper() == "N/A") | ~email_clean.str.contains(r"^[\w\.-]+@[\w\.-]+\.\w+$", regex=True)
    checks.append({
        "check_name": "customers_validity_email_format",
        "dimension": "Validity",
        "severity": "Warning",
        "rows_checked": n_rows,
        "rows_failed": int(mask_w_email.sum())
    })

    mask_w_phone = ~df["phone"].astype(str).str.match(r"^\d{10}$")
    checks.append({
        "check_name": "customers_validity_phone_10digits",
        "dimension": "Validity",
        "severity": "Warning",
        "rows_checked": n_rows,
        "rows_failed": int(mask_w_phone.sum())
    })

    # 4. Consistency: loyalty_tier casing (Warning)
    valid_tiers = ["Bronze", "Silver", "Gold", "Platinum"]
    mask_w_tier = ~df["loyalty_tier"].isin(valid_tiers)
    checks.append({
        "check_name": "customers_consistency_loyalty_tier_casing",
        "dimension": "Consistency",
        "severity": "Warning",
        "rows_checked": n_rows,
        "rows_failed": int(mask_w_tier.sum())
    })

    # 5. Timeliness: No future-dated signup_date (Critical)
    mask_t1 = df["signup_date"] > REFERENCE_DATE
    for i in df[mask_t1].index:
        reasons[i].append(f"Timeliness: future signup_date ({df.loc[i, 'signup_date']})")
    checks.append({
        "check_name": "customers_timeliness_future_signup_date",
        "dimension": "Timeliness",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_t1.sum())
    })

    failed_mask = [len(r) > 0 for r in reasons]
    failed_df = df[failed_mask].copy()
    failed_df["failure_reason"] = ["; ".join(reasons[i]) for i in range(n_rows) if failed_mask[i]]
    passed_df = df[[not m for m in failed_mask]].copy()

    return passed_df, failed_df, checks


def validate_products(df: pd.DataFrame, run_id: str, checked_at: str):
    """Validate products.csv across the 6 dimensions."""
    df = df.copy()
    n_rows = len(df)
    reasons = [[] for _ in range(n_rows)]
    checks = []

    # 1. Completeness: category not null
    mask_c1 = df["category"].isnull() | (df["category"].astype(str).str.strip() == "")
    for i in df[mask_c1].index:
        reasons[i].append("Completeness: category is null or empty")
    checks.append({
        "check_name": "products_completeness_category",
        "dimension": "Completeness",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_c1.sum())
    })

    mask_c2 = df["product_id"].isnull() | (df["product_id"].astype(str).str.strip() == "")
    for i in df[mask_c2].index:
        reasons[i].append("Completeness: missing product_id")
    checks.append({
        "check_name": "products_completeness_product_id",
        "dimension": "Completeness",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_c2.sum())
    })

    # 2. Uniqueness: one row per product_id
    mask_u1 = df["product_id"].duplicated(keep=False)
    for i in df[mask_u1].index:
        reasons[i].append(f"Uniqueness: duplicate product_id ({df.loc[i, 'product_id']})")
    checks.append({
        "check_name": "products_uniqueness_product_id",
        "dimension": "Uniqueness",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_u1.sum())
    })

    # 3. Accuracy: mrp > 0
    mask_a1 = df["mrp"] <= 0
    for i in df[mask_a1].index:
        reasons[i].append(f"Accuracy: mrp must be > 0 ({df.loc[i, 'mrp']})")
    checks.append({
        "check_name": "products_accuracy_mrp_positive",
        "dimension": "Accuracy",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_a1.sum())
    })

    # 4. Accuracy: cost_price <= mrp
    mask_a2 = df["cost_price"] > df["mrp"]
    for i in df[mask_a2].index:
        reasons[i].append(f"Accuracy: cost_price ({df.loc[i, 'cost_price']}) exceeds mrp ({df.loc[i, 'mrp']})")
    checks.append({
        "check_name": "products_accuracy_cost_price_lte_mrp",
        "dimension": "Accuracy",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_a2.sum())
    })

    # 5. Validity: is_perishable boolean
    mask_v1 = ~df["is_perishable"].isin([True, False, "True", "False", 1, 0])
    for i in df[mask_v1].index:
        reasons[i].append("Validity: invalid is_perishable value")
    checks.append({
        "check_name": "products_validity_is_perishable",
        "dimension": "Validity",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_v1.sum())
    })

    # 6. Timeliness: launch_date not in future
    mask_t1 = df["launch_date"] > REFERENCE_DATE
    for i in df[mask_t1].index:
        reasons[i].append(f"Timeliness: future launch_date ({df.loc[i, 'launch_date']})")
    checks.append({
        "check_name": "products_timeliness_future_launch_date",
        "dimension": "Timeliness",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_t1.sum())
    })

    failed_mask = [len(r) > 0 for r in reasons]
    failed_df = df[failed_mask].copy()
    failed_df["failure_reason"] = ["; ".join(reasons[i]) for i in range(n_rows) if failed_mask[i]]
    passed_df = df[[not m for m in failed_mask]].copy()

    return passed_df, failed_df, checks


def validate_stores(df: pd.DataFrame, run_id: str, checked_at: str):
    """Validate stores.csv across the 6 dimensions."""
    df = df.copy()
    n_rows = len(df)
    reasons = [[] for _ in range(n_rows)]
    checks = []

    mask_c1 = df["zone"].isnull() | (df["zone"].astype(str).str.strip() == "")
    for i in df[mask_c1].index:
        reasons[i].append("Completeness: zone is null or empty")
    checks.append({
        "check_name": "stores_completeness_zone",
        "dimension": "Completeness",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_c1.sum())
    })

    mask_u1 = df["store_id"].duplicated(keep=False)
    for i in df[mask_u1].index:
        reasons[i].append("Uniqueness: duplicate store_id")
    checks.append({
        "check_name": "stores_uniqueness_store_id",
        "dimension": "Uniqueness",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_u1.sum())
    })

    mask_a1 = df["capacity_orders_per_day"] <= 0
    for i in df[mask_a1].index:
        reasons[i].append(f"Accuracy: capacity_orders_per_day must be > 0 ({df.loc[i, 'capacity_orders_per_day']})")
    checks.append({
        "check_name": "stores_accuracy_capacity_positive",
        "dimension": "Accuracy",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_a1.sum())
    })

    mask_v1 = ~df["pincode"].astype(str).str.match(r"^\d{6}$")
    for i in df[mask_v1].index:
        reasons[i].append(f"Validity: invalid pincode ({df.loc[i, 'pincode']})")
    checks.append({
        "check_name": "stores_validity_pincode_6digits",
        "dimension": "Validity",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_v1.sum())
    })

    mask_t1 = df["open_date"] > REFERENCE_DATE
    for i in df[mask_t1].index:
        reasons[i].append(f"Timeliness: future open_date ({df.loc[i, 'open_date']})")
    checks.append({
        "check_name": "stores_timeliness_future_open_date",
        "dimension": "Timeliness",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_t1.sum())
    })

    failed_mask = [len(r) > 0 for r in reasons]
    failed_df = df[failed_mask].copy()
    failed_df["failure_reason"] = ["; ".join(reasons[i]) for i in range(n_rows) if failed_mask[i]]
    passed_df = df[[not m for m in failed_mask]].copy()

    return passed_df, failed_df, checks


def validate_orders(df: pd.DataFrame, customers_df: pd.DataFrame, products_df: pd.DataFrame, stores_df: pd.DataFrame, run_id: str, checked_at: str):
    """Validate orders.csv across the 6 dimensions."""
    df = df.copy()
    n_rows = len(df)
    reasons = [[] for _ in range(n_rows)]
    checks = []

    mask_c1 = df["customer_id"].isnull() | (df["customer_id"].astype(str).str.strip() == "")
    for i in df[mask_c1].index:
        reasons[i].append("Completeness: missing customer_id")
    checks.append({
        "check_name": "orders_completeness_customer_id",
        "dimension": "Completeness",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_c1.sum())
    })

    mask_c2 = (df["order_status"].astype(str).str.upper() == "DELIVERED") & df["delivery_minutes"].isnull()
    for i in df[mask_c2].index:
        reasons[i].append("Completeness: missing delivery_minutes for DELIVERED order")
    checks.append({
        "check_name": "orders_completeness_delivered_has_delivery_minutes",
        "dimension": "Completeness",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_c2.sum())
    })

    mask_u1 = df["order_id"].duplicated(keep=False)
    for i in df[mask_u1].index:
        reasons[i].append(f"Uniqueness: duplicate order_id ({df.loc[i, 'order_id']})")
    checks.append({
        "check_name": "orders_uniqueness_order_id",
        "dimension": "Uniqueness",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_u1.sum())
    })

    valid_statuses = ["DELIVERED", "CANCELLED", "RETURNED"]
    mask_v1 = ~df["order_status"].isin(valid_statuses)
    for i in df[mask_v1].index:
        reasons[i].append(f"Validity: invalid order_status ({df.loc[i, 'order_status']})")
    checks.append({
        "check_name": "orders_validity_order_status_allowed",
        "dimension": "Validity",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_v1.sum())
    })

    valid_pay_modes = ["UPI", "Card", "Cash on Delivery", "Wallet"]
    mask_v2 = ~df["payment_mode"].isin(valid_pay_modes)
    for i in df[mask_v2].index:
        reasons[i].append(f"Validity: invalid payment_mode ({df.loc[i, 'payment_mode']})")
    checks.append({
        "check_name": "orders_validity_payment_mode_allowed",
        "dimension": "Validity",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_v2.sum())
    })

    ts_parsed = pd.to_datetime(df["order_ts"], format="mixed", errors="coerce")
    mask_v3 = ts_parsed.isnull()
    for i in df[mask_v3].index:
        reasons[i].append(f"Validity: unparseable order_ts ({df.loc[i, 'order_ts']})")
    checks.append({
        "check_name": "orders_validity_order_ts_parseable",
        "dimension": "Validity",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_v3.sum())
    })

    mask_v4 = (df["discount_pct"] < 0) | (df["discount_pct"] > 100)
    for i in df[mask_v4].index:
        reasons[i].append(f"Validity: discount_pct out of range 0-100 ({df.loc[i, 'discount_pct']})")
    checks.append({
        "check_name": "orders_validity_discount_pct_range",
        "dimension": "Validity",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_v4.sum())
    })

    mask_a1 = df["quantity"] <= 0
    for i in df[mask_a1].index:
        reasons[i].append(f"Accuracy: quantity must be > 0 ({df.loc[i, 'quantity']})")
    checks.append({
        "check_name": "orders_accuracy_quantity_positive",
        "dimension": "Accuracy",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_a1.sum())
    })

    exp_amount = (df["quantity"] * df["unit_price"] * (1 - df["discount_pct"] / 100)).round(2)
    mask_a2 = (df["order_amount"] - exp_amount).abs() > 0.05
    for i in df[mask_a2].index:
        reasons[i].append(f"Accuracy: order_amount formula mismatch (expected {exp_amount.loc[i]}, got {df.loc[i, 'order_amount']})")
    checks.append({
        "check_name": "orders_accuracy_order_amount_formula",
        "dimension": "Accuracy",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_a2.sum())
    })

    mask_a3 = (df["order_status"].astype(str).str.upper() == "DELIVERED") & (df["delivery_minutes"] > 120)
    for i in df[mask_a3].index:
        reasons[i].append(f"Accuracy: delivery_minutes exceeds 120 ({df.loc[i, 'delivery_minutes']})")
    checks.append({
        "check_name": "orders_accuracy_delivery_minutes_below_120",
        "dimension": "Accuracy",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_a3.sum())
    })

    valid_stores = set(stores_df["store_id"].dropna())
    mask_k1 = ~df["store_id"].isin(valid_stores)
    for i in df[mask_k1].index:
        reasons[i].append(f"Consistency: store_id not found in store master ({df.loc[i, 'store_id']})")
    checks.append({
        "check_name": "orders_consistency_store_id_in_master",
        "dimension": "Consistency",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_k1.sum())
    })

    valid_products = set(products_df["product_id"].dropna())
    mask_k2 = ~df["product_id"].isin(valid_products)
    for i in df[mask_k2].index:
        reasons[i].append(f"Consistency: product_id not found in product master ({df.loc[i, 'product_id']})")
    checks.append({
        "check_name": "orders_consistency_product_id_in_master",
        "dimension": "Consistency",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_k2.sum())
    })

    valid_customers = set(customers_df["customer_id"].dropna())
    mask_k3 = df["customer_id"].notnull() & ~df["customer_id"].isin(valid_customers)
    for i in df[mask_k3].index:
        reasons[i].append(f"Consistency: customer_id not found in customer master ({df.loc[i, 'customer_id']})")
    checks.append({
        "check_name": "orders_consistency_customer_id_in_master",
        "dimension": "Consistency",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_k3.sum())
    })

    mask_t1 = ts_parsed > REFERENCE_TIMESTAMP
    for i in df[mask_t1].index:
        reasons[i].append(f"Timeliness: future-dated order_ts ({df.loc[i, 'order_ts']})")
    checks.append({
        "check_name": "orders_timeliness_future_order_ts",
        "dimension": "Timeliness",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_t1.sum())
    })

    mask_w_stale = ts_parsed < "2026-09-01"
    checks.append({
        "check_name": "orders_timeliness_stale_orders",
        "dimension": "Timeliness",
        "severity": "Warning",
        "rows_checked": n_rows,
        "rows_failed": int(mask_w_stale.sum())
    })

    failed_mask = [len(r) > 0 for r in reasons]
    failed_df = df[failed_mask].copy()
    failed_df["failure_reason"] = ["; ".join(reasons[i]) for i in range(n_rows) if failed_mask[i]]
    passed_df = df[[not m for m in failed_mask]].copy()

    return passed_df, failed_df, checks


def validate_payments(df: pd.DataFrame, orders_df: pd.DataFrame, run_id: str, checked_at: str):
    """Validate payments.csv across the 6 dimensions."""
    df = df.copy()
    n_rows = len(df)
    reasons = [[] for _ in range(n_rows)]
    checks = []

    mask_u1 = df["payment_id"].duplicated(keep=False)
    for i in df[mask_u1].index:
        reasons[i].append(f"Uniqueness: duplicate payment_id ({df.loc[i, 'payment_id']})")
    checks.append({
        "check_name": "payments_uniqueness_payment_id",
        "dimension": "Uniqueness",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_u1.sum())
    })

    valid_statuses = ["SUCCESS", "FAILED", "REFUNDED"]
    mask_v1 = ~df["payment_status"].isin(valid_statuses)
    for i in df[mask_v1].index:
        reasons[i].append(f"Validity: invalid payment_status ({df.loc[i, 'payment_status']})")
    checks.append({
        "check_name": "payments_validity_payment_status_allowed",
        "dimension": "Validity",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_v1.sum())
    })

    valid_modes = ["UPI", "Card", "Cash on Delivery", "Wallet"]
    mask_v2 = ~df["payment_mode"].isin(valid_modes)
    for i in df[mask_v2].index:
        reasons[i].append(f"Validity: invalid payment_mode ({df.loc[i, 'payment_mode']})")
    checks.append({
        "check_name": "payments_validity_payment_mode_allowed",
        "dimension": "Validity",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_v2.sum())
    })

    mask_a1 = df["amount_paid"] <= 0
    for i in df[mask_a1].index:
        reasons[i].append(f"Accuracy: amount_paid must be > 0 ({df.loc[i, 'amount_paid']})")
    checks.append({
        "check_name": "payments_accuracy_amount_paid_positive",
        "dimension": "Accuracy",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_a1.sum())
    })

    valid_orders = set(orders_df["order_id"].dropna())
    mask_k1 = ~df["order_id"].isin(valid_orders)
    for i in df[mask_k1].index:
        reasons[i].append(f"Consistency: order_id not found in orders master ({df.loc[i, 'order_id']})")
    checks.append({
        "check_name": "payments_consistency_order_id_in_orders",
        "dimension": "Consistency",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_k1.sum())
    })

    orders_unique = orders_df.drop_duplicates(subset=["order_id"], keep="first")
    merged = df.merge(orders_unique[["order_id", "order_amount"]], on="order_id", how="left")
    mask_k2 = (merged["payment_status"] == "SUCCESS") & merged["order_amount"].notnull() & ((merged["amount_paid"] - merged["order_amount"]).abs() > 0.05)
    for i in merged[mask_k2].index:
        reasons[i].append(f"Consistency: amount_paid ({df.loc[i, 'amount_paid']}) does not match order_amount ({merged.loc[i, 'order_amount']})")
    checks.append({
        "check_name": "payments_consistency_amount_matches_order",
        "dimension": "Consistency",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_k2.sum())
    })

    ts_parsed = pd.to_datetime(df["payment_ts"], format="mixed", errors="coerce")
    mask_t1 = ts_parsed > REFERENCE_TIMESTAMP
    for i in df[mask_t1].index:
        reasons[i].append(f"Timeliness: future payment_ts ({df.loc[i, 'payment_ts']})")
    checks.append({
        "check_name": "payments_timeliness_future_payment_ts",
        "dimension": "Timeliness",
        "severity": "Critical",
        "rows_checked": n_rows,
        "rows_failed": int(mask_t1.sum())
    })

    failed_mask = [len(r) > 0 for r in reasons]
    failed_df = df[failed_mask].copy()
    failed_df["failure_reason"] = ["; ".join(reasons[i]) for i in range(n_rows) if failed_mask[i]]
    passed_df = df[[not m for m in failed_mask]].copy()

    return passed_df, failed_df, checks


# ---------------------------------------------------------------------------
# Scorecard Builder Helper
# ---------------------------------------------------------------------------
def build_scorecard_dataframe(all_checks: list, run_id: str, dag_id: str, checked_at: str) -> pd.DataFrame:
    records = []
    for c in all_checks:
        status = "PASSED" if c["rows_failed"] == 0 else "FAILED"
        records.append({
            "run_id": run_id,
            "dag_id": dag_id,
            "check_name": c["check_name"],
            "dimension": c["dimension"],
            "severity": c["severity"],
            "rows_checked": c["rows_checked"],
            "rows_failed": c["rows_failed"],
            "status": status,
            "checked_at": checked_at,
        })
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Reconciliation Helper
# ---------------------------------------------------------------------------
def build_payment_reconciliation(clean_orders_df: pd.DataFrame, payments_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compares clean orders with payments.csv and writes reconciliation records:
    identifying missing payments, orphan payments, amount mismatches, and status mismatches.
    """
    clean_ord = clean_orders_df.copy()
    pay_df = payments_df.copy()

    clean_ord["payment_mode_std"] = clean_ord["payment_mode"].apply(standardize_payment_mode)
    pay_df["payment_mode_std"] = pay_df["payment_mode"].apply(standardize_payment_mode)

    m = clean_ord.merge(pay_df, on="order_id", how="outer", suffixes=("_order", "_pay"))

    records = []
    now_ts = datetime.now(timezone.utc).isoformat()

    for idx, r in m.iterrows():
        reasons = []
        status = "RECONCILED"

        if pd.isna(r["payment_id"]):
            status = "MISSING_PAYMENT"
            ord_stat = r["order_status"]
            if ord_stat == "DELIVERED":
                reasons.append("Delivered order has no payment record in gateway")
            else:
                reasons.append(f"Order with status {ord_stat} has no payment record in gateway")
        elif pd.isna(r.get("order_ts")):
            status = "ORPHAN_PAYMENT"
            reasons.append("Payment record has no matching clean order (order was quarantined or missing)")
        else:
            diff = abs(r["order_amount"] - r["amount_paid"])
            if diff > 0.05:
                reasons.append(f"Amount mismatch: order amount is {r['order_amount']} but gateway paid {r['amount_paid']}")
            if r["order_status"] == "DELIVERED" and r["payment_status"] != "SUCCESS":
                pay_stat = r["payment_status"]
                reasons.append(f"Delivered order has gateway status {pay_stat}")
            if r["order_status"] in ("CANCELLED", "RETURNED") and r["payment_status"] == "SUCCESS":
                ord_stat = r["order_status"]
                reasons.append(f"Order {ord_stat} but gateway payment is SUCCESS (refund check needed)")
            if reasons:
                status = "MISMATCH"

        if reasons:
            disc = 0.0
            if pd.notna(r["order_amount"]) and pd.notna(r["amount_paid"]):
                disc = round(float(r["order_amount"] - r["amount_paid"]), 2)
            elif pd.notna(r["order_amount"]):
                disc = round(float(r["order_amount"]), 2)
            elif pd.notna(r["amount_paid"]):
                disc = round(-float(r["amount_paid"]), 2)

            records.append({
                "order_id": r["order_id"],
                "payment_id": r["payment_id"] if pd.notna(r["payment_id"]) else None,
                "order_status": r["order_status"] if pd.notna(r["order_status"]) else None,
                "payment_status": r["payment_status"] if pd.notna(r["payment_status"]) else None,
                "order_amount": float(r["order_amount"]) if pd.notna(r["order_amount"]) else None,
                "amount_paid": float(r["amount_paid"]) if pd.notna(r["amount_paid"]) else None,
                "discrepancy_amount": disc,
                "order_payment_mode": r["payment_mode_std_order"] if pd.notna(r["payment_mode_std_order"]) else None,
                "gateway_payment_mode": r["payment_mode_std_pay"] if pd.notna(r["payment_mode_std_pay"]) else None,
                "reconciliation_status": status,
                "mismatch_reason": "; ".join(reasons),
                "reconciled_at": now_ts,
            })

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Default Arguments & DAG Declaration
# ---------------------------------------------------------------------------
default_args = {
    "owner": "jitendra",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=15),
}

if pendulum:
    dag_start_date = pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata")
else:
    dag_start_date = datetime(2026, 9, 1, tzinfo=timezone.utc)


@dag(
    dag_id="nammamart_etl_jitendra",
    schedule="0 9 * * 1-6",
    start_date=dag_start_date,
    catchup=False,
    default_args=default_args,
    tags=["nammamart", "etl", "extract", "jitendra"],
    doc_md="""
    # NammaMart ETL Data Pipeline - Full Lifecycle: Extract, Validate, Quarantine, Transform, Load, Reconcile & Report
    **Author:** Jitendra
    **SLA:** Report data ready before 9:00 AM IST, Monday to Saturday (`0 9 * * 1-6`).

    ## Pipeline Overview
    1. **Extract:** 5 parallel tasks extract source CSVs from `include/data/nammamart/`.
    2. **Validate:** Enforces 6 Data Quality dimensions from `data_extract_rule.docx`.
    3. **Quarantine:** Quarantines all failed rows to `include/quarantine/jitendra/namamart_failed_20261020.csv`.
    4. **Transform:** Standardises values, deduplicates master tables, masks PII, joins tables, and derives metrics.
    5. **Load:** Idempotently loads star schema into DuckDB (`etl_fact_orders`, `etl_dim_customer`, `etl_dim_product`, `etl_dim_store`).
    6. **Reconcile:** Compares clean orders with `payments.csv` and records `etl_payment_reconciliation`.
    7. **Report:** Generates and logs `etl_daily_zone_revenue` and `etl_category_margin`.
    """
)
def nammamart_etl_pipeline():

    # ── STEP 1: EXTRACT (5 parallel tasks, one per file) ─────────────────────

    @task
    def extract_customers() -> str:
        path = os.path.join(DATA_DIR, "customers.csv")
        df = pd.read_csv(path)
        logger.info(f"[Extract:customers] Loaded {len(df)} rows from {path}")
        return df.to_json(orient="split")

    @task
    def extract_products() -> str:
        path = os.path.join(DATA_DIR, "products.csv")
        df = pd.read_csv(path)
        logger.info(f"[Extract:products] Loaded {len(df)} rows from {path}")
        return df.to_json(orient="split")

    @task
    def extract_stores() -> str:
        path = os.path.join(DATA_DIR, "stores.csv")
        df = pd.read_csv(path)
        logger.info(f"[Extract:stores] Loaded {len(df)} rows from {path}")
        return df.to_json(orient="split")

    @task
    def extract_orders() -> str:
        path = os.path.join(DATA_DIR, "orders.csv")
        df = pd.read_csv(path)
        logger.info(f"[Extract:orders] Loaded {len(df)} rows from {path}")
        return df.to_json(orient="split")

    @task
    def extract_payments() -> str:
        path = os.path.join(DATA_DIR, "payments.csv")
        df = pd.read_csv(path)
        logger.info(f"[Extract:payments] Loaded {len(df)} rows from {path}")
        return df.to_json(orient="split")

    # ── STEP 2: VALIDATE (Apply Data Quality Rules across 6 Dimensions) ─────

    @task
    def validate_data(customers_raw: str, products_raw: str, stores_raw: str, orders_raw: str, payments_raw: str) -> dict:
        checked_at = datetime.now(timezone.utc).isoformat()
        run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

        cust_df = to_df(customers_raw)
        prod_df = to_df(products_raw)
        store_df = to_df(stores_raw)
        ord_df = to_df(orders_raw)
        pay_df = to_df(payments_raw)

        logger.info(f"[Validate] Beginning DQ validation on 5 datasets (Total rows: {len(cust_df) + len(prod_df) + len(store_df) + len(ord_df) + len(pay_df)})")

        cust_pass, cust_fail, cust_checks = validate_customers(cust_df, run_id, checked_at)
        prod_pass, prod_fail, prod_checks = validate_products(prod_df, run_id, checked_at)
        store_pass, store_fail, store_checks = validate_stores(store_df, run_id, checked_at)

        ord_pass, ord_fail, ord_checks = validate_orders(ord_df, cust_df, prod_df, store_df, run_id, checked_at)
        pay_pass, pay_fail, pay_checks = validate_payments(pay_df, ord_df, run_id, checked_at)

        all_checks = cust_checks + prod_checks + store_checks + ord_checks + pay_checks
        scorecard_df = build_scorecard_dataframe(all_checks, run_id, "nammamart_etl_jitendra", checked_at)

        total_checked = len(cust_df) + len(prod_df) + len(store_df) + len(ord_df) + len(pay_df)
        total_passed = len(cust_pass) + len(prod_pass) + len(store_pass) + len(ord_pass) + len(pay_pass)
        total_failed = len(cust_fail) + len(prod_fail) + len(store_fail) + len(ord_fail) + len(pay_fail)

        logger.info(f"[Validate] Summary: Checked={total_checked} | Passed={total_passed} | Quarantined Failed={total_failed}")

        return {
            "run_id": run_id,
            "checked_at": checked_at,
            "passed_count": len(ord_pass),
            "failed_count": total_failed,
            "raw_customers": customers_raw,
            "raw_products": products_raw,
            "raw_stores": stores_raw,
            "raw_payments": payments_raw,
            "summary": {
                "total_checked": total_checked,
                "total_passed": total_passed,
                "total_failed": total_failed,
                "customers": {"total": len(cust_df), "passed": len(cust_pass), "failed": len(cust_fail)},
                "products": {"total": len(prod_df), "passed": len(prod_pass), "failed": len(prod_fail)},
                "stores": {"total": len(store_df), "passed": len(store_pass), "failed": len(store_fail)},
                "orders": {"total": len(ord_df), "passed": len(ord_pass), "failed": len(ord_fail)},
                "payments": {"total": len(pay_df), "passed": len(pay_pass), "failed": len(pay_fail)},
            },
            "failed": {
                "orders": ord_fail.to_json(orient="split"),
                "payments": pay_fail.to_json(orient="split"),
                "products": prod_fail.to_json(orient="split"),
                "customers": cust_fail.to_json(orient="split"),
                "stores": store_fail.to_json(orient="split"),
            },
            "passed": {
                "orders": ord_pass.to_json(orient="split"),
                "payments": pay_pass.to_json(orient="split"),
                "products": prod_pass.to_json(orient="split"),
                "customers": cust_pass.to_json(orient="split"),
                "stores": store_pass.to_json(orient="split"),
            },
            "scorecard": scorecard_df.to_json(orient="split"),
        }

    # ── STEP 3: QUARANTINE (Write failed rows with failure_reason column) ───

    @task
    def quarantine_failed(val_result: dict) -> dict:
        os.makedirs(QUARANTINE_DIR, exist_ok=True)

        failed_dict = val_result.get("failed", {})
        ord_fail = to_df(failed_dict.get("orders", "{}"))
        pay_fail = to_df(failed_dict.get("payments", "{}"))
        prod_fail = to_df(failed_dict.get("products", "{}"))
        cust_fail = to_df(failed_dict.get("customers", "{}"))
        store_fail = to_df(failed_dict.get("stores", "{}"))

        parts = []
        if not ord_fail.empty:
            df_p = ord_fail.copy()
            df_p["source_file"] = "orders.csv"
            df_p["record_id"] = df_p["order_id"]
            parts.append(df_p)

        if not pay_fail.empty:
            df_p = pay_fail.copy()
            df_p["source_file"] = "payments.csv"
            df_p["record_id"] = df_p["payment_id"]
            parts.append(df_p)

        if not prod_fail.empty:
            df_p = prod_fail.copy()
            df_p["source_file"] = "products.csv"
            df_p["record_id"] = df_p["product_id"]
            parts.append(df_p)

        if not cust_fail.empty:
            df_p = cust_fail.copy()
            df_p["source_file"] = "customers.csv"
            df_p["record_id"] = df_p["customer_id"]
            parts.append(df_p)

        if not store_fail.empty:
            df_p = store_fail.copy()
            df_p["source_file"] = "stores.csv"
            df_p["record_id"] = df_p["store_id"]
            parts.append(df_p)

        if parts:
            all_failed = pd.concat(parts, ignore_index=True)
            front_cols = ["source_file", "record_id", "failure_reason"]
            other_cols = [c for c in all_failed.columns if c not in front_cols]
            all_failed = all_failed[front_cols + other_cols]
        else:
            all_failed = pd.DataFrame(columns=["source_file", "record_id", "failure_reason"])

        target_csv_path = os.path.join(QUARANTINE_DIR, "namamart_failed_20261020.csv")
        all_failed.to_csv(target_csv_path, index=False)

        mirror_csv_path = os.path.join(QUARANTINE_DIR, "nammamart_failed_20261020.csv")
        all_failed.to_csv(mirror_csv_path, index=False)

        ord_fail.to_csv(os.path.join(QUARANTINE_DIR, "orders_failed_20261020.csv"), index=False)
        pay_fail.to_csv(os.path.join(QUARANTINE_DIR, "payments_failed_20261020.csv"), index=False)
        prod_fail.to_csv(os.path.join(QUARANTINE_DIR, "products_failed_20261020.csv"), index=False)
        cust_fail.to_csv(os.path.join(QUARANTINE_DIR, "customers_failed_20261020.csv"), index=False)
        store_fail.to_csv(os.path.join(QUARANTINE_DIR, "stores_failed_20261020.csv"), index=False)

        summary_json_path = os.path.join(QUARANTINE_DIR, "summary_20261020.json")
        with open(summary_json_path, "w", encoding="utf-8") as f:
            json.dump(val_result["summary"], f, indent=2)

        scorecard_df = to_df(val_result.get("scorecard", "{}"))
        scorecard_csv_path = os.path.join(QUARANTINE_DIR, "dq_results_20261020.csv")
        scorecard_df.to_csv(scorecard_csv_path, index=False)

        return {
            "quarantine_file": target_csv_path,
            "failed_count": len(all_failed),
            "summary_file": summary_json_path,
            "scorecard_file": scorecard_csv_path,
        }

    # ── STEP 4: TRANSFORM (Clean/Passed rows only -> Star Schema Merged Fact) ───

    @task
    def transform(val_result: dict) -> str:
        passed_count = val_result.get("passed_count", 0)
        passed_dict = val_result.get("passed", {})
        ord_pass_json = passed_dict.get("orders")

        if not ord_pass_json or passed_count == 0:
            logger.warning("[Transform] No clean passed orders to transform.")
            return "{}"

        orders_df = to_df(ord_pass_json)

        cust_source = val_result.get("raw_customers") or passed_dict.get("customers")
        prod_source = val_result.get("raw_products") or passed_dict.get("products")
        store_source = val_result.get("raw_stores") or passed_dict.get("stores")

        customers_df = to_df(cust_source)
        products_df = to_df(prod_source)
        stores_df = to_df(store_source)

        logger.info(f"[Transform] Executing transform on {len(orders_df)} clean orders...")

        merged_orders, clean_cust, clean_prod, clean_store = transform_data(
            orders_df, customers_df, products_df, stores_df
        )

        os.makedirs(TRANSFORMED_DIR, exist_ok=True)
        transformed_orders_path = os.path.join(TRANSFORMED_DIR, "transformed_orders_20261020.csv")
        merged_orders.to_csv(transformed_orders_path, index=False)

        clean_cust.to_csv(os.path.join(TRANSFORMED_DIR, "dim_customer_20261020.csv"), index=False)
        clean_prod.to_csv(os.path.join(TRANSFORMED_DIR, "dim_product_20261020.csv"), index=False)
        clean_store.to_csv(os.path.join(TRANSFORMED_DIR, "dim_store_20261020.csv"), index=False)

        logger.info(f"[Transform] Successfully generated {len(merged_orders)} transformed orders.")
        return merged_orders.to_json(orient="split")

    # ── STEP 5: LOAD TO DUCKDB (Idempotent Star Schema) ──────────────────────

    @task
    def load_to_warehouse(transformed_json: str) -> dict:
        """
        Idempotent load into DuckDB Star Schema:
          - etl_dim_customer
          - etl_dim_product
          - etl_dim_store
          - etl_fact_orders
        """
        if transformed_json == "{}":
            logger.warning("[Load] Nothing to load.")
            return {}

        merged_orders = to_df(transformed_json)
        now_ts = datetime.now(timezone.utc).isoformat()
        merged_orders["ingested_at"] = now_ts

        # Load clean dimension records from TRANSFORMED_DIR
        dim_cust_path = os.path.join(TRANSFORMED_DIR, "dim_customer_20261020.csv")
        dim_prod_path = os.path.join(TRANSFORMED_DIR, "dim_product_20261020.csv")
        dim_store_path = os.path.join(TRANSFORMED_DIR, "dim_store_20261020.csv")

        cust_df = pd.read_csv(dim_cust_path) if os.path.exists(dim_cust_path) else merged_orders[["customer_id", "full_name", "email", "phone", "city", "customer_area", "customer_pincode", "signup_date", "loyalty_tier", "is_active"]].drop_duplicates("customer_id")
        prod_df = pd.read_csv(dim_prod_path) if os.path.exists(dim_prod_path) else merged_orders[["product_id", "product_name", "category", "sub_category", "brand", "mrp", "cost_price", "unit", "is_perishable", "launch_date"]].drop_duplicates("product_id")
        store_df = pd.read_csv(dim_store_path) if os.path.exists(dim_store_path) else merged_orders[["store_id", "store_name", "zone", "store_area", "store_pincode", "open_date", "manager_name", "capacity_orders_per_day"]].drop_duplicates("store_id")

        cust_df["ingested_at"] = now_ts
        prod_df["ingested_at"] = now_ts
        store_df["ingested_at"] = now_ts

        fact_cols = [
            "order_id", "order_date", "order_ts", "customer_id", "store_id", "product_id",
            "quantity", "unit_price", "discount_pct", "order_amount", "net_revenue",
            "cost_price", "gross_margin", "payment_mode", "order_status", "channel",
            "delivery_minutes", "within_15_min", "ingested_at"
        ]
        fact_orders = merged_orders[fact_cols].copy()

        if not duckdb:
            logger.warning("[Load] DuckDB not installed. Skipping warehouse load.")
            return {"status": "skipped", "reason": "duckdb not available"}

        con = duckdb.connect(WAREHOUSE_DB)

        # 1. Load etl_dim_customer (Idempotent)
        con.execute("CREATE TABLE IF NOT EXISTS etl_dim_customer AS SELECT * FROM cust_df WHERE 1=0")
        existing_cust = con.execute("SELECT customer_id FROM etl_dim_customer").fetchdf()["customer_id"].tolist()
        new_cust = cust_df[~cust_df["customer_id"].isin(existing_cust)]
        if len(new_cust) > 0:
            con.execute("INSERT INTO etl_dim_customer SELECT * FROM new_cust")
            logger.info(f"[Load] Inserted {len(new_cust)} new records into etl_dim_customer")

        # 2. Load etl_dim_product (Idempotent)
        con.execute("CREATE TABLE IF NOT EXISTS etl_dim_product AS SELECT * FROM prod_df WHERE 1=0")
        existing_prod = con.execute("SELECT product_id FROM etl_dim_product").fetchdf()["product_id"].tolist()
        new_prod = prod_df[~prod_df["product_id"].isin(existing_prod)]
        if len(new_prod) > 0:
            con.execute("INSERT INTO etl_dim_product SELECT * FROM new_prod")
            logger.info(f"[Load] Inserted {len(new_prod)} new records into etl_dim_product")

        # 3. Load etl_dim_store (Idempotent)
        con.execute("CREATE TABLE IF NOT EXISTS etl_dim_store AS SELECT * FROM store_df WHERE 1=0")
        existing_store = con.execute("SELECT store_id FROM etl_dim_store").fetchdf()["store_id"].tolist()
        new_store = store_df[~store_df["store_id"].isin(existing_store)]
        if len(new_store) > 0:
            con.execute("INSERT INTO etl_dim_store SELECT * FROM new_store")
            logger.info(f"[Load] Inserted {len(new_store)} new records into etl_dim_store")

        # 4. Load etl_fact_orders (Idempotent)
        con.execute("CREATE TABLE IF NOT EXISTS etl_fact_orders AS SELECT * FROM fact_orders WHERE 1=0")
        existing_orders = con.execute("SELECT order_id FROM etl_fact_orders").fetchdf()["order_id"].tolist()
        new_orders = fact_orders[~fact_orders["order_id"].isin(existing_orders)]
        if len(new_orders) > 0:
            con.execute("INSERT INTO etl_fact_orders SELECT * FROM new_orders")
            logger.info(f"[Load] Inserted {len(new_orders)} new records into etl_fact_orders")
        else:
            logger.info("[Load] 0 new orders inserted - all order_ids already exist in etl_fact_orders")

        cust_count = con.execute("SELECT COUNT(*) FROM etl_dim_customer").fetchone()[0]
        prod_count = con.execute("SELECT COUNT(*) FROM etl_dim_product").fetchone()[0]
        store_count = con.execute("SELECT COUNT(*) FROM etl_dim_store").fetchone()[0]
        fact_count = con.execute("SELECT COUNT(*) FROM etl_fact_orders").fetchone()[0]

        con.close()

        logger.info(f"[Load] Warehouse row counts -> Fact Orders: {fact_count} | Customers: {cust_count} | Products: {prod_count} | Stores: {store_count}")

        return {
            "status": "success",
            "fact_orders_count": fact_count,
            "dim_customer_count": cust_count,
            "dim_product_count": prod_count,
            "dim_store_count": store_count,
            "new_orders_loaded": len(new_orders),
        }

    # ── STEP 6: RECONCILE PAYMENTS ──────────────────────────────────────────

    @task
    def reconcile_payments(transformed_json: str, payments_raw: str) -> dict:
        """
        Compare clean orders with payments.csv and record discrepancies in
        etl_payment_reconciliation (one row per mismatch with reason).
        """
        if transformed_json == "{}":
            logger.warning("[Reconcile] No clean orders to reconcile.")
            return {}

        clean_orders = to_df(transformed_json)
        payments_df = to_df(payments_raw)

        recon_df = build_payment_reconciliation(clean_orders, payments_df)

        os.makedirs(RECON_DIR, exist_ok=True)
        recon_csv_path = os.path.join(RECON_DIR, "etl_payment_reconciliation_20261020.csv")
        recon_df.to_csv(recon_csv_path, index=False)

        logger.info(f"[Reconcile] Found {len(recon_df)} payment mismatches. Wrote to {recon_csv_path}")

        # Idempotently write into DuckDB warehouse
        if duckdb:
            con = duckdb.connect(WAREHOUSE_DB)
            con.execute("CREATE OR REPLACE TABLE etl_payment_reconciliation AS SELECT * FROM recon_df")
            total_recon = con.execute("SELECT COUNT(*) FROM etl_payment_reconciliation").fetchone()[0]
            con.close()
            logger.info(f"[DuckDB] Recorded {total_recon} reconciliation records in {WAREHOUSE_DB}")

        return {
            "total_mismatches": len(recon_df),
            "reconciliation_file": recon_csv_path,
        }

    # ── STEP 7: REPORTING ────────────────────────────────────────────────────

    @task
    def report_metrics(load_summary: dict) -> dict:
        """
        Builds etl_daily_zone_revenue and etl_category_margin tables in DuckDB
        and prints formatted summary tables directly into the task execution log.
        """
        if not duckdb:
            logger.warning("[Report] DuckDB not available. Skipping reporting.")
            return {}

        con = duckdb.connect(WAREHOUSE_DB)

        # 1. Build etl_daily_zone_revenue
        con.execute("""
            CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
            SELECT
                f.order_date,
                s.zone,
                s.store_id,
                s.store_name,
                COUNT(f.order_id) AS total_orders,
                COUNT(CASE WHEN f.order_status = 'DELIVERED' THEN 1 END) AS delivered_orders,
                ROUND(SUM(CASE WHEN f.order_status = 'DELIVERED' THEN f.net_revenue ELSE 0 END), 2) AS daily_net_revenue
            FROM etl_fact_orders f
            JOIN etl_dim_store s ON f.store_id = s.store_id
            GROUP BY f.order_date, COALESCE(s.zone, 'Unknown'), s.store_id, s.store_name
            ORDER BY f.order_date, COALESCE(s.zone, 'Unknown'), s.store_name;
        """)

        # 2. Build etl_category_margin
        con.execute("""
            CREATE OR REPLACE TABLE etl_category_margin AS
            SELECT
                COALESCE(p.category, 'Uncategorized') AS category,
                SUM(f.quantity) AS total_quantity_sold,
                ROUND(SUM(f.net_revenue), 2) AS total_revenue,
                ROUND(SUM(f.quantity * p.cost_price), 2) AS total_cost,
                ROUND(SUM(f.gross_margin), 2) AS total_gross_margin,
                ROUND(SUM(f.gross_margin) * 100.0 / NULLIF(SUM(f.net_revenue), 0), 2) AS margin_pct
            FROM etl_fact_orders f
            JOIN etl_dim_product p ON f.product_id = p.product_id
            WHERE f.order_status = 'DELIVERED'
            GROUP BY p.category
            ORDER BY total_revenue DESC;
        """)

        df_zone = con.execute("SELECT * FROM etl_daily_zone_revenue").fetchdf()
        df_margin = con.execute("SELECT * FROM etl_category_margin").fetchdf()
        con.close()

        # Save CSV reports
        os.makedirs(REPORTS_DIR, exist_ok=True)
        zone_csv = os.path.join(REPORTS_DIR, "etl_daily_zone_revenue_20261020.csv")
        margin_csv = os.path.join(REPORTS_DIR, "etl_category_margin_20261020.csv")
        df_zone.to_csv(zone_csv, index=False)
        df_margin.to_csv(margin_csv, index=False)

        # Print reports in log as required by task
        logger.info("=" * 95)
        logger.info("EXECUTIVE REPORT: DAILY NET REVENUE BY ZONE & STORE (etl_daily_zone_revenue)")
        logger.info("=" * 95)
        logger.info(df_zone.head(20).to_string(index=False))

        logger.info("=" * 95)
        logger.info("EXECUTIVE REPORT: REVENUE AND GROSS MARGIN BY CATEGORY (etl_category_margin)")
        logger.info("=" * 95)
        logger.info(df_margin.to_string(index=False))
        logger.info("=" * 95)

        return {
            "daily_zone_revenue_rows": len(df_zone),
            "category_margin_rows": len(df_margin),
            "total_delivered_revenue": float(df_margin["total_revenue"].sum()),
            "total_delivered_margin": float(df_margin["total_gross_margin"].sum()),
        }

    # ── Pipeline Task Execution Flow ───────────────────────────────────────
    c_raw = extract_customers()
    p_raw = extract_products()
    s_raw = extract_stores()
    o_raw = extract_orders()
    y_raw = extract_payments()

    val_out = validate_data(c_raw, p_raw, s_raw, o_raw, y_raw)

    quarantined = quarantine_failed(val_out)
    transformed = transform(val_out)
    loaded = load_to_warehouse(transformed)
    reconciled = reconcile_payments(transformed, y_raw)
    reported = report_metrics(loaded)

    if AIRFLOW_AVAILABLE:
        loaded >> [reconciled, reported]


# Instantiate the DAG
if AIRFLOW_AVAILABLE:
    nammamart_etl_dag = nammamart_etl_pipeline()
else:
    class DummyDag:
        dag_id = "nammamart_etl_jitendra"
        tags = ["nammamart", "etl", "extract", "jitendra"]
        default_args = default_args
    nammamart_etl_dag = DummyDag()


# ---------------------------------------------------------------------------
# Standalone CLI Execution Mode
# ---------------------------------------------------------------------------
def run_standalone():
    """Runs the complete ETL pipeline (Extract -> Validate -> Quarantine -> Transform -> Load -> Reconcile -> Report) standalone."""
    print("=" * 95)
    print("Running NammaMart End-to-End ETL Pipeline (Standalone Execution)")
    print("=" * 95)

    # 1. Extract 5 files
    print("[1/6] Extracting 5 files in parallel...")
    cust_df = pd.read_csv(os.path.join(DATA_DIR, "customers.csv"))
    prod_df = pd.read_csv(os.path.join(DATA_DIR, "products.csv"))
    store_df = pd.read_csv(os.path.join(DATA_DIR, "stores.csv"))
    ord_df = pd.read_csv(os.path.join(DATA_DIR, "orders.csv"))
    pay_df = pd.read_csv(os.path.join(DATA_DIR, "payments.csv"))
    print(f"  - customers.csv: {len(cust_df)} rows")
    print(f"  - products.csv:  {len(prod_df)} rows")
    print(f"  - stores.csv:    {len(store_df)} rows")
    print(f"  - orders.csv:    {len(ord_df)} rows")
    print(f"  - payments.csv:  {len(pay_df)} rows")

    # 2. Validate
    print("\n[2/6] Validating against 6 Data Quality dimensions (data_extract_rule.docx)...")
    run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    checked_at = datetime.now(timezone.utc).isoformat()

    cust_pass, cust_fail, cust_checks = validate_customers(cust_df, run_id, checked_at)
    prod_pass, prod_fail, prod_checks = validate_products(prod_df, run_id, checked_at)
    store_pass, store_fail, store_checks = validate_stores(store_df, run_id, checked_at)
    ord_pass, ord_fail, ord_checks = validate_orders(ord_df, cust_df, prod_df, store_df, run_id, checked_at)
    pay_pass, pay_fail, pay_checks = validate_payments(pay_df, ord_df, run_id, checked_at)

    all_checks = cust_checks + prod_checks + store_checks + ord_checks + pay_checks
    scorecard_df = build_scorecard_dataframe(all_checks, run_id, "nammamart_etl_jitendra", checked_at)

    print("\n--- VALIDATION RESULTS SUMMARY ---")
    print(f"Orders:    {len(ord_pass):>4} Passed | {len(ord_fail):>4} Failed (Total: {len(ord_df)})")
    print(f"Payments:  {len(pay_pass):>4} Passed | {len(pay_fail):>4} Failed (Total: {len(pay_df)})")
    print(f"Products:  {len(prod_pass):>4} Passed | {len(prod_fail):>4} Failed (Total: {len(prod_df)})")
    print(f"Customers: {len(cust_pass):>4} Passed | {len(cust_fail):>4} Failed (Total: {len(cust_df)})")
    print(f"Stores:    {len(store_pass):>4} Passed | {len(store_fail):>4} Failed (Total: {len(store_df)})")

    # 3. Quarantine
    print("\n[3/6] Writing quarantine records...")
    os.makedirs(QUARANTINE_DIR, exist_ok=True)
    parts = []
    for name, f_df, id_col in [
        ("orders.csv", ord_fail, "order_id"),
        ("payments.csv", pay_fail, "payment_id"),
        ("products.csv", prod_fail, "product_id"),
        ("customers.csv", cust_fail, "customer_id"),
        ("stores.csv", store_fail, "store_id"),
    ]:
        if not f_df.empty:
            p = f_df.copy()
            p["source_file"] = name
            p["record_id"] = p[id_col]
            parts.append(p)

    all_failed = pd.concat(parts, ignore_index=True)
    front_cols = ["source_file", "record_id", "failure_reason"]
    other_cols = [c for c in all_failed.columns if c not in front_cols]
    all_failed = all_failed[front_cols + other_cols]

    target_csv = os.path.join(QUARANTINE_DIR, "namamart_failed_20261020.csv")
    all_failed.to_csv(target_csv, index=False)
    mirror_csv = os.path.join(QUARANTINE_DIR, "nammamart_failed_20261020.csv")
    all_failed.to_csv(mirror_csv, index=False)

    ord_fail.to_csv(os.path.join(QUARANTINE_DIR, "orders_failed_20261020.csv"), index=False)
    pay_fail.to_csv(os.path.join(QUARANTINE_DIR, "payments_failed_20261020.csv"), index=False)
    prod_fail.to_csv(os.path.join(QUARANTINE_DIR, "products_failed_20261020.csv"), index=False)
    cust_fail.to_csv(os.path.join(QUARANTINE_DIR, "customers_failed_20261020.csv"), index=False)
    store_fail.to_csv(os.path.join(QUARANTINE_DIR, "stores_failed_20261020.csv"), index=False)

    scorecard_path = os.path.join(QUARANTINE_DIR, "dq_results_20261020.csv")
    scorecard_df.to_csv(scorecard_path, index=False)

    summary_path = os.path.join(QUARANTINE_DIR, "summary_20261020.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump({
            "total_checked": len(cust_df) + len(prod_df) + len(store_df) + len(ord_df) + len(pay_df),
            "total_passed": len(cust_pass) + len(prod_pass) + len(store_pass) + len(ord_pass) + len(pay_pass),
            "total_failed": len(all_failed),
            "breakdown": {
                "orders": {"total": len(ord_df), "passed": len(ord_pass), "failed": len(ord_fail)},
                "payments": {"total": len(pay_df), "passed": len(pay_pass), "failed": len(pay_fail)},
                "products": {"total": len(prod_df), "passed": len(prod_pass), "failed": len(prod_fail)},
                "customers": {"total": len(cust_df), "passed": len(cust_pass), "failed": len(cust_fail)},
                "stores": {"total": len(store_df), "passed": len(store_pass), "failed": len(store_fail)},
            }
        }, f, indent=2)

    print(f"  ✓ Quarantine file written: {target_csv} ({len(all_failed)} rows)")
    print(f"  ✓ DQ Scorecard written:    {scorecard_path}")

    # 4. Transform
    print("\n[4/6] Executing Transform on clean passed orders...")
    merged_orders, clean_cust, clean_prod, clean_store = transform_data(
        ord_pass, cust_df, prod_df, store_df
    )

    os.makedirs(TRANSFORMED_DIR, exist_ok=True)
    transformed_csv = os.path.join(TRANSFORMED_DIR, "transformed_orders_20261020.csv")
    merged_orders.to_csv(transformed_csv, index=False)

    clean_cust.to_csv(os.path.join(TRANSFORMED_DIR, "dim_customer_20261020.csv"), index=False)
    clean_prod.to_csv(os.path.join(TRANSFORMED_DIR, "dim_product_20261020.csv"), index=False)
    clean_store.to_csv(os.path.join(TRANSFORMED_DIR, "dim_store_20261020.csv"), index=False)

    print(f"  ✓ Transformed orders:      {len(merged_orders)} rows -> {transformed_csv}")
    print(f"  ✓ Deduplicated customers:  {len(clean_cust)} unique rows")
    print(f"  ✓ Deduplicated products:   {len(clean_prod)} unique rows")
    print(f"  ✓ Deduplicated stores:     {len(clean_store)} unique rows")

    # 5. Load into DuckDB Warehouse
    print("\n[5/6] Idempotently loading into DuckDB star schema...")
    now_ts = datetime.now(timezone.utc).isoformat()
    merged_orders["ingested_at"] = now_ts
    clean_cust["ingested_at"] = now_ts
    clean_prod["ingested_at"] = now_ts
    clean_store["ingested_at"] = now_ts

    fact_cols = [
        "order_id", "order_date", "order_ts", "customer_id", "store_id", "product_id",
        "quantity", "unit_price", "discount_pct", "order_amount", "net_revenue",
        "cost_price", "gross_margin", "payment_mode", "order_status", "channel",
        "delivery_minutes", "within_15_min", "ingested_at"
    ]
    fact_orders = merged_orders[fact_cols].copy()

    con = duckdb.connect(WAREHOUSE_DB)

    # 5a. Customer dimension
    con.execute("CREATE TABLE IF NOT EXISTS etl_dim_customer AS SELECT * FROM clean_cust WHERE 1=0")
    existing_c = set(con.execute("SELECT customer_id FROM etl_dim_customer").fetchdf()["customer_id"])
    new_c = clean_cust[~clean_cust["customer_id"].isin(existing_c)]
    if len(new_c) > 0:
        con.execute("INSERT INTO etl_dim_customer SELECT * FROM new_c")

    # 5b. Product dimension
    con.execute("CREATE TABLE IF NOT EXISTS etl_dim_product AS SELECT * FROM clean_prod WHERE 1=0")
    existing_p = set(con.execute("SELECT product_id FROM etl_dim_product").fetchdf()["product_id"])
    new_p = clean_prod[~clean_prod["product_id"].isin(existing_p)]
    if len(new_p) > 0:
        con.execute("INSERT INTO etl_dim_product SELECT * FROM new_p")

    # 5c. Store dimension
    con.execute("CREATE TABLE IF NOT EXISTS etl_dim_store AS SELECT * FROM clean_store WHERE 1=0")
    existing_s = set(con.execute("SELECT store_id FROM etl_dim_store").fetchdf()["store_id"])
    new_s = clean_store[~clean_store["store_id"].isin(existing_s)]
    if len(new_s) > 0:
        con.execute("INSERT INTO etl_dim_store SELECT * FROM new_s")

    # 5d. Fact orders
    con.execute("CREATE TABLE IF NOT EXISTS etl_fact_orders AS SELECT * FROM fact_orders WHERE 1=0")
    existing_o = set(con.execute("SELECT order_id FROM etl_fact_orders").fetchdf()["order_id"])
    new_o = fact_orders[~fact_orders["order_id"].isin(existing_o)]
    if len(new_o) > 0:
        con.execute("INSERT INTO etl_fact_orders SELECT * FROM new_o")

    # Scorecard in duckdb
    con.execute("CREATE TABLE IF NOT EXISTS dq_results AS SELECT * FROM scorecard_df WHERE 1=0")
    con.execute("INSERT INTO dq_results SELECT * FROM scorecard_df")

    cust_count = con.execute("SELECT COUNT(*) FROM etl_dim_customer").fetchone()[0]
    prod_count = con.execute("SELECT COUNT(*) FROM etl_dim_product").fetchone()[0]
    store_count = con.execute("SELECT COUNT(*) FROM etl_dim_store").fetchone()[0]
    fact_count = con.execute("SELECT COUNT(*) FROM etl_fact_orders").fetchone()[0]
    con.close()

    print(f"  ✓ DuckDB warehouse:        {WAREHOUSE_DB}")
    print(f"  ✓ etl_fact_orders:         {fact_count} rows ({len(new_o)} new appended)")
    print(f"  ✓ etl_dim_customer:        {cust_count} rows ({len(new_c)} new appended)")
    print(f"  ✓ etl_dim_product:         {prod_count} rows ({len(new_p)} new appended)")
    print(f"  ✓ etl_dim_store:           {store_count} rows ({len(new_s)} new appended)")

    # 6. Reconcile & Report
    print("\n[6/6] Reconciling payments & Generating executive reports...")
    recon_df = build_payment_reconciliation(merged_orders, pay_df)

    os.makedirs(RECON_DIR, exist_ok=True)
    recon_csv = os.path.join(RECON_DIR, "etl_payment_reconciliation_20261020.csv")
    recon_df.to_csv(recon_csv, index=False)

    con = duckdb.connect(WAREHOUSE_DB)
    con.execute("CREATE OR REPLACE TABLE etl_payment_reconciliation AS SELECT * FROM recon_df")

    con.execute("""
        CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
        SELECT
            f.order_date,
            COALESCE(s.zone, 'Unknown') AS zone,
            s.store_id,
            s.store_name,
            COUNT(f.order_id) AS total_orders,
            COUNT(CASE WHEN f.order_status = 'DELIVERED' THEN 1 END) AS delivered_orders,
            ROUND(SUM(CASE WHEN f.order_status = 'DELIVERED' THEN f.net_revenue ELSE 0 END), 2) AS daily_net_revenue
        FROM etl_fact_orders f
        JOIN etl_dim_store s ON f.store_id = s.store_id
        GROUP BY f.order_date, COALESCE(s.zone, 'Unknown'), s.store_id, s.store_name
        ORDER BY f.order_date, COALESCE(s.zone, 'Unknown'), s.store_name;
    """)

    con.execute("""
        CREATE OR REPLACE TABLE etl_category_margin AS
        SELECT
            COALESCE(p.category, 'Uncategorized') AS category,
            SUM(f.quantity) AS total_quantity_sold,
            ROUND(SUM(f.net_revenue), 2) AS total_revenue,
            ROUND(SUM(f.quantity * p.cost_price), 2) AS total_cost,
            ROUND(SUM(f.gross_margin), 2) AS total_gross_margin,
            ROUND(SUM(f.gross_margin) * 100.0 / NULLIF(SUM(f.net_revenue), 0), 2) AS margin_pct
        FROM etl_fact_orders f
        JOIN etl_dim_product p ON f.product_id = p.product_id
        WHERE f.order_status = 'DELIVERED'
        GROUP BY p.category
        ORDER BY total_revenue DESC;
    """)

    df_zone = con.execute("SELECT * FROM etl_daily_zone_revenue").fetchdf()
    df_margin = con.execute("SELECT * FROM etl_category_margin").fetchdf()
    recon_count = con.execute("SELECT COUNT(*) FROM etl_payment_reconciliation").fetchone()[0]
    con.close()

    os.makedirs(REPORTS_DIR, exist_ok=True)
    df_zone.to_csv(os.path.join(REPORTS_DIR, "etl_daily_zone_revenue_20261020.csv"), index=False)
    df_margin.to_csv(os.path.join(REPORTS_DIR, "etl_category_margin_20261020.csv"), index=False)

    print(f"  ✓ etl_payment_reconciliation: {recon_count} mismatches -> {recon_csv}")
    print(f"  ✓ etl_daily_zone_revenue:     {len(df_zone)} rows -> {os.path.join(REPORTS_DIR, 'etl_daily_zone_revenue_20261020.csv')}")
    print(f"  ✓ etl_category_margin:        {len(df_margin)} categories -> {os.path.join(REPORTS_DIR, 'etl_category_margin_20261020.csv')}")

    print("\n" + "=" * 95)
    print("EXECUTIVE REPORT: DAILY NET REVENUE BY ZONE & STORE (etl_daily_zone_revenue - Top 10)")
    print("=" * 95)
    print(df_zone.head(10).to_string(index=False))

    print("\n" + "=" * 95)
    print("EXECUTIVE REPORT: REVENUE AND GROSS MARGIN BY CATEGORY (etl_category_margin)")
    print("=" * 95)
    print(df_margin.to_string(index=False))
    print("=" * 95)

    print("\nDone! End-to-End ETL Pipeline (Extract -> Validate -> Quarantine -> Transform -> Load -> Reconcile -> Report) completed successfully.")


if __name__ == "__main__":
    run_standalone()
