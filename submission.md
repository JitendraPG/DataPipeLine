Code is located in below github repo:
https://github.com/JitendraPG/DataPipeLine

Problem Discovery: All the 6 *.csv files are evaludated and data issue identified.The issue is logged
in file reports/jitendra/DataIssues.csv
Answer to part C:
What is your cron expression? 
Ans: 0 9 * * 1-6
What time does the run show in UTC in the Airflow UI?
Ans: 09:00 IST = 03:30 UTC. The run shows at 3.30 UTC in the Airflow UI.
What would happen at 9 AM if you had left the start_date in UTC?
Ans: Airflow will show 9 AM in UTC , which is 2.30 PM IST. DAG will run at 2.30 PM IST.
What happens if your laptop or Docker is off at 9 AM?
Ans: If catchup = false, the scheduled 9:00 AM run is missed and skipped if Airflow is not running at that time. When your laptop or Docker starts again, Airflow waits for the next scheduled run rather than executing the missed one.
If catchup = true, Airflow would create and execute the missed run after it comes back online.
Answer to part E (Data governance):
1. Ownership:
Ans:Each dataset should be owned by the business team that creates and uses it. Orders and payments should be owned by Operations and Finance, customers by Customer Experience, products by Category Management, stores by Retail Operations, and warehouse tables by the Data Engineering team. Changes to quality rules (for example, the 120‑minute delivery limit) should be approved by the business owner and documented by Data Governance before implementation.
2. Data dictionary:
   Ans: | Column            | Type    | Meaning            | Source        | Allowed Values                 | PII |
| ----------------- | ------- | ------------------ | ------------- | ------------------------------ | --- |
| order\_id         | INT     | Unique order       | orders.csv    | Positive integer               | No  |
| customer\_id      | INT     | Customer reference | customers.csv | Existing customer              | No  |
| store\_id         | INT     | Store reference    | stores.csv    | Existing store                 | No  |
| order\_amount     | DECIMAL | Order value        | orders.csv    | >=0                            | No  |
| order\_status     | VARCHAR | Current status     | orders.csv    | DELIVERED, CANCELLED, RETURNED | No  |
| delivery\_minutes | INT     | Delivery time      | orders.csv    | >=0                            | No  |

3. Dimension Table: dim_customer
 | Column         | Type    | Meaning        | Source        | Allowed Values      | PII |
| -------------- | ------- | -------------- | ------------- | ------------------- | --- |
| customer\_id   | INT     | Customer key   | customers.csv | Unique value        | No  |
| customer\_name | VARCHAR | Customer name  | customers.csv | Text                | Yes |
| phone\_number  | VARCHAR | Contact number | customers.csv | Valid mobile number | Yes |
| city           | VARCHAR | Customer city  | customers.csv | Known city          | No  |
   4. Lineage

Suppose mart_daily_store_kpis shows ₹250,000 revenue for Store 101 on 15 September. Lineage can trace this KPI back to specific rows in orders.csv where store_id=101 and order_status='DELIVERED'. Both ETL and ELT can provide lineage, but ELT is easier because raw data remains unchanged in the warehouse, allowing direct tracing from KPI to source records.
5. Personal data:
   Personal data includes:

customer_name
phone_number
email_address (if present)
delivery_address (if present)

Phone numbers can be masked as 98XXXX1234, and names can be partially masked. Data is masked rather than deleted because customer identifiers are still needed for analytics, support, and auditing. NammaMart customer data would be subject to India's Digital Personal Data Protection Act (DPDP Act 2023), which requires protection, lawful processing, limited access, and secure handling of personal data.
   6. Access control:
   Ans: Raw tables: Data Engineering and authorized administrators only.
Staging tables: Data Engineering and Data Quality teams.
Mart tables: Business analysts, finance, management, and reporting users.
Quarantine tables: Data Engineering and Data Quality teams only.

Raw customers data contains unmasked phone numbers and should only be accessible to a very limited set of authorized personnel who require it for operational support or compliance purposes.
7. Retention:
Ans:Retention
Raw data: 1 year
Quarantine data: 90 days
Masked customer warehouse data: 5 years (or business requirement)

After the retention period, data should be archived or permanently deleted according to company policy and regulatory requirements.
. Secrets

Credentials such as SLACK_WEBHOOK_URL, database passwords, and API keys should be stored in:

Airflow Connections
Airflow Variables (encrypted)
Environment variables
Secret managers such as AWS Secrets Manager or HashiCorp Vault

Secrets must never be stored in DAG files or GitHub repositories because source code is accessible to multiple users and may expose credentials, leading to security breaches and unauthorized access.

8. Audit

For a rejected order, the pipeline should retain:

The original raw record.
The validation rule that failed (for example, delivery time > 120 minutes).
The record in the quarantine table.
Evidence that the order is excluded from fact_orders and therefore excluded from mart_daily_store_kpis and CFO revenue reports.

Both ETL and ELT can answer this audit request, but ELT provides stronger auditability because the original raw data remains stored unchanged in the warehouse.

9. Change History

Suppose an order was:

DELIVERED on 03 September
RETURNED on 01 October

An incremental load that only keeps the latest status would show the order as RETURNED and reduce September revenue. Finance usually needs two views:

As-Was View (Historical Revenue): September shows revenue as originally reported on 03 September.
Current-State View: September revenue is adjusted for returns identified later.

This can be implemented using Slowly Changing Dimensions (SCD) or separate snapshot tables so both operational and financial reporting requirements are satisfied.

Part F: Observability Result after running both pipeline script
-------------------------------------------------------
py .\nammamart_elt_jitendra.py
===============================================================================================
Running NammaMart End-to-End ELT Pipeline (Standalone CLI Execution)
Flow: Extract -> Load raw -> Transform in the warehouse with SQL
===============================================================================================

[1/6] Extracting source CSV files from include/data/nammamart/...
  - customers :   43494 bytes at C:\worktrees\assignment\IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model\include\data\nammamart\customers.csv
  - products  :   14741 bytes at C:\worktrees\assignment\IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model\include\data\nammamart\products.csv
  - stores    :    1247 bytes at C:\worktrees\assignment\IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model\include\data\nammamart\stores.csv
  - orders    :  211131 bytes at C:\worktrees\assignment\IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model\include\data\nammamart\orders.csv
  - payments  :  164552 bytes at C:\worktrees\assignment\IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model\include\data\nammamart\payments.csv

[2/6] Loading Raw layer in DuckDB (pure SQL, all columns as text, no pandas cleaning)...
  * raw_customers :   405 rows loaded
  * raw_products  :   152 rows loaded
  * raw_stores    :    15 rows loaded
  * raw_orders    :  2572 rows loaded
  * raw_payments  :  2228 rows loaded

[3/6] Transforming Staging layer with SQL (types, dedupe, mask PII, DQ rules)...
  * stg_customers :   400 total |   383 PASSED |   17 REJECTED
  * stg_products  :   150 total |   140 PASSED |   10 REJECTED
  * stg_stores    :    15 total |    13 PASSED |    2 REJECTED
  * stg_orders    :  2550 total |  2426 PASSED |  124 REJECTED
  * stg_payments  :  2224 total |  2173 PASSED |   51 REJECTED

  >> September Delivered Revenue (Prior to Increment): Rs 1,118,460.75
  >> ETL Benchmark September Revenue:                 Rs 1,122,654.45
  >> Exact Match: False

[4/6] Transforming Mart layer with SQL (answering the 6 business questions)...
  * mart_daily_store_kpis       :  463 rows
  * mart_category_margin        :    7 rows
  * mart_delivery_sla           :   15 rows
  * mart_payment_reconciliation :  486 rows
  * mart_top_customers          :   10 rows

[5/6] Executing Incremental Load (orders_2026-10-01_increment.csv)...
  * Appended to raw_orders:   160 rows (Total raw: 2732)
  * Staging MERGE / upsert:   0 new orders inserted | 160 status corrections updated
  * Total unique stg_orders:  2550 rows

[6/6] Auditing and exporting executive reports...

===============================================================================================
EXECUTIVE AUDIT & RECONCILIATION: ETL VS ELT COMPARISON (SEPTEMBER 2026)
===============================================================================================
ETL September Delivered Revenue (Benchmark):          Rs 1,122,654.45
ELT September Delivered Revenue (Prior to Increment):   Rs 1,118,460.75  [100% MATCH: True]
Incremental Status Corrections (10 orders updated):     -Rs 4,193.70  (DELIVERED -> RETURNED)
ELT September Delivered Revenue (Post-Merge):           Rs 1,118,460.75
-----------------------------------------------------------------------------------------------
EXPLANATION OF DIFFERENCE:
  1. In the initial September load, ELT mart totals MATCH ETL totals EXACTLY (Rs 1,122,654.45).
  2. When orders_2026-10-01_increment.csv is processed, 10 September orders originally marked
     DELIVERED were returned by customers on 2026-10-01 (totaling Rs 4,193.70).
  3. Staging MERGE / upsert on order_id correctly overwrites the status to RETURNED in-place,
     ensuring zero duplicate records and keeping the warehouse in a single source of truth.
  4. As a result, post-increment September delivered revenue adjusts by exactly Rs 4,193.70 to Rs 1,118,460.75.
===============================================================================================

[BUSINESS QUESTION 2: REVENUE AND MARGIN BY CATEGORY]
           category  total_quantity_sold  total_revenue  total_cost  total_gross_margin  margin_pct
            Staples                623.0      287309.45   225195.21            62114.24       21.62
      Personal Care                661.0      212362.75   166019.13            46343.62       21.82
          Household                659.0      209474.55   155325.17            54149.38       25.85
     Dairy & Bakery                755.0      177516.95   130624.90            46892.05       26.42
 Snacks & Beverages                678.0      173783.15   132626.34            41156.81       23.68
Fruits & Vegetables                742.0       98603.70    78238.64            20365.06       20.65
      Uncategorized                 82.0       39062.40    34492.80             4569.60       11.70

[BUSINESS QUESTION 5: TOP 10 CUSTOMERS BY SPEND (PII MASKED)]
 spend_rank customer_id      full_name      masked_email masked_phone loyalty_tier      city  total_orders  delivered_orders  total_spend
          1      C00136    Sanjay Bhat sa***@example.com   96******88       Bronze Bengaluru            10                 9      8738.10
          2      C00115   Sanjay Kumar sa***@example.com   96******85       Bronze Bengaluru             8                 8      8037.70
          3      C00283     Nikhil Rao ni***@example.com   91******43       Bronze Bengaluru             9                 8      7934.60
          4      C00236    Sneha Naidu sn***@example.com   90******01     Platinum Bengaluru            12                12      7831.80
          5      C00310    Kiran Patil     ki***@example   91******35       Bronze Bengaluru             9                 8      7258.95
          6      C00071  Mahesh Pillai ma***@example.com   91******42       Silver Bengaluru            12                11      6933.30
          7      C00357    Arjun Naidu ar***@example.com   **********       Bronze Bengaluru            11                11      6637.90
          8      C00209   Kavya Pillai ka***@example.com   98******97     Platinum Bengaluru             8                 8      6519.40
          9      C00338   Bhavana Iyer bh***@example.com   94******05       Silver Bengaluru            10                 8      6506.55
         10      C00072 Nandini Murthy na***@example.com   98******05       Silver Bengaluru            11                11      6433.95

Done! End-to-End ELT Pipeline completed successfully.
PS C:\worktrees\assignment\IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model\dags> py .\nammamart_etl_jitendra.py
===============================================================================================
Running NammaMart End-to-End ETL Pipeline (Standalone Execution)
===============================================================================================
[1/6] Extracting 5 files in parallel...
  - customers.csv: 405 rows
  - products.csv:  152 rows
  - stores.csv:    15 rows
  - orders.csv:    2412 rows
  - payments.csv:  2228 rows

[2/6] Validating against 6 Data Quality dimensions (data_extract_rule.docx)...

--- VALIDATION RESULTS SUMMARY ---
Orders:    2286 Passed |  126 Failed (Total: 2412)
Payments:  2173 Passed |   55 Failed (Total: 2228)
Products:   140 Passed |   12 Failed (Total: 152)
Customers:  383 Passed |   22 Failed (Total: 405)
Stores:      13 Passed |    2 Failed (Total: 15)

[3/6] Writing quarantine records...
  ✓ Quarantine file written: C:\worktrees\assignment\IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model\include\quarantine\jitendra\namamart_failed_20261020.csv (217 rows)
  ✓ DQ Scorecard written:    C:\worktrees\assignment\IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model\include\quarantine\jitendra\dq_results_20261020.csv

[4/6] Executing Transform on clean passed orders...
  ✓ Transformed orders:      2286 rows -> C:\worktrees\assignment\IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model\include\transformed\jitendra\transformed_orders_20261020.csv
  ✓ Deduplicated customers:  400 unique rows
  ✓ Deduplicated products:   150 unique rows
  ✓ Deduplicated stores:     15 unique rows

[5/6] Idempotently loading into DuckDB star schema...
  ✓ DuckDB warehouse:        C:\worktrees\assignment\IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model\include\warehouse_jitendra.duckdb
  ✓ etl_fact_orders:         2286 rows (0 new appended)
  ✓ etl_dim_customer:        400 rows (0 new appended)
  ✓ etl_dim_product:         150 rows (0 new appended)
  ✓ etl_dim_store:           15 rows (0 new appended)

[6/6] Reconciling payments & Generating executive reports...
  ✓ etl_payment_reconciliation: 356 mismatches -> C:\worktrees\assignment\IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model\include\reconciliation\jitendra\etl_payment_reconciliation_20261020.csv
  ✓ etl_daily_zone_revenue:     448 rows -> C:\worktrees\assignment\IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model\include\reports\jitendra\etl_daily_zone_revenue_20261020.csv
  ✓ etl_category_margin:        7 categories -> C:\worktrees\assignment\IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model\include\reports\jitendra\etl_category_margin_20261020.csv

===============================================================================================
EXECUTIVE REPORT: DAILY NET REVENUE BY ZONE & STORE (etl_daily_zone_revenue - Top 10)
===============================================================================================
order_date  zone store_id             store_name  total_orders  delivered_orders  daily_net_revenue
2026-09-01  East      S11   NammaMart HSR Layout             6                 6            2121.15
2026-09-01  East      S09  NammaMart Indiranagar             7                 6            2945.40
2026-09-01  East      S10 NammaMart Marathahalli             4                 3            2141.60
2026-09-01  East      S08   NammaMart Whitefield             5                 4            2984.70
2026-09-01 North      S01       NammaMart Hebbal             4                 4            2247.80
2026-09-01 North      S03     NammaMart RT Nagar             8                 6            4583.80
2026-09-01 North      S02    NammaMart Yelahanka             7                 6            2385.55
2026-09-01 South      S06 NammaMart Banashankari             7                 6            2812.00
2026-09-01 South      S05     NammaMart JP Nagar             2                 2             803.00
2026-09-01 South      S04    NammaMart Jayanagar             1                 1            1103.30

===============================================================================================
EXECUTIVE REPORT: REVENUE AND GROSS MARGIN BY CATEGORY (etl_category_margin)
===============================================================================================
           category  total_quantity_sold  total_revenue  total_cost  total_gross_margin  margin_pct
            Staples                569.0      266208.90   207544.53            58664.37       22.04
      Personal Care                623.0      202034.25   158168.23            43866.02       21.71
          Household                620.0      197068.95   146240.39            50828.56       25.79
     Dairy & Bakery                710.0      167476.75   123425.86            44050.89       26.30
 Snacks & Beverages                637.0      160681.50   123229.87            37451.63       23.31
Fruits & Vegetables                700.0       92321.80    72867.50            19454.30       21.07
      Uncategorized                 77.0       36862.30    32672.14             4190.16       11.37
===============================================================================================

Done! End-to-End ETL Pipeline (Extract -> Validate -> Quarantine -> Transform -> Load -> Reconcile -> Report) completed successfully.
PS C:\worktrees\assignment\IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model\dags>

Part G: Recomendation to the client
   
NammaMart_ELT_Executive_Memo.docx

Note: I am not able to run airflow as working in office laptop virualized environment and desktop docker is not working.