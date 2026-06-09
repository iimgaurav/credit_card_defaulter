# Data Loading Issues — Credit Card Defaulter Analysis

**Date:** 2026-06-06  
**Issue:** Data not loading into Bronze and Silver layers  
**Environment:** Databricks workspace `dbc-f49d67a0-6a22.cloud.databricks.com`  
**Catalog:** `credit_card_dev`

---

## Executive Summary

The end-to-end pipeline is blocked at the **Bronze ingestion layer**. No Bronze tables have been created or populated, which means Silver and Gold layers are also empty. This document identifies all root causes and provides a prioritized fix plan.

---

## Issue #1: Bronze Ingestion Notebooks Never Executed

**Severity:** CRITICAL  
**Impact:** No data in Bronze, Silver, or Gold  
**Root Cause:** The ingestion notebooks exist in the codebase but have never been run inside the Databricks workspace.

**Evidence:**
- `credit_card_dev.bronze` schema exists but contains **0 tables** (confirmed via Unity Catalog API)
- All Bronze table row counts return 0 or FAIL (confirmed via SQL warehouse queries)
- Raw data IS present in `/Volumes/credit_card_dev/raw/landing` (confirmed via file upload)
- Notebooks exist locally but were not triggered in Databricks

**Fix:** Execute the bronze ingestion notebooks in the Databricks workspace.

---

## Issue #2: Notebook Execution Method Mismatch

**Severity:** CRITICAL  
**Impact:** Orchestrators fail silently, blocking pipeline execution  
**Root Cause:** The orchestrator notebooks use `dbutils.notebook.run()` with file system paths instead of Databricks workspace notebook paths.

**Affected Files:**
- `notebooks/01_ingestion/bronze_orchestrator.py`
- `notebooks/03_silver/silver_orchestrator.py`
- `notebooks/04_gold/gold_orchestrator.py`

**Problem Code:**
```python
# This FAILS because "silver_crm_customer" is not a valid Databricks notebook path
dbutils.notebook.run("silver_crm_customer", timeout_seconds=600)
```

**Why It Fails:**
- `dbutils.notebook.run()` expects a full Databricks workspace path like `/Users/k.gaurav653@gmail.com/credit_card_defaulter/notebooks/03_silver/silver_crm_customer`
- The current code passes only the notebook filename, which Databricks cannot resolve
- This causes the orchestrator to fail at the first task, preventing all downstream execution

**Fix Options:**
1. **Option A (Recommended):** Update orchestrator to use `%run` magic commands (Databricks-native)
2. **Option B:** Update paths to full workspace paths: `/Users/k.gaurav653@gmail.com/credit_card_defaulter/notebooks/03_silver/silver_crm_customer`
3. **Option C:** Use `databricks-cli` or Jobs API to trigger each notebook as a separate task

---

## Issue #3: Workspace Path Confusion (Backslashes vs Forward Slashes)

**Severity:** HIGH  
**Impact:** Notebooks uploaded but not discoverable via Jobs API  
**Root Cause:** When notebooks were uploaded via the REST API, Windows backslash paths (`\`) were used instead of forward slashes (`/`).

**Evidence:**
- Workspace listing shows: `/Users/k.gaurav653@gmail.com/credit_card_defaulter/00_utilities\config.py`
- Jobs API cannot resolve paths with backslashes
- Job creation failed with: `Unable to access the notebook "/Users/k.gaurav653@gmail.com/credit_card_defaulter/notebooks/00_utilities\verify_landing_data.py"`

**Why This Happens:**
- The upload script used `os.path.relpath()` which returns Windows-style paths with backslashes
- Databricks workspace API requires Unix-style forward slashes
- This creates a mismatch between uploaded paths and job references

**Fix:** Re-upload notebooks using forward slashes, or rename existing notebooks in the workspace.

---

## Issue #4: Bronze Ingestion Runner Not Used

**Severity:** HIGH  
**Impact:** No standardized ingestion process exists  
**Root Cause:** A fixed bronze ingestion runner (`bronze_ingestion_runner.py`) was created but never executed.

**Evidence:**
- File exists: `notebooks/01_ingestion/bronze_ingestion_runner.py`
- Not referenced in any job or orchestrator
- Original `bronze_orchestrator.py` calls individual ingestion notebooks, not the runner

**Why This Matters:**
- The runner consolidates all 11 ingestion streams into one executable notebook
- It includes checkpoint reset logic, validation, and error handling
- Without it, each table must be ingested individually

**Fix:** Execute `bronze_ingestion_runner.py` as the primary ingestion entry point.

---

## Issue #5: DQ Infrastructure Tables Not Initialized

**Severity:** MEDIUM  
**Impact:** Pipeline logging and data quality tracking unavailable  
**Root Cause:** DQ tables (pipeline_logs, dq_quarantine, dq_scores) were never created.

**Evidence:**
- `init_dq_tables.py` exists but was not run
- Pipeline logs table does not exist
- DQ quarantine tables do not exist
- Silver DQ scores table does not exist

**Why This Matters:**
- Without `pipeline_logs`, there is no audit trail of pipeline execution
- Without `dq_quarantine`, bad records cannot be tracked
- Without `dq_scores`, data quality cannot be monitored
- Silver layer DQ checks will fail if these tables don't exist

**Fix:** Run `notebooks/00_utilities/init_dq_tables.py` before running the pipeline.

---

## Issue #6: Auto Loader Checkpoint Issues

**Severity:** MEDIUM  
**Impact:** Bronze ingestion may fail or re-process old data  
**Root Cause:** Checkpoint directories may be missing, corrupted, or pointing to wrong locations.

**Evidence:**
- Checkpoint path configured as: `{LANDING_VOLUME}/_checkpoints/{table_name}`
- If checkpoints were never created, Auto Loader will attempt full re-ingestion
- If checkpoints are corrupted, ingestion will fail silently or throw errors

**Why This Matters:**
- Auto Loader uses checkpoints to track processed files
- Missing checkpoints = full re-ingestion (slow, expensive)
- Corrupted checkpoints = ingestion failures

**Fix:** Delete and recreate checkpoints before first run:
```python
dbutils.fs.rm("/Volumes/credit_card_dev/raw/landing/_checkpoints/", recurse=True)
```

---

## Issue #7: Silver Layer Blocked by Empty Bronze

**Severity:** CRITICAL  
**Impact:** No Silver data can be produced  
**Root Cause:** Silver transformations read from Bronze tables, which are empty/non-existent.

**Evidence:**
- Silver `customer_clean` reads from `bronze.crm_customer_master` and `bronze.crm_customer_address`
- If Bronze tables don't exist or are empty, Silver transformations will fail or produce empty results
- All Silver tables depend on Bronze being populated first

**Dependency Chain:**
```
bronze.crm_customer_master → silver.customer_clean
bronze.crm_customer_address → silver.customer_clean
bronze.card_details → silver.card_clean
bronze.card_status → silver.card_clean
bronze.txn_transactions → silver.transaction_clean
bronze.billing_statements → silver.statement_clean
bronze.billing_payments → silver.payment_clean
bronze.collections_defaults → silver.default_clean
bronze.collections_recovery → silver.recovery_clean

silver.customer_clean + silver.card_clean + silver.transaction_clean +
silver.statement_clean + silver.payment_clean + silver.default_clean +
silver.recovery_clean → silver.customer_360_view
```

**Fix:** Bronze must be fully populated before Silver can run.

---

## Issue #8: Gold Layer Blocked by Empty Silver

**Severity:** HIGH  
**Impact:** No dimensional model or analytics available  
**Root Cause:** Gold layer reads from Silver tables, which are empty.

**Evidence:**
- `gold.dim_customer` reads from `silver.customer_clean`
- `gold.dim_card` reads from `silver.card_clean`
- `gold.fact_transaction` reads from `silver.transaction_clean`
- All Gold tables depend on Silver being populated first

**Dependency Chain:**
```
silver.customer_clean → gold.dim_customer (SCD2)
silver.card_clean → gold.dim_card (SCD2)
bronze.dim_calendar → gold.dim_date
bronze.ref_* → gold.dim_geography

gold.dim_customer + gold.dim_card + gold.dim_date + gold.dim_geography
    → gold.fact_transaction
    → gold.fact_statement
    → gold.fact_default_analysis

gold.fact_* → gold.analytics_*
```

**Fix:** Silver must be fully populated before Gold can run.

---

## Issue #9: SCD2 Logic Requires Existing Tables

**Severity:** MEDIUM  
**Impact:** Gold dimension tables fail on first run  
**Root Cause:** The SCD2 merge logic in `gold_dim_customer.py` and `gold_dim_card.py` checks if tables exist before running.

**Problem Code:**
```python
table_exists = spark.catalog.tableExists(GOLD_DIM_CUSTOMER)
if not table_exists:
    # First load: insert all as current records
    ...
else:
    # Incremental SCD2: detect changes vs current records
    ...
```

**Why This Causes Issues:**
- On first run, `table_exists` returns False
- The code attempts to create and populate the table
- If the CREATE TABLE statement fails (e.g., permissions, schema issues), the entire Gold build fails
- Subsequent runs expect the table to exist with data

**Fix:** Ensure table creation succeeds on first run, or provide a full-refresh flag.

---

## Issue #10: Orchestrator Uses Wrong Execution Method

**Severity:** HIGH  
**Impact:** Sequential task execution fails  
**Root Cause:** `dbutils.notebook.run()` is synchronous but requires valid notebook paths, which don't exist.

**Affected Files:**
- `notebooks/01_ingestion/bronze_orchestrator.py`
- `notebooks/03_silver/silver_orchestrator.py`
- `notebooks/04_gold/gold_orchestrator.py`

**Problem Pattern:**
```python
# This will FAIL because the path is not a valid Databricks notebook
dbutils.notebook.run("silver_crm_customer", timeout_seconds=600)
```

**Why This Is Critical:**
- If one task fails, the entire orchestrator stops
- There is no retry logic
- There is no error recovery
- The orchestrator cannot proceed to subsequent tasks

**Fix:** Replace `dbutils.notebook.run()` with:
1. `%run` magic (for Databricks notebooks in the same workspace)
2. Or Jobs API task dependencies (for production)

---

## Issue #11: No Cluster/Compute Configuration

**Severity:** MEDIUM  
**Impact:** Cannot execute notebooks even if paths are correct  
**Root Cause:** No cluster configuration exists for running the notebooks.

**Evidence:**
- Jobs API requires either `existing_cluster_id` or `new_cluster` configuration
- The workspace only supports serverless compute
- No serverless compute configuration exists in the job definitions

**Why This Matters:**
- Without compute, notebooks cannot execute
- Serverless compute must be explicitly enabled
- Cluster configuration affects performance and cost

**Fix:** Configure serverless compute in job definitions or use DABs with compute resources.

---

## Issue #12: Missing DABs Resources

**Severity:** LOW  
**Impact:** Cannot deploy pipelines via `databricks bundle deploy`  
**Root Cause:** The `databricks.yml` references `resources/*.yml` but these files don't exist.

**Evidence:**
- `databricks.yml` line 9: `include: - resources/*.yml`
- No `resources/` directory found in project structure
- Pipeline and job definitions are missing

**Why This Matters:**
- DABs cannot deploy without resource definitions
- Jobs and pipelines cannot be scheduled
- CI/CD is broken

**Fix:** Create `resources/` directory with pipeline and job YAML definitions.

---

## Issue #13: Environment Variables Not Set

**Severity:** LOW  
**Impact:** Local scripts cannot upload data or connect to Databricks  
**Root Cause:** `DATABRICKS_HOST` and `DATABRICKS_TOKEN` environment variables are not set.

**Evidence:**
- `upload_to_volume.py` requires these variables
- Local diagnostic scripts require these variables
- The `.databrickscfg` file exists but is not being read by scripts

**Current State:**
- `.databrickscfg` file exists at `C:\Users\Navneet\.databrickscfg`
- Contains valid credentials
- But scripts use `os.environ[]` which doesn't read from `.databrickscfg`

**Fix:** Either:
1. Set environment variables before running scripts, OR
2. Modify scripts to read from `.databrickscfg`, OR
3. Use Databricks CLI authentication

---

## Issue #14: No Error Handling in Ingestion Notebooks

**Severity:** LOW  
**Impact:** Failures are silent or hard to debug  
**Root Cause:** Individual ingestion notebooks don't have robust error handling.

**Evidence:**
- `crm_customer.py` has basic try/except but no retry logic
- No alerting on failure
- No automatic recovery

**Why This Matters:**
- Silent failures are hard to detect
- No notification when ingestion fails
- Pipeline can be broken for days without notice

**Fix:** Add try/except blocks, logging, and alerting to all ingestion notebooks.

---

## Prioritized Fix Plan

### Priority 1: Execute Bronze Ingestion (BLOCKING)
**Action:** Run bronze ingestion in Databricks workspace
**Steps:**
1. Open Databricks workspace
2. Navigate to `/Users/k.gaurav653@gmail.com/credit_card_defaulter/00_utilities/verify_landing_data.py`
3. Attach to a cluster and run to verify landing data
4. Run `init_dq_tables.py` to create DQ infrastructure
5. Run `bronze_ingestion_runner.py` to ingest all source data

### Priority 2: Fix Orchestrator Paths (BLOCKING)
**Action:** Update orchestrator notebooks to use correct execution method
**Steps:**
1. Replace `dbutils.notebook.run("silver_crm_customer")` with `%run ./silver_crm_customer`
2. Or use full workspace paths: `/Users/k.gaurav653@gmail.com/credit_card_defaulter/notebooks/03_silver/silver_crm_customer`

### Priority 3: Fix Workspace Paths (BLOCKING)
**Action:** Re-upload notebooks with forward slashes
**Steps:**
1. Use the upload script with path normalization
2. Ensure all notebook paths use `/` not `\`
3. Verify in workspace that paths are correct

### Priority 4: Configure Compute (BLOCKING)
**Action:** Set up serverless compute for job execution
**Steps:**
1. Configure serverless compute in Databricks workspace
2. Update job definitions to use serverless
3. Test with a simple notebook execution

### Priority 5: Enable DQ and Monitoring (IMPORTANT)
**Action:** Initialize DQ tables and add monitoring
**Steps:**
1. Run `init_dq_tables.py`
2. Verify DQ tables are created
3. Run a test ingestion with DQ checks enabled

---

## Quick Diagnostic Commands

Run these in a Databricks notebook to verify current state:

```python
# 1. Check landing data
dbutils.fs.ls("/Volumes/credit_card_dev/raw/landing/")

# 2. Check bronze tables
spark.sql("SHOW TABLES IN credit_card_dev.bronze").show()

# 3. Check silver tables
spark.sql("SHOW TABLES IN credit_card_dev.silver").show()

# 4. Check gold tables
spark.sql("SHOW TABLES IN credit_card_dev.gold").show()

# 5. Check specific bronze table row counts
for tbl in ["crm_customer_master", "card_details", "txn_transactions"]:
    cnt = spark.sql(f"SELECT COUNT(*) FROM credit_card_dev.bronze.{tbl}").collect()[0][0]
    print(f"{tbl}: {cnt} rows")

# 6. Check DQ tables
spark.sql("SHOW TABLES IN credit_card_dev.bronze").filter("tableName = 'pipeline_logs'").show()
spark.sql("SHOW TABLES IN credit_card_dev.silver").filter("tableName = 'dq_scores'").show()
```

---

## Summary Table

| Issue | Severity | Blocking | Fix Complexity |
|-------|----------|----------|----------------|
| #1 Bronze never executed | CRITICAL | YES | Low - just run it |
| #2 Execution method mismatch | CRITICAL | YES | Medium - update code |
| #3 Workspace path confusion | HIGH | YES | Low - re-upload |
| #4 Runner not used | HIGH | NO | Low - use existing runner |
| #5 DQ tables not initialized | MEDIUM | NO | Low - run init script |
| #6 Checkpoint issues | MEDIUM | NO | Low - reset checkpoints |
| #7 Silver blocked by Bronze | CRITICAL | YES | Automatic - fix Bronze |
| #8 Gold blocked by Silver | HIGH | YES | Automatic - fix Silver |
| #9 SCD2 first-run logic | MEDIUM | NO | Low - test first run |
| #10 Orchestrator method | HIGH | YES | Medium - update code |
| #11 No compute config | MEDIUM | YES | Medium - configure serverless |
| #12 Missing DABs resources | LOW | NO | Medium - create YAMLs |
| #13 Env vars not set | LOW | NO | Low - set variables |
| #14 No error handling | LOW | NO | Medium - add try/except |

---

## Recommended Immediate Actions

1. **TODAY:** Run bronze ingestion manually in Databricks workspace
2. **TODAY:** Verify Bronze tables are populated
3. **THIS WEEK:** Fix orchestrator execution paths
4. **THIS WEEK:** Re-upload all notebooks with correct paths
5. **NEXT WEEK:** Set up DABs and automated scheduling

---

*End of Issues List*
