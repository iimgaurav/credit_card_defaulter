# Databricks notebook source
# MAGIC %md
# MAGIC # Init Environment
# MAGIC Run this once per session to install the shared `src` package.
# MAGIC Called by bundle init script on cluster startup.

# COMMAND ----------

# MAGIC %pip install -e /Workspace/Users/k.gaurav653@gmail.com/.bundle/credit_card_defaulter/ce/files/src
# MAGIC # For local CE notebooks: %pip install -e /dbfs/FileStore/src

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify installation

# COMMAND ----------

try:
    import src.utils
    print("✅ src.utils package installed successfully")
    print(f"  Watermark:   {'✓' if hasattr(src.utils, 'Watermark') else '✗'}")
    print(f"  Logger:      {'✓' if hasattr(src.utils, 'PipelineLogger') else '✗'}")
    print(f"  DQ Framework: {'✓' if hasattr(src.utils, 'run_dq_suite') else '✗'}")
    print(f"  Config:      {'✓' if hasattr(src.utils, 'make_config') else '✗'}")
except ImportError as e:
    print(f"⚠️  src.utils not installed: {e}")
    print("Falling back to %run-style imports (notebook local scope)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cost Guardrails
# MAGIC
# MAGIC Sets cluster usage tags and warns if approaching the 2-hour CE runtime limit.

# COMMAND ----------

from src.utils.cost_guardrails import set_cluster_tags, check_runtime_limit

# Tag the cluster for cost tracking
set_cluster_tags(spark, project="credit_card_defaulter", purpose="pipeline")

# Check if we're approaching the 2-hour CE limit
try:
    check_runtime_limit(spark, max_hours=2)
except RuntimeError as e:
    print(f"⚠️  {e}")
    # Trigger graceful shutdown instead of hard kill
    dbutils.notebook.exit("RUNTIME_LIMIT_EXCEEDED")
