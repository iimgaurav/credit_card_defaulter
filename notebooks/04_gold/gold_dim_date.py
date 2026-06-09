# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — dim_date
# MAGIC **Source:** `bronze.dim_calendar`
# MAGIC **Target:** `gold.dim_date`
# MAGIC **Type:** Static Type 1 (full refresh)
# MAGIC **Grain:** 1 row per calendar day
# MAGIC **SK:** `date_sk` = YYYYMMDD integer

# COMMAND ----------

# MAGIC %run ../00_utilities/config

# COMMAND ----------

# MAGIC %run ../00_utilities/logger

# COMMAND ----------

from pyspark.sql import functions as F
import uuid

run_id = str(uuid.uuid4())
logger = PipelineLogger(spark, "gold_dim_date", run_id)

# COMMAND ----------

task_log = logger.start_task("build_dim_date")
try:
    calendar = spark.read.table(BRONZE_DIM_CALENDAR)

    HOLIDAYS = ["01-01", "01-26", "03-25", "04-14", "05-01", "08-15", "10-02", "10-24", "11-01", "12-25"]
    holiday_flag = F.when(
        F.array_contains(F.array(*[F.lit(h) for h in HOLIDAYS]), F.date_format("full_date", "MM-dd")),
        True
    ).otherwise(False)

    dim_date = (
        calendar
        .withColumnRenamed("date", "full_date")
        .withColumnRenamed("is_weekend", "is_weekend")
        .withColumn("date_sk", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn("day_of_month", F.dayofmonth("full_date"))
        .withColumn("day_of_year", F.dayofyear("full_date"))
        .withColumn("week_of_year", F.weekofyear("full_date"))
        .withColumn("month_number", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("year", F.year("full_date"))
        .withColumn("is_weekend", F.col("day_of_week").isin(1, 7))
        .withColumn("is_holiday", holiday_flag)
        .withColumn("fiscal_year",
            F.when(F.col("month_number") >= 4, F.col("year"))
             .otherwise(F.col("year") - 1))
        .withColumn("fiscal_quarter",
            F.when(F.col("month_number").between(4, 6), F.lit(1))
             .when(F.col("month_number").between(7, 9), F.lit(2))
             .when(F.col("month_number").between(10, 12), F.lit(3))
             .otherwise(F.lit(4)))
        .withColumn("_created_at", F.current_timestamp())
        .select("date_sk", "full_date", "day_of_week", "day_name",
                "day_of_month", "day_of_year", "week_of_year",
                "month_number", "month_name", "quarter", "year",
                "is_weekend", "is_holiday", "fiscal_year", "fiscal_quarter",
                "_created_at")
        .dropDuplicates(["date_sk"])
        .orderBy("date_sk")
    )

    dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_DIM_DATE)

    cnt = spark.read.table(GOLD_DIM_DATE).count()
    logger.complete_task("build_dim_date", task_log, row_count=cnt)
    print(f"dim_date: {cnt:,} rows | Run ID: {run_id}")
except Exception as e:
    logger.fail_task("build_dim_date", task_log, e)
    raise
