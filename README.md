# pyspark-learning-repo
scenerio based questions with answers
Im preparing for big data interviews.

## Scenario: Using all PySpark save modes
**Question:** You receive a daily batch of orders and need to land results into a managed table. The pipeline must support strict production runs, safe replays, incremental adds, and full backfills. How would you demonstrate all save modes in PySpark?  

**Answer (scenario walkthrough):**  
You ingest a small batch, then apply each save mode to the same destination to illustrate how production, dev, and recovery behaviors differ.  

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("save-mode-scenario").getOrCreate()

daily_orders = [
    (101, "2024-01-10", 45.50),
    (102, "2024-01-10", 19.99),
]
df = spark.createDataFrame(daily_orders, ["order_id", "order_date", "amount"])

target_table = "sales.daily_orders"

# 1) ErrorIfExists (default): fail fast to prevent accidental overwrites in prod
df.write.mode("errorifexists").saveAsTable(target_table)

# 2) Ignore: reruns in dev/test skip the write if the table already exists
df.write.mode("ignore").saveAsTable(target_table)

# 3) Append: late-arriving orders are added without touching existing rows
df.write.mode("append").saveAsTable(target_table)

# 4) Overwrite: full reloads replace the table for backfills or schema fixes
df.write.mode("overwrite").saveAsTable(target_table)
```

**Why this covers all save modes:**
- **ErrorIfExists** protects production tables from accidental overwrites.  
- **Ignore** avoids duplicate writes during safe replays in lower environments.  
- **Append** supports incremental loads and late-arriving records.  
- **Overwrite** enables controlled backfills or full reprocessing.  

**Optional variation:** If you are writing to files instead of a table, swap `saveAsTable` with `.format("parquet").save("/mnt/sales/daily_orders")` (or another format) and keep the same `.mode(...)` calls to demonstrate the identical behaviors.  
