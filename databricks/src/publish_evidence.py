# Databricks notebook source
"""Export the run's metrics as an evidence record the OSS lane can read.

The pipeline event log is the only observability Free Edition offers: there is no
system.billing and no system.lakeflow without an account console. So the numbers this
notebook publishes are pipeline-level (rows written per table, expectation pass and fail
counts, run duration), and they are labelled as such rather than dressed up as cost.
"""

# COMMAND ----------
import json

catalog = spark.conf.get("samegold.catalog", "samegold")
pipeline_id = spark.conf.get("pipelines.id", "")

event_log = spark.sql(f"SELECT * FROM event_log('{pipeline_id}')")
expectations = spark.sql(f"""
SELECT explode(from_json(details:flow_progress.data_quality.expectations,
       'array<struct<name:string, dataset:string,
                     passed_records:bigint, failed_records:bigint>>')) AS e
FROM event_log('{pipeline_id}')
WHERE event_type = 'flow_progress'
""").selectExpr(
    "e.name AS name",
    "e.dataset AS dataset",
    "e.passed_records AS passed",
    "e.failed_records AS failed",
)

record = {
    "claim_id": "SG-09",
    "title": "the Databricks lane ran and its expectations reported",
    "runtime": "databricks-free",
    "expectations": [row.asDict() for row in expectations.collect()],
    "rows": {
        row["table"]: row["n"]
        for row in spark.sql(
            "SELECT 'revenue_by_month' AS table, COUNT(*) AS n "
            f"FROM {catalog}.main.revenue_by_month"
        ).collect()
    },
}
print(json.dumps(record, indent=2))
dbutils.jobs.taskValues.set("evidence", json.dumps(record))
