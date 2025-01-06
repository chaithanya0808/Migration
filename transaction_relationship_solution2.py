
from pyspark.sql import functions as F

# Explode the fcAlertTransactionRelationship array
result_df = df_to_ser.select(
    F.explode("value.fcAlertTransactionRelationship").alias("relationship")
).select(
    F.col("relationship.alertId").alias("alertId"),
    F.col("relationship.transactionId").alias("transactionId")
)

# Aggregate transactions per original message row
result_df_with_message = result_df.groupBy().agg(
    F.collect_list(F.struct("alertId", "transactionId")).alias("alert_transactions")
)

# Flatten transactions while keeping the original structure
result_df_flat = result_df_with_message.select(
    F.explode("alert_transactions").alias("alert_data")
).select(
    F.col("alert_data.alertId").alias("alertId"),
    F.col("alert_data.transactionId").alias("transactionId")
)

# Aggregate per Kafka message row without grouping by alertId
result_df_final = result_df_flat.groupBy("alertId", "transactionId").agg(
    F.collect_list("transactionId").alias("transactionIds"),
    F.count("transactionId").alias("txn_count")
).selectExpr(
    "alertId",
    "transactionIds",
    "txn_count",
    "map('alertId_transactionId', struct(alertId, transactionIds, txn_count)) as message"
)

# Display the result
result_df_final.select("message").show(truncate=False)
