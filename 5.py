df_statistics = df_to_ser.select(
    F.expr("transform(value.fcAlertTransactionRelationship, x -> x.alertId)[0]").alias("alert_id"),  
    F.expr("value.fcAlertTransactionRelationship").alias("fc_alerted_transaction_relationship")
).withColumn(
    "transactions", F.expr("transform(fc_alerted_transaction_relationship, x -> x.transactionId)")
).withColumn(
    "txn_count", F.size(F.col("transactions"))
).select(
    "alert_id", "transactions", "txn_count"
)

# Display the result
df_statistics.show(1000, truncate=False)
df_statistics.count()
