def generate_details_stats_df(input_df: DataFrame) -> DataFrame:
    """
    Generates a statistics DataFrame from the input DataFrame by selecting specific columns
    and calculating sizes of related relationships.
    :param input_df: Input PySpark DataFrame
    :return: A DataFrame with the required fields and statistics
    """
    # Select required fields directly
    interim_df = input_df.select(
        F.col("value.fcCase.caseId").alias("CASE_ID"),
        F.col("value.fcCase.caseVersion").alias("CASE_VERSION"),
        F.size(F.coalesce(F.col("value.fcCase.fcCasePartyRelationship"), F.array())).alias("PARTY_RELATIONSHIP_SIZE"),
        F.size(F.coalesce(F.col("value.fcCase.fcCaseCaseRelationship"), F.array())).alias("CASE_RELATIONSHIP_SIZE"),
        F.size(F.coalesce(F.col("value.fcCase.fcCaseAlertRelationship"), F.array())).alias("ALERT_RELATIONSHIP_SIZE"),
        F.col("value.metadata.correlationId").alias("correlation_Id")
    )
    
    # Generate the final DataFrame with stats column
    final_df = interim_df.select(
        F.create_map(
            F.lit("CASE_ID"), F.col("CASE_ID"),
            F.lit("CASE_VERSION"), F.col("CASE_VERSION"),
            F.lit("PARTY_RELATIONSHIP_SIZE"), F.col("PARTY_RELATIONSHIP_SIZE"),
            F.lit("CASE_RELATIONSHIP_SIZE"), F.col("CASE_RELATIONSHIP_SIZE"),
            F.lit("ALERT_RELATIONSHIP_SIZE"), F.col("ALERT_RELATIONSHIP_SIZE")
        ).alias("stats"),
        F.col("correlation_Id")
    )
    
    return final_df
