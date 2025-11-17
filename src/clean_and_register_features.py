
# comments

from pyspark.sql import SparkSession
from databricks.feature_store import FeatureStoreClient

spark = SparkSession.builder.getOrCreate()
fs = FeatureStoreClient()

# 1. Read raw table
raw_df = spark.table("workspace.feature_store_project.fct_claim")

# 2. Data Cleaning
df_cleaned = (
    raw_df
    .filter("claim_amount > 0")
    .dropDuplicates(["claim_id"])
)
spark_df = spark.createDataFrame(df_cleaned)

# Write the Spark DataFrame to Databricks table
spark_df.write.format("delta").mode("overwrite").saveAsTable("workspace.feature_store_project.cleaned_claims1")

# 3. Write clean table

# Register as Feature Table (Persistent)
fs = FeatureStoreClient()
df_cleaned = spark.read.table("workspace.feature_store_project.cleaned_claims")

fs.create_table(
    name="fct_claim_features_after_datacleanup",
    primary_keys=["clm_id"],  # Ensure this column exists
    df=df_cleaned,
    description="Claim-level features after cleaning: includes payment details, service duration, and derived metrics."
)


print("Feature Table Created & Updated Successfully")

# trigger CI
