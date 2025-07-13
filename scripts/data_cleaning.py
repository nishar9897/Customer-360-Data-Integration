from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, regexp_replace
from pyspark.sql.types import *

INPUT_BUCKET = "gs://raw_data_bronze/"
OUTPUT_BUCKET = "gs://cleaned_silverlayer/"

spark = SparkSession.builder.appName("Bootcamp3DataCleaning").getOrCreate()

def clean_and_save(filename, schema, key_cols, required_cols):
    print(f"Processing {filename}")
    df = spark.read.csv(INPUT_BUCKET + filename, header=True, schema=schema)
    df = df.dropDuplicates(key_cols)
    for c in df.columns:
        if df.schema[c].dataType == StringType():
            df = df.withColumn(c, trim(col(c)))
    if required_cols:
        df = df.dropna(subset=required_cols)
    df.write.mode('overwrite').csv(OUTPUT_BUCKET + filename.replace('.csv', '_cleaned.csv'), header=True)

# --- Customers ---
customers_schema = StructType([
    StructField("CustomerID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Email", StringType(), True),
    StructField("Address", StringType(), True),
])
clean_and_save("Customers.csv", customers_schema, ["CustomerID"], ["CustomerID", "Name", "Email"])

# --- Products ---
products_schema = StructType([
    StructField("ProductID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Price", StringType(), True),  # Clean up below
])
df_products = spark.read.csv(INPUT_BUCKET + "Products.csv", header=True, schema=products_schema)
df_products = df_products.dropDuplicates(["ProductID"])
for c in ["Name", "Category"]:
    df_products = df_products.withColumn(c, trim(col(c)))
df_products = df_products.withColumn("Price", regexp_replace(col("Price"), "[$,]", ""))
df_products = df_products.withColumn("Price", col("Price").cast("float"))
df_products = df_products.dropna(subset=["ProductID", "Name", "Category", "Price"])
df_products.write.mode('overwrite').csv(OUTPUT_BUCKET + "Products_cleaned.csv", header=True)

# --- Agents ---
agents_schema = StructType([
    StructField("AgentID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Department", StringType(), True),
    StructField("Shift", StringType(), True),
])
clean_and_save("Agents.csv", agents_schema, ["AgentID"], ["AgentID", "Name"])

# --- Stores ---
stores_schema = StructType([
    StructField("StoreID", IntegerType(), True),
    StructField("Location", StringType(), True),
    StructField("Manager", StringType(), True),
    StructField("OpenHours", StringType(), True),
])
clean_and_save("Stores.csv", stores_schema, ["StoreID"], ["StoreID", "Location"])

# --- OnlineTransactions ---
onlinetrans_schema = StructType([
    StructField("OrderID", IntegerType(), True),
    StructField("CustomerID", IntegerType(), True),
    StructField("ProductID", IntegerType(), True),
    StructField("DateTime", StringType(), True),
    StructField("PaymentMethod", StringType(), True),
    StructField("Amount", StringType(), True),  # Clean up below
    StructField("Status", StringType(), True),
])
df_online = spark.read.csv(INPUT_BUCKET + "OnlineTransactions.csv", header=True, schema=onlinetrans_schema)
df_online = df_online.dropDuplicates(["OrderID"])
df_online = df_online.withColumn("Amount", regexp_replace(col("Amount"), "[$,]", ""))
df_online = df_online.withColumn("Amount", col("Amount").cast("float"))
for c in ["PaymentMethod", "Status"]:
    df_online = df_online.withColumn(c, trim(col(c)))
df_online = df_online.dropna(subset=["OrderID", "CustomerID", "ProductID", "Amount"])
df_online.write.mode('overwrite').csv(OUTPUT_BUCKET + "OnlineTransactions_cleaned.csv", header=True)

# --- InStoreTransactions ---
instore_schema = StructType([
    StructField("TransactionID", IntegerType(), True),
    StructField("CustomerID", IntegerType(), True),
    StructField("StoreID", IntegerType(), True),
    StructField("DateTime", StringType(), True),
    StructField("Amount", StringType(), True),  # Clean up below
    StructField("PaymentMethod", StringType(), True),
])
df_instore = spark.read.csv(INPUT_BUCKET + "InStoreTransactions.csv", header=True, schema=instore_schema)
df_instore = df_instore.dropDuplicates(["TransactionID"])
df_instore = df_instore.withColumn("Amount", regexp_replace(col("Amount"), "[$,]", ""))
df_instore = df_instore.withColumn("Amount", col("Amount").cast("float"))
df_instore = df_instore.withColumn("PaymentMethod", trim(col("PaymentMethod")))
df_instore = df_instore.dropna(subset=["TransactionID", "CustomerID", "StoreID", "Amount"])
df_instore.write.mode('overwrite').csv(OUTPUT_BUCKET + "InStoreTransactions_cleaned.csv", header=True)

# --- CustomerServiceInteractions ---
csi_schema = StructType([
    StructField("InteractionID", IntegerType(), True),
    StructField("CustomerID", IntegerType(), True),
    StructField("DateTime", StringType(), True),
    StructField("AgentID", IntegerType(), True),
    StructField("IssueType", StringType(), True),
    StructField("ResolutionStatus", StringType(), True),
])
clean_and_save("CustomerServiceInteractions.csv", csi_schema, ["InteractionID"], ["InteractionID", "CustomerID", "AgentID"])

# --- LoyaltyAccounts ---
loyaltyacc_schema = StructType([
    StructField("LoyaltyID", IntegerType(), True),
    StructField("CustomerID", IntegerType(), True),
    StructField("PointsEarned", IntegerType(), True),
    StructField("TierLevel", StringType(), True),
    StructField("JoinDate", StringType(), True),
])
clean_and_save("LoyaltyAccounts.csv", loyaltyacc_schema, ["LoyaltyID"], ["LoyaltyID", "CustomerID"])

# --- LoyaltyTransactions ---
loyaltytrans_schema = StructType([
    StructField("LoyaltyID", IntegerType(), True),
    StructField("DateTime", StringType(), True),
    StructField("PointsChange", IntegerType(), True),
    StructField("Reason", StringType(), True),
])
clean_and_save("LoyaltyTransactions.csv", loyaltytrans_schema, ["LoyaltyID", "DateTime"], ["LoyaltyID", "DateTime"])

spark.stop()
