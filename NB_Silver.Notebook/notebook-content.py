# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "99f23fd6-08cc-44d0-8bc3-a63a42125ba1",
# META       "default_lakehouse_name": "LH_Crypto_Bronze",
# META       "default_lakehouse_workspace_id": "ee0621a0-e142-4c39-98f0-e3ae8164f3f9",
# META       "known_lakehouses": [
# META         {
# META           "id": "99f23fd6-08cc-44d0-8bc3-a63a42125ba1"
# META         },
# META         {
# META           "id": "17321d11-e8eb-4b4c-b384-2accbef01f7f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Import required libraries
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Variables
bronze_table = "LH_Crypto_Bronze.bronze_coin_exchange_prices"
silver_table = "LH_Crypto_Silver.silver_coin_exchange_prices"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Find the last value loaded into the silver layer
try:
    latest_ts = spark.read.table(silver_table).agg(F.max('load_ts')).collect()[0][0]
except:
    latest_ts = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get only new records from the bronze table

if latest_ts:
    df_bronze = spark.read.table(bronze_table)\
        .filter(col('load_ts') > latest_ts)
else:
    df_bronze = spark.read.table(bronze_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Apply cleaning steps
df_clean = (
    df_bronze
    .filter(col("last_price").isNotNull() & col("volume").isNotNull())
    .withColumn("last_price", col("last_price").cast(DoubleType()))
    .withColumn("volume", col("volume").cast(DoubleType()))
    .dropDuplicates(["coin_id", "exchange", "base", "target", "timestamp"])
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Append to silver table
df_clean.write.format("delta") \
    .mode("append") \
    .saveAsTable(silver_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
