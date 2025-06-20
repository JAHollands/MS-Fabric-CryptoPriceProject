# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "17321d11-e8eb-4b4c-b384-2accbef01f7f",
# META       "default_lakehouse_name": "LH_Crypto_Silver",
# META       "default_lakehouse_workspace_id": "ee0621a0-e142-4c39-98f0-e3ae8164f3f9",
# META       "known_lakehouses": [
# META         {
# META           "id": "17321d11-e8eb-4b4c-b384-2accbef01f7f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col, row_number, lit, max as spark_max
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Table names

silver_table = 'silver_coin_exchange_prices'
dim_coin_table = 'gold_dim_coin'
dim_exchange_table = 'gold_dim_exchange'
dim_pair_table = 'gold_dim_pair'
fact_table = 'gold_fact_coin_prices'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load silver data

df_silver = spark.read.table(silver_table)\
    .withColumn('price_date', col('date').cast('date'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get latest loaded price_date

try:
    df_fact_existing = spark.read.table(fact_table)
    latest_date = df_fact_existing.agg(spark_max('price_date')).collect()[0][0]
except:
    df_fact_existing = None
    latest_date = None

if latest_date:
    df_silver = df_silver.filter(col('price_date') > lit(latest_date))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load or init dim_coin
try:
    df_coin = spark.read.table(dim_coin_tbl)
    max_coin_key = df_coin.agg(spark_max("coin_key")).collect()[0][0] or 0
except:
    df_coin = spark.createDataFrame([], "coin_id STRING, coin_key INT")
    max_coin_key = 0

# Load or init dim_exchange
try:
    df_exchange = spark.read.table(dim_exchange_tbl)
    max_exchange_key = df_exchange.agg(spark_max("exchange_key")).collect()[0][0] or 0
except:
    df_exchange = spark.createDataFrame([], "exchange STRING, trust_score STRING, exchange_key INT")
    max_exchange_key = 0

# Load or init dim_pair
try:
    df_pair = spark.read.table(dim_pair_tbl)
    max_pair_key = df_pair.agg(spark_max("pair_key")).collect()[0][0] or 0
except:
    df_pair = spark.createDataFrame([], "base STRING, target STRING, pair_key INT")
    max_pair_key = 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Generate new dim rows
df_new_coin = df_silver.select("coin_id").distinct().join(df_coin, "coin_id", "left_anti") \
    .withColumn("coin_key", row_number().over(Window.orderBy("coin_id")) + max_coin_key)

df_new_exchange = df_silver.select("exchange", "trust_score").distinct().join(df_exchange, ["exchange", "trust_score"], "left_anti") \
    .withColumn("exchange_key", row_number().over(Window.orderBy("exchange", "trust_score")) + max_exchange_key)

df_new_pair = df_silver.select("base", "target").distinct().join(df_pair, ["base", "target"], "left_anti") \
    .withColumn("pair_key", row_number().over(Window.orderBy("base", "target")) + max_pair_key)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_coin_updated = df_coin.unionByName(df_new_coin)
df_coin_updated.write.mode("overwrite").format("delta").saveAsTable(dim_coin_table)

df_exchange_updated = df_exchange.unionByName(df_new_exchange)
df_exchange_updated.write.mode("overwrite").format("delta").saveAsTable(dim_exchange_table)

df_pair_updated = df_pair.unionByName(df_new_pair)
df_pair_updated.write.mode("overwrite").format("delta").saveAsTable(dim_pair_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact = df_silver \
    .join(df_coin_updated, "coin_id") \
    .join(df_exchange_updated, ["exchange", "trust_score"]) \
    .join(df_pair_updated, ["base", "target"]) \
    .select("coin_key", "exchange_key", "pair_key", "price_date", "last_price", "volume", "load_ts")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if df_fact_existing:
    DeltaTable.forName(spark, fact_table).alias("target") \
        .merge(
            df_fact.alias("source"),
            "target.coin_key = source.coin_key AND \
             target.exchange_key = source.exchange_key AND \
             target.pair_key = source.pair_key AND \
             target.price_date = source.price_date"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df_fact.write.mode("overwrite").format("delta").saveAsTable(fact_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
