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
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Import required libraries

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from datetime import datetime
import requests

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Params
# - Parameters from control pipeline

# MARKDOWN ********************

# # Create variables
# - records, empty list to store values
# - load_ts, runtime datetime value to store

# CELL ********************

coin_list = ['bitcoin', 'ethereum']
records = []
load_ts = datetime.utcnow().isoformat()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Ingestion
# - API call to coin gecko, looping through selected coins (bitcoin and ethereum)
# - For each coin, parse the response to return ticker level pricing data for multiple exchanges
# - Flatten each ticker into a dictionary of values (coin_id, exchange, base etc...)
# - Append all parsed tickers into a list

# CELL ********************

for coin_id in coin_list:
    r = requests.get(f"https://api.coingecko.com/api/v3/coins/{coin_id}/tickers")
    for t in r.json().get("tickers", []):
        records.append({
            "coin_id": coin_id,
            "exchange": t["market"]["name"],
            "base": t["base"],
            "target": t["target"],
            "last_price": t["last"],
            "volume": t["volume"],
            "trust_score": t["trust_score"],
            "timestamp": t["timestamp"],
            "load_ts": load_ts,
            "date": load_ts.split("T")[0]
        })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Define schema for spark dataframe
# - Define schema to convert our list into a spark dataframe

# CELL ********************

# Define Spark schema
schema = StructType([
    StructField("coin_id", StringType(), True),
    StructField("exchange", StringType(), True),
    StructField("base", StringType(), True),
    StructField("target", StringType(), True),
    StructField("last_price", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("trust_score", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("load_ts", StringType(), True),
    StructField("date", StringType(), True),
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Create a spark dataframe
# - Visual inspection of data whilst developing extraction process
# - Writing to delta table in our bronze lakehouse

# CELL ********************

# Create Spark DataFrame directly
df = spark.createDataFrame(records, schema=schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Append data to our Bronze delta table
# - Partition date by date, resulting in even partitions for each ingestion

# CELL ********************

df.write.format('delta').mode('append').partitionBy('date').save('Tables/bronze_coin_exchange_prices')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
