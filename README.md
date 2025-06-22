# MS-Fabric-CryptoPriceProject

This repository contains a Microsoft Fabric implementation that ingests, transforms, and models cryptocurrency pricing data from the CoinGecko API using Spark Notebooks and Lakehouse tables. The architecture follows a medallion pattern and supports incremental data processing and analytical consumption via Direct Lake in Power BI.

## Tools Used

- Microsoft Fabric Spark Notebooks (PySpark)
- Lakehouse storage (Delta format) in OneLake
- Data Pipelines for orchestration
- Power BI (Direct Lake mode)

## Architecture: Medallion Layers

### Bronze Layer: `LH_Crypto_Bronze`
- Stores raw API responses ingested from CoinGecko.
- Ingestion logic is implemented in `NB_Bronze_Data_Ingestion.Notebook`.

### Silver Layer: `LH_Crypto_Silver`
- Contains cleaned, deduplicated, and structured data.
- Table: `silver_coin_exchange_prices`
- Transformation logic is implemented in `NB_Silver.Notebook`.

### Gold Layer: `LH_Crypto_Gold`
- Implements a star schema with surrogate keys.
- Dimension tables:
  - `gold_dim_coin`
  - `gold_dim_exchange`
  - `gold_dim_pair`
- Fact table:
  - `gold_fact_coin_prices`
- Modeling logic is implemented in `NB_Gold_Modelling.Notebook`.

## Data Processing

### Ingestion
- Live data is fetched from CoinGecko.
- Data is normalized and written to the Bronze layer in Delta format.

### Transformation and Modeling
- Silver applies validation, enrichment, and business logic.
- Gold builds a relational star schema for reporting.
- Surrogate keys are generated in Spark notebooks.

## Orchestration

A Microsoft Fabric Data Pipeline (`DP_Crypto_Orch.DataPipeline`) coordinates:
1. Bronze ingestion
2. Silver transformation
3. Gold layer construction
4. Semantic model refresh (optional)

All stages support incremental processing.

## Output and Consumption

- Gold tables are served via the Lakehouse SQL endpoint.
- Power BI connects directly to the Lakehouse using Direct Lake mode.
- No Fabric Warehouse is used.
- Row-Level Security (RLS) is not currently implemented.

## How to Use

1. Clone this repository to your GitHub account.
2. In Microsoft Fabric, connect your workspace to the GitHub repo.
3. Sync the workspace with the repository contents.
4. Update all Lakehouse references in:
   - `NB_Bronze_Data_Ingestion.Notebook`
   - `NB_Silver.Notebook`
   - `NB_Gold_Modelling.Notebook`
   - `DP_Crypto_Orch.DataPipeline`
5. Run the pipeline or notebooks manually to create the initial tables.
6. Build the Power BI semantic model manually after the first run, once Gold tables exist.

## License

MIT License
