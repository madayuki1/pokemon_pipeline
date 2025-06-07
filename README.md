PokeAPI ETL Pipeline with Medallion Architecture

An end-to-end, Dockerized data engineering project that extracts complex Pokémon data from the PokeAPI, normalizes it using a Medallion architecture (Bronze/Silver/Gold), and generates analytical reports to uncover patterns in types, stats, and abilities.

This project demonstrate key skills in moderm data engineering:
- **ETL orchestration** with *Apache Airflow*  
- **File-based lakehouse** architecture using *Parquet* and *DuckDB*  
- **Normalization of nested API JSON** into structured tables  
- **Analytic summary layers** and **visualizations** for actionable insights

Project Architecture Overview

![PokeAPI Architecture](https://github.com/user-attachments/assets/7fb982a0-037c-473c-8f95-42a98f1e39f9)

Key Features: 
- **Raw ingestion** of nested PokeAPI JSON into Bronze layer  
- **Normalization** into Silver dimension tables  
- **Medallion layering** for clean separation of concerns  
- Fast local processing with **DuckDB + Pandas**  
- Automated **Matplotlib** visualizations  
- Modular code—easy to extend and test

How to Run  
```bash
# 1. Clone repo
git clone https://github.com/madayuki1/pokemon_pipeline.git
cd pokemon-pipeline

# 2. Build & start services
docker-compose up --build

# 3. Open Airflow
#    http://localhost:8080
#    username & password: airflow
#    Trigger the DAG named `pokeapi_etl`

# 4. Find Parquet outputs under data/{bronze,silver,gold}/
#    and charts under data/reports
