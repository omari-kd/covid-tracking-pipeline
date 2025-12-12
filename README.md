# COVID Tracking ETL Pipeline

This project is an automated **ETL pipeline** that ingests COVID-19 county-level case surveillance data from the CDC, transforms it into analysis-ready tables and loads it into a **PostgreSQL (Neon)** data warehouse. The pipeline is designed for reproducibility, modularity and automation through GitHub Actions.

## Data Source

**CDC COVID-19 Case Surveillance Data**  
`https://data.cdc.gov/resource/vbim-akqf.json`

This endpoint provides case-level data for confirmed and probable COVID-19 cases across U.S. counties.

---

## 📁 Project Structure

├── .github/workflows/
│ └── daily_etl.yml # GitHub Actions workflow (scheduled ingestion)
├── logs/
│ └── etl.log # Runtime logs
├── notebooks/
│ └── exploratory_analysis.ipynb
├── scripts/
│ └── run_daily.sh # Shell wrapper to run ETL locally
├── src/
│ ├── config/
│ │ └── config.yaml # Pipeline configuration (API, DB credentials)
│ ├── etl/
│ │ ├── ingest_covid.py # Extract & load raw CDC data
│ │ ├── transform_covid.py # Transform cleaned dataset
│ │ └── utils.py # Shared helpers (logging, db connections)
│ └── sql/
│ ├── create_tables.sql # Raw + staging table definitions
│ └── create_cleaned_tables.sql # Final analytics tables
├── .gitignore
├── README.md
├── requirements.txt
└── setup_db.py # Initializes Neon database schema
