import requests
import datetime
from .utils import load_config, get_db_connection
from psycopg2.extras import execute_batch
import logging
from src.logging_config import setup_logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

setup_logging()
logger = logging.getLogger(__name__)

logger.info("Starting COVID data ingestion")
def fetch_covid_data(api_url):
    response = requests.get(api_url)
    response.raise_for_status()
    return response.json()

def parse_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%f").date()
    except ValueError:
        return datetime.datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S").date()

def format_record(record):
    """
    Convert API dates and ensure missing values become None.
    """
    # date_str = str(record.get("date"))  # e.g., 20210310
    # date = datetime.datetime.strptime(date_str, "%Y%m%d").date()
    # date_str = record.get("cdc_case_earliest_dt")  # e.g., "2020-10-25T00:00:00.000"
    # date = datetime.datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%f").date()


    return (
        parse_date(record.get("cdc_case_earliest_dt")),
        parse_date(record.get("cdc_report_dt")),
        parse_date(record.get("pos_spec_dt")),
        record.get("current_status"),
        record.get("sex"),
        record.get("age_group"),
        record.get("race_ethnicity_combined"),
        record.get("hosp_yn"),
        record.get("icu_yn"),
        record.get("death_yn"),
        record.get("medcond_yn")
    )

def insert_data(conn, records):
    sql = """
        INSERT INTO covid_daily (
            cdc_case_earliest_dt, cdc_report_dt, pos_spec_dt, current_status,
            sex, age_group, race_ethnicity_combined, hosp_yn, icu_yn, death_yn,medcond_yn
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (cdc_case_earliest_dt) DO UPDATE SET
            cdc_case_earliest_dt = EXCLUDED.cdc_case_earliest_dt,
            cdc_report_dt = EXCLUDED.cdc_report_dt,
            pos_spec_dt = EXCLUDED.pos_spec_dt,
            current_status = EXCLUDED.current_status,
            sex = EXCLUDED.sex,
            age_group = EXCLUDED.age_group,
            race_ethnicity_combined = EXCLUDED.race_ethnicity_combined,
            hosp_yn = EXCLUDED.hosp_yn,
            icu_yn = EXCLUDED.icu_yn,
            death_yn = EXCLUDED.death_yn,
            medcond_yn = EXCLUDED.medcond_yn
    """

    cur = conn.cursor()
    #cur.executemany(sql, records)
    execute_batch(cur, sql, records, page_size=100)
    conn.commit()
    cur.close()

def main():
    config = load_config("src/config/config.yaml")
    api_url = config["api_url"]

    print("Fetching data from API...")
    data = fetch_covid_data(api_url)

    print(f"Retrieved {len(data)} records. Formatting...")
    formatted = [format_record(record) for record in data]

    print("Connecting to Neon database...")
    conn = get_db_connection(config)

    print("Inserting data...")
    insert_data(conn, formatted)

    conn.close()
    print("ETL Complete! Data inserted into covid_daily table.")

if __name__ == "__main__":
    main()
