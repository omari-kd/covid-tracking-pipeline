import requests
import datetime
from utils import load_config, get_db_connection
from psycopg2.extras import execute_batch

def fetch_covid_data(api_url):
    response = requests.get(api_url)
    response.raise_for_status()
    return response.json()

def format_record(record):
    """
    Convert API dates and ensure missing values become None.
    """
    date_str = str(record.get("date"))  # e.g., 20210310
    date = datetime.datetime.strptime(date_str, "%Y%m%d").date()

    return (
        date,
        record.get("positive"),
        record.get("negative"),
        record.get("hospitalizedCurrently"),
        record.get("death"),
        record.get("totalTestResults"),
        record.get("dataQualityGrade")
    )

def insert_data(conn, records):
    sql = """
        INSERT INTO covid_daily (
            date, positive, negative, hospitalized_currently,
            death, total_test_results, data_quality_grade
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date) DO UPDATE SET
            positive = EXCLUDED.positive,
            negative = EXCLUDED.negative,
            hospitalized_currently = EXCLUDED.hospitalized_currently,
            death = EXCLUDED.death,
            total_test_results = EXCLUDED.total_test_results,
            data_quality_grade = EXCLUDED.data_quality_grade;
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
