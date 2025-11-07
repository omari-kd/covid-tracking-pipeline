import pandas as pd
from utils import load_config, get_db_connection
from psycopg2.extras import execute_batch
import numpy as np

def load_raw_data(conn):
    query = "SELECT * FROM covid_daily ORDER BY date;"
    return pd.read_sql(query, conn)


def transform_data(df):
    # Replace missing values with 0 numeric values
    # df = df.fillna(0).infer_objects(copy=False)
    df = df.fillna(0).infer_objects(copy=False)
    # Calculate metrics (avoid division errors)
    df['positivity_rate'] = df['positive'] / df['total_test_results'].replace(0, pd.NA)
    df['case_fatality_rate'] = df['death'] / df['positive'].replace(0, pd.NA)

    # Fill any NaN that appear from divisions with 0 again
    df = df.fillna(0)
    return df

def save_cleaned_data(conn, df):
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO covid_daily_cleaned (
        date, positive, negative, hospitalized_currently, death,
        total_test_results, positivity_rate, case_fatality_rate
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (date) DO UPDATE SET
            positive = EXCLUDED.positive,
            negative = EXCLUDED.negative,
            hospitalized_currently = EXCLUDED.hospitalized_currently,
            death = EXCLUDED.death,
            total_test_results = EXCLUDED.total_test_results,
            positivity_rate = EXCLUDED.positivity_rate,
            case_fatality_rate = EXCLUDED.case_fatality_rate;
    """

    # Explicitly pick only the columns that match the sql
    df = df[
        [
            "date",
            "positive",
            "negative",
            "hospitalized_currently",
            "death",
            "total_test_results",
            "positivity_rate",
            "case_fatality_rate",
        ]
    ]
     # Convert dataframe to list of tuples with Python-native types and None for NaN
    rows = []
    for row in df.to_numpy():
        converted = []
        for v in row:
            if pd.isna(v):
                converted.append(None)
            elif isinstance(v, (np.integer,)):
                converted.append(int(v))
            elif isinstance(v, (np.floating,)):
                converted.append(float(v))
            else:
                converted.append(v)
        rows.append(tuple(converted))

    # records = df.to_records(index=False)
    execute_batch(cur, insert_sql, rows, page_size=100)
    conn.commit()
    cur.close()


def main():
    config = load_config("src/config/config.yaml")
    conn = get_db_connection(config)

    print("Loading raw covid data...")
    df_raw = load_raw_data(conn)

    print("Transforming data...")
    df_clean = transform_data(df_raw)

    print("Saving cleaned data...")
    save_cleaned_data(conn, df_clean)

    conn.close()
    print("Transformation Complete! Cleaned data is ready.")

if __name__ == "__main__":
    main()