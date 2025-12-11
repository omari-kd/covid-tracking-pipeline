import pandas as pd
from utils import load_config, get_db_connection
from psycopg2.extras import execute_batch
import numpy as np

def load_raw_data(conn):
    query = "SELECT * FROM covid_daily ORDER BY cdc_case_earliest_dt;"
    return pd.read_sql(query, conn)


def transform_data(df):
    # Replace missing values with 0 numeric values
    # df = df.fillna(0).infer_objects(copy=False)
    # df = df.fillna(0).infer_objects(copy=False)
    # Calculate metrics (avoid division errors)
    # df['positivity_rate'] = df['positive'] / df['total_test_results'].replace(0, pd.NA)
    # df['case_fatality_rate'] = df['death'] / df['positive'].replace(0, pd.NA)

    # Fill any NaN that appear from divisions with 0 again
    # df = df.fillna(0)
    # Replace NaN strings with None — NOT zero
    df = df.replace({np.nan: None})
    return df

def save_cleaned_data(conn, df):
    cur = conn.cursor()

    insert_sql = """
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

    # Explicitly pick only the columns that match the sql
    df = df[
        [
            "cdc_case_earliest_dt",
            "cdc_report_dt",
            "pos_spec_dt",
            "current_status",
            "sex",
            "age_group",
            "race_ethnicity_combined",
            "hosp_yn",
            "icu_yn",
            "death_yn",
            "medcond_yn"
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