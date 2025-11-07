CREATE TABLE IF NOT EXISTS covid_daily (
    date DATE PRIMARY KEY,
    positive INTEGER,
    negative INTEGER,
    hospitalized_currently INTEGER,
    death INTEGER,
    total_test_results INTEGER,
    data_quality_grade TEXT
);