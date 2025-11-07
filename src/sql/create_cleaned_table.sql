CREATE TABLE IF NOT EXISTS covid_daily_cleaned (
    date DATE PRIMARY KEY,
    positive INTEGER,
    negative INTEGER,
    hospitalized_currently INTEGER,
    death INTEGER,
    total_test_results INTEGER,
    positivity_rate FLOAT,
    case_fatality_rate FLOAT
);