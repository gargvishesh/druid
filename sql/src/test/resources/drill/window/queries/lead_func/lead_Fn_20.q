SELECT LEAD(col1 ) OVER ( PARTITION BY col2 ORDER BY col0 nulls LAST ) LEAD_col1 FROM "fewRowsAllData.parquet"