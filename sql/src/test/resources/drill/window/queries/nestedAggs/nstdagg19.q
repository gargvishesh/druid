SELECT c2, COUNT(AVG(c1)) OVER() FROM "tblWnulls.parquet" GROUP BY c2
