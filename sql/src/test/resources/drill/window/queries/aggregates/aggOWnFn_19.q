SELECT COUNT(LEAD_c1) FROM (SELECT LEAD(c1) OVER(PARTITION BY c2 ORDER BY c1) LEAD_c1 , c2 FROM "tblWnulls.parquet") sub_query