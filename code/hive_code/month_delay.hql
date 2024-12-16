INSERT OVERWRITE DIRECTORY 'output/most_common_month_for_delays'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
SELECT
    Month,
    COUNT(*) AS TotalDelays
FROM 
    final_flight
WHERE 
    Cancelled = 0
GROUP BY 
    Month
ORDER BY 
    Month ASC;

