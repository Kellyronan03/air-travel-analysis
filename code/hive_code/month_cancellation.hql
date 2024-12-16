INSERT OVERWRITE DIRECTORY 'output/most_common_month_for_cancellations'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
SELECT 
    Month,
    COUNT(*) AS TotalCancellations
FROM 
    final_flight
WHERE 
    Cancelled = 1
GROUP BY 
    Month
ORDER BY 
    Month ASC;
