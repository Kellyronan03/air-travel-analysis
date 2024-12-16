INSERT OVERWRITE DIRECTORY 'output/most_common_day_for_cancellations'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
SELECT 
    DayOfWeek,
    COUNT(*) AS TotalCancellations
FROM 
    final_flight
WHERE 
    Cancelled = 1
GROUP BY 
    DayOfWeek
ORDER BY 
    DayOfWeek ASC;
