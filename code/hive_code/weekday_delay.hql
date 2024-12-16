INSERT OVERWRITE DIRECTORY 'output/most_common_day_for_delays'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
SELECT
    DayOfWeek,
    COUNT(*) AS TotalDelays
FROM 
    final_flight
WHERE 
    COALESCE(Cancelled, 0) = 0
GROUP BY 
    DayOfWeek
ORDER BY 
    DayOfWeek ASC;