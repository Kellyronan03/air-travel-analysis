INSERT OVERWRITE DIRECTORY 'output/average_delays'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
SELECT 
    Description AS Airline,
    AVG(DepDelay) AS AvgDepartureDelay,
    AVG(ArrDelay) AS AvgArrivalDelay
FROM 
    final_flight
WHERE 
    DepDelay != -1000 AND ArrDelay != -1000 
GROUP BY 
    Description
ORDER BY 
    Airline;
