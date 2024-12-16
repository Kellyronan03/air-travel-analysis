from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("MostCommonDelayType").getOrCreate()

# Define the column names 
columns = ["Date", "Month", "DayOfWeek", "DepTime", "CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier",
           "FlightNum", "ActualElapsedTime", "CRSElapsedTime", "AirTime", "ArrDelay", "DepDelay", "Origin",
           "Dest", "Distance", "TaxiIn", "TaxiOut", "Cancelled", "CancellationCode", "Diverted", "CarrierDelay",
           "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay", "OriginAirport", "OriginCity",
           "OriginState", "DestAirport", "DestCity", "DestState", "Description", "ArrDelayFlag"]

# Load the CSV file without header and assign column names
df = spark.read.csv("input/final_flights.csv", header=False, inferSchema=True).toDF(*columns)

# Register DataFrame as a SQL temporary view
df.createOrReplaceTempView("flights")

# SQL query to find the most common delay type
most_common_delay_type = spark.sql("""
    SELECT 
        CASE 
            WHEN CarrierDelay > 0 THEN 'Carrier Delay' 
            WHEN WeatherDelay > 0 THEN 'Weather Delay' 
            WHEN NASDelay > 0 THEN 'NAS Delay' 
            WHEN SecurityDelay > 0 THEN 'Security Delay' 
            WHEN LateAircraftDelay > 0 THEN 'Late Aircraft Delay' 
            ELSE 'No Delay'
        END AS DelayType,
        COUNT(*) AS Count
    FROM flights
    WHERE CarrierDelay != -1000 
      AND WeatherDelay != -1000
      AND NASDelay != -1000
      AND SecurityDelay != -1000
      AND LateAircraftDelay != -1000
    GROUP BY DelayType
    ORDER BY Count DESC
""")

# Show the most common delay types
most_common_delay_type.show()

# Save the result as a CSV file
most_common_delay_type.write.csv("output/most_common_delay_type.csv", header=True, mode="overwrite")
