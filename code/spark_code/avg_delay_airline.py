from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# Initialize Spark session
spark = SparkSession.builder.appName("AverageDelays").getOrCreate()

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

# Run SQL query to calculate average delays by airline
average_delays = spark.sql("""
    SELECT Description AS Airline, 
           AVG(DepDelay) AS AvgDepartureDelay, 
           AVG(ArrDelay) AS AvgArrivalDelay
    FROM flights
    WHERE DepDelay != -1000 AND ArrDelay != -1000 -- Exclude cancelled flights
    GROUP BY Description
    ORDER BY Airline
""")

# Show results
average_delays.show()
average_delays.write.csv("output/average_delays.csv", header=True, mode="overwrite")
