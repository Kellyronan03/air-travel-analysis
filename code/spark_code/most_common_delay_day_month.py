from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Initialize Spark session
spark = SparkSession.builder.appName("FlightDelaysAndCancellations").getOrCreate()

# Define the column names 
columns = ["Date", "Month", "DayOfWeek", "DepTime", "CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier",
           "FlightNum", "ActualElapsedTime", "CRSElapsedTime", "AirTime", "ArrDelay", "DepDelay", "Origin",
           "Dest", "Distance", "TaxiIn", "TaxiOut", "Cancelled", "CancellationCode", "Diverted", "CarrierDelay",
           "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay", "OriginAirport", "OriginCity",
           "OriginState", "DestAirport", "DestCity", "DestState", "Description", "ArrDelayFlag"]

# Load the CSV file without header and assign column names
df = spark.read.csv("input/final_flights.csv", header=False, inferSchema=True).toDF(*columns)

# Cancellations by day of the week
cancellations_by_day = df.filter(col("Cancelled") == 1) \
    .groupBy("DayOfWeek") \
    .agg(count("*").alias("TotalCancellations")) \
    .orderBy("DayOfWeek")

# Cancellations by month
cancellations_by_month = df.filter(col("Cancelled") == 1) \
    .groupBy("Month") \
    .agg(count("*").alias("TotalCancellations")) \
    .orderBy("Month")

# Show results
cancellations_by_day.show()
cancellations_by_month.show()

#Save the results to CSV
cancellations_by_day.write.csv("output/cancellations_by_day.csv", header=True, mode="overwrite")
cancellations_by_month.write.csv("output/cancellations_by_month.csv", header=True, mode="overwrite")
