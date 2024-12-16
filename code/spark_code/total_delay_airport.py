from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, when
import pyspark.sql.functions as F
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import geopandas as gpd


# Initialize Spark session
spark = SparkSession.builder.appName("FlightAnalysis").getOrCreate()

# Define the column names 
columns = ["Date", "Month", "DayOfWeek", "DepTime", "CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier",
           "FlightNum", "ActualElapsedTime", "CRSElapsedTime", "AirTime", "ArrDelay", "DepDelay", "Origin",
           "Dest", "Distance", "TaxiIn", "TaxiOut", "Cancelled", "CancellationCode", "Diverted", "CarrierDelay",
           "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay", "OriginAirport", "OriginCity",
           "OriginState", "DestAirport", "DestCity", "DestState", "Description", "ArrDelayFlag"]

# Load the CSV file 
df = spark.read.csv("input/final_flights.csv", header=False, inferSchema=True).toDF(*columns)


# Calculate if a flight is delayed 
airport_delays = df.withColumn("Delayed", 
                               (col("ArrDelay") > 0) | (col("DepDelay") > 0))  # Delayed if either arrival or departure delay > 0

# Group by origin airport and calculate the total flights, delayed flights, and cancellations
totalDelays = airport_delays.groupBy("OriginState") \
    .agg(
        F.count("*").alias("TotalFlights"),  # Total number of flights
        F.sum(F.when(col("Delayed"), 1).otherwise(0)).alias("DelayedFlights"),  # Sum of delayed flights
        F.sum(F.when(col("Cancelled") == 1, 1).otherwise(0)).alias("CancelledFlights")  # Sum of cancelled flights
    ) \
    .withColumn("TotalDelays", col("DelayedFlights")) 

# Sort airports by Total delays
totalDelays = totalDelays.orderBy("TotalDelays", ascending=False)

# Show the results 
totalDelays.show()

#Save the results to a CSV file
totalDelays.write.csv("output/total_delay_airport.csv", header=True, mode="overwrite")

totalDelays_pd = totalDelays.select("OriginState", "TotalDelays").toPandas()

# Create a mapping dictionary for state abbreviations and full names
state_mapping = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "DC": "District of Columbia",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming"
}

# Map the abbreviations to full names in the pandas DataFrame
totalDelays_pd['FullStateName'] = totalDelays_pd['OriginState'].map(state_mapping)

# Load US states shapefile 
shapefile_path = "/Users/ronankelly/Desktop/tl_2024_us_state/tl_2024_us_state.shp"
states = gpd.read_file(shapefile_path)

# Reproject the GeoDataFrame to a suitable projection 
states = states.to_crs({'init': 'epsg:5070'})  

# Merge the DataFrames based on the full state name
states = states.merge(totalDelays_pd, left_on="NAME", right_on="FullStateName", how="left")

# Fill NaN flight counts with 0 for states without data
states['TotalDelays'] = states['TotalDelays'].fillna(0)

# Plot the choropleth map with adjusted figure size and colorbar
fig, ax = plt.subplots(figsize=(15, 10))  

states.plot(column="TotalDelays", cmap='OrRd', legend=True,
            legend_kwds={'label': 'TotalDelays'}, edgecolor='black', ax=ax)

# Add a title
plt.title("Total Airport Delays by State (2008)", fontsize=16, y=1.05)
plt.axis('off')

# Save the map as an image
plt.savefig("output/TotalDelays.jpeg", format='jpeg', dpi=300)

# Show the plot
plt.show()
