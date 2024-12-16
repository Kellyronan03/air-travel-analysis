from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, count, udf
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

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

# Group by destination state and count the number of flights
popular_states = df.groupBy("DestState") \
    .agg(count("*").alias("TotalFlights")) \
    .orderBy("TotalFlights", ascending=False)

# Show the top states
popular_states.show()

#Save the result to a CSV
popular_states.write.csv("output/popular_states.csv", header=True, mode="overwrite")

# Collect data to Pandas DataFrame for visualization
popular_states_pd = popular_states.select("DestState", "TotalFlights").toPandas()

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
popular_states_pd['FullStateName'] = popular_states_pd['DestState'].map(state_mapping)

# Load US states shapefile 
shapefile_path = "/Users/ronankelly/Desktop/tl_2024_us_state/tl_2024_us_state.shp"
states = gpd.read_file(shapefile_path)

# Reproject the GeoDataFrame to a suitable projection 
states = states.to_crs({'init': 'epsg:5070'}) 

# Merge the DataFrames based on the full state name
states = states.merge(popular_states_pd, left_on="NAME", right_on="FullStateName", how="left")

# Fill NaN flight counts with 0 for states without data
states['TotalFlights'] = states['TotalFlights'].fillna(0)

# Plot the choropleth map with adjusted figure size and colorbar
fig, ax = plt.subplots(figsize=(15, 10)) 

states.plot(column="TotalFlights", cmap='OrRd', legend=True,
            legend_kwds={'label': 'Total Flights'}, edgecolor='black', ax=ax)

# Add a title
plt.title("Most Popular US Flight Destinations by State (2008)", fontsize=16, y=1.05)
plt.axis('off')

# Save the map as an image
plt.savefig("output/popular_states_map.jpeg", format='jpeg', dpi=300)

# Show the plot
plt.show()