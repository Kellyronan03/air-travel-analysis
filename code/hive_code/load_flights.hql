DROP TABLE IF EXISTS final_flight;

CREATE TABLE final_flight (
    Fllight_Date STRING,
    Month INT,
    DayOfWeek INT,
    DepTime INT,
    CRSDepTime INT,
    ArrTime INT,
    CRSArrTime INT,
    UniqueCarrier STRING,
    FlightNum INT,
    ActualElapsedTime INT,
    CRSElapsedTime INT,
    AirTime INT,
    ArrDelay INT,
    DepDelay INT,
    Origin STRING,
    Dest STRING,
    Distance INT,
    TaxiIn INT,
    TaxiOut INT,
    Cancelled INT,
    CancellationCode STRING,
    Diverted INT,
    CarrierDelay INT,
    WeatherDelay INT,
    NASDelay INT,
    SecurityDelay INT,
    LateAircraftDelay INT,
    OriginAirport STRING,
    OriginCity STRING,
    OriginState STRING,
    DestAirport STRING,
    DestCity STRING,
    DestState STRING,
    Description STRING,
    ArrDelayFlag STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/Users/ronankelly/hive/input/A2/final_flights.csv' INTO TABLE final_flight;


