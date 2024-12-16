-- Load the data
flights = LOAD 'input/A2/semi-processed.csv' USING PigStorage(',') AS (
    Year:int, Month:int, DayofMonth:int, DayOfWeek:int, DepTime:int,
    CRSDepTime:int, ArrTime:int, CRSArrTime:int, UniqueCarrier:chararray, 
    FlightNum:int, ActualElapsedTime:int, 
    CRSElapsedTime:int, AirTime:int, ArrDelay:int, DepDelay:int, 
    Origin:chararray, Dest:chararray, Distance:int, TaxiIn:int, 
    TaxiOut:int, Cancelled:int, CancellationCode:chararray, Diverted:int, 
    CarrierDelay:int, WeatherDelay:int, NASDelay:int, SecurityDelay:int, 
    LateAircraftDelay:int, OriginAirport:chararray, OriginCity:chararray, 
    OriginState:chararray, DestAirport:chararray, DestCity:chararray, DestState:chararray, Description:chararray);

flights_filled = FOREACH flights GENERATE
    Year, Month, DayofMonth, DayOfWeek,
    (DepTime IS NOT NULL ? DepTime : -1) AS DepTime, CRSDepTime,
    (ArrTime IS NOT NULL ? ArrTime : -1) AS ArrTime, CRSArrTime, UniqueCarrier, FlightNum,
    (ActualElapsedTime IS NOT NULL ? ActualElapsedTime : -1) AS ActualElapsedTime,
    (CRSElapsedTime IS NOT NULL ? CRSElapsedTime : -1) AS CRSElapsedTime,
    (AirTime IS NOT NULL ? AirTime : -1) AS AirTime,
    (ArrDelay IS NOT NULL ? ArrDelay : -1000) AS ArrDelay,
    (DepDelay IS NOT NULL ? DepDelay : -1000) AS DepDelay, Origin, Dest, Distance,
    (TaxiIn IS NOT NULL ? TaxiIn : -1) AS TaxiIn,
    (TaxiOut IS NOT NULL ? TaxiOut : -1) AS TaxiOut,  Cancelled,
    (CancellationCode IS NOT NULL ? CancellationCode : 'N/A') AS CancellationCode, Diverted,
    (CarrierDelay IS NOT NULL ? CarrierDelay : -1) AS CarrierDelay,
    (WeatherDelay IS NOT NULL ? WeatherDelay : -1) AS WeatherDelay,
    (NASDelay IS NOT NULL ? NASDelay : -1) AS NASDelay,
    (SecurityDelay IS NOT NULL ? SecurityDelay : -1) AS SecurityDelay,
    (LateAircraftDelay IS NOT NULL ? LateAircraftDelay : -1) AS LateAircraftDelay,
    OriginAirport, OriginCity,OriginState, DestAirport, DestCity, DestState, Description;


flights_deduplicated = DISTINCT flights_filled;

flights_with_delay_flag = FOREACH flights_deduplicated GENERATE *, 
                            (CASE 
                                WHEN ArrDelay == -1000 THEN 'Cancelled' 
                                WHEN ArrDelay == 0 THEN 'On-time' 
                                WHEN ArrDelay < 0 THEN 'Early' 
                                WHEN ArrDelay > 0 THEN 'Delayed' 
                                ELSE 'Unknown' 
                             END) AS ArrDelayFlag;

flights_with_date = FOREACH flights_with_delay_flag GENERATE 
                        *,
                        CONCAT(
                            CONCAT((chararray)Year, '-'),
                            CONCAT(
                                (Month < 10 ? CONCAT('0', (chararray)Month) : (chararray)Month),
                                CONCAT('-', 
                                    (DayofMonth < 10 ? CONCAT('0', (chararray)DayofMonth) : (chararray)DayofMonth)
                                )
                            )
                        ) AS Date;

flights_final = FOREACH flights_with_date GENERATE 
    Date, Month, DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime,
    UniqueCarrier,FlightNum, ActualElapsedTime, CRSElapsedTime, AirTime, ArrDelay,DepDelay, Origin, Dest, Distance,
    TaxiIn, TaxiOut,Cancelled, CancellationCode, Diverted, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay, 
    LateAircraftDelay, OriginAirport,OriginCity, OriginState, DestAirport, DestCity, DestState, Description, ArrDelayFlag;


-- Output the cleaned and formatted data
STORE flights_final INTO 'output/final_flight' USING PigStorage(',');
