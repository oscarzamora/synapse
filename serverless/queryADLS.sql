-- Query for year 2019

SELECT
    TOP 100 *,
    r.filepath(1) AS [Year],
    r.filepath(2) AS [Month]
FROM
    OPENROWSET(
        BULK 'https://oszamora.dfs.core.windows.net/oszamora/taxiparquet/Year=*/Month=*/*.parquet',
        FORMAT = 'PARQUET'
    ) AS r
WHERE tpepPickupDateTime >= '1/1/2019'
    AND tpepPickupDateTime <= '12/31/2019'


-- Year 2019

USE [MyData];
-- Drop View taxiTrips
CREATE VIEW taxiTrips AS
SELECT
    tpepPickupDateTime AS PickupDateTime,
    tpepDropoffDateTime AS DropoffDateTime,
    passengerCount,
    tripDistance,
    paymentType,
    fareAmount,
    extra,
    mtaTax,
    improvementSurcharge,
    tipAmount,
    tollsAmount,
    totalAmount,
    r.filepath(1) AS [Year],
    r.filepath(2) AS [Month]
FROM
    OPENROWSET(
        BULK 'https://oszamora.dfs.core.windows.net/oszamora/taxiparquet/Year=*/Month=*/*.parquet',
        FORMAT = 'PARQUET'
    ) AS r
;

-- January 2019
SELECT COUNT(*) FROM taxiTrips
WHERE [Year] = 2019
    AND [Month] = 1;
-- 7667648

-- Aggregate for a Month
SELECT [Year], [Month], DAY(PickupDateTime) AS [Day], COUNT(1) AS totalTrips
FROM taxiTrips
WHERE [Year] = 2019
    AND [Month] = 1
GROUP BY [Year], [Month], DAY(PickupDateTime) 
ORDER BY 1,2,3;

-- CETAS Aggregate per month per year between 2009 and 2019

-- Aggregate by Day
-- Drop View taxiTripsAgg
CREATE VIEW taxiTripsAgg AS
SELECT
    CAST(PickupDateTime AS DATE) AS [day],
    SUM(passengerCount) AS passengerCount,
    SUM(tripDistance) AS tripDistance,
    SUM(fareAmount) AS fareAmount,
    SUM(extra + mtaTax + improvementSurcharge + tollsAmount) AS surcharges,
    SUM(tipAmount) AS tipAmount,
    SUM(totalAmount) AS totalAmount,
    COUNT(totalAmount) AS totalTrips
FROM taxiTrips
WHERE [Year] >= 2009
    AND [Year] <= 2019
GROUP BY CAST(PickupDateTime AS DATE)
;

SELECT * 
FROM taxiTripsAgg
ORDER BY 1;


-- DROP EXTERNAL DATA SOURCE Taxi
CREATE EXTERNAL DATA SOURCE Taxi
WITH ( LOCATION = 'https://oszamora.dfs.core.windows.net/oszamora/taxiparquet');

-- DROP EXTERNAL FILE FORMAT parquet
CREATE EXTERNAL FILE FORMAT parquet  
WITH (  
    FORMAT_TYPE = parquet 
    ); 

-- DROP EXTERNAL TABLE taxiTripsAggMonth
CREATE EXTERNAL TABLE taxiTripsAggMonth
WITH (
    LOCATION = 'taxiTripsAggMonth/',
    DATA_SOURCE = Taxi,  
    FILE_FORMAT = parquet
)  
AS
SELECT
    CAST(PickupDateTime AS DATE) AS [day],
    SUM(passengerCount) AS passengerCount,
    SUM(tripDistance) AS tripDistance,
    SUM(fareAmount) AS fareAmount,
    SUM(extra + mtaTax + improvementSurcharge + tollsAmount) AS surcharges,
    SUM(tipAmount) AS tipAmount,
    SUM(totalAmount) AS totalAmount,
    COUNT(totalAmount) AS totalTrips
FROM taxiTrips
WHERE [Year] >= 2009
    AND [Year] <= 2019
GROUP BY CAST(PickupDateTime AS DATE)
;


SELECT * FROM taxiTripsAggMonth;
