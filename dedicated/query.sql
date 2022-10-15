-- Ingest all Data
COPY INTO dbo.[stageTaxi]
FROM 'https://oszamora.dfs.core.windows.net/oszamora/taxiparquet/Year=*/Month=*/*.parquet'
WITH (
    FILE_TYPE = 'Parquet',
    AUTO_CREATE_TABLE = 'ON'
);

-- 12 minutes DWU500, 1.2 billion rows
-- Check data

SELECT TOP 10 * FROM dbo.[stageTaxi];

-- Insert into Hash distributed table
CREATE TABLE [dbo].[Taxi]
( 
	[vendorID] [varchar](100)  NULL,
	[tpepPickupDateTime] [datetime2](7)  NULL,
	[tpepDropoffDateTime] [datetime2](7)  NULL,
	[passengerCount] [int]  NULL,
	[tripDistance] [float]  NULL,
	[puLocationId] [varchar](100)  NULL,
	[doLocationId] [varchar](100)  NULL,
	[startLon] [float]  NULL,
	[startLat] [float]  NULL,
	[endLon] [float]  NULL,
	[endLat] [float]  NULL,
	[rateCodeId] [int]  NULL,
	[storeAndFwdFlag] [varchar](100)  NULL,
	[paymentType] [varchar](100)  NULL,
	[fareAmount] [float]  NULL,
	[extra] [float]  NULL,
	[mtaTax] [float]  NULL,
	[improvementSurcharge] [varchar](100)  NULL,
	[tipAmount] [float]  NULL,
	[tollsAmount] [float]  NULL,
	[totalAmount] [float]  NULL,
	[puYear] [int]  NULL,
	[puMonth] [int]  NULL
)
WITH
(
	DISTRIBUTION = HASH(tpepPickupDateTime), CLUSTERED COLUMNSTORE INDEX
);

INSERT INTO [dbo].[Taxi]
SELECT  * FROM [dbo].[stageTaxi];

-- Distributed Table created in 12:45

Create Statistics stat_stage_salesdata_Region on [dbo].[Taxi] (tpepPickupDateTime) WITH SAMPLE 15 PERCENT;  


SELECT MONTH([tpepPickupDateTime]) AS [month],
    CAST(SUM(totalAmount) AS MONEY) AS totalAmount,
    SUM(CAST(SUM(totalAmount) AS MONEY)) OVER(ORDER BY MONTH([tpepPickupDateTime])) AS cumulativeAmount,
    COUNT(totalAmount) AS totalTrips,
    SUM(COUNT(totalAmount)) OVER(ORDER BY MONTH([tpepPickupDateTime])) AS cumulativeTrips
FROM [dbo].[Taxi]
WHERE [tpepPickupDateTime] >= '1/1/2018'
    AND [tpepPickupDateTime] <= '12/31/2018'
GROUP BY MONTH([tpepPickupDateTime])
ORDER BY 1;


-- Ingest Year 2019 Data
-- DROP TABLE dbo.[stageTaxi2018];
COPY INTO dbo.[stageTaxi2018]
FROM 'https://oszamora.dfs.core.windows.net/oszamora/taxiparquet/Year=2018/Month=*/*.parquet'
WITH (
    FILE_TYPE = 'Parquet',
    AUTO_CREATE_TABLE = 'ON'
);

-- 1:09
-- Insert into Hash distributed table

-- DROP TABLE [dbo].[Taxi2018]
CREATE TABLE [dbo].[Taxi2018]
( 
    [date] DATE,
    [vendorID] [varchar](100)  NULL,
	[tpepPickupDateTime] [datetime2](7)  NULL,
	[tpepDropoffDateTime] [datetime2](7)  NULL,
	[passengerCount] [int]  NULL,
	[tripDistance] [float]  NULL,
	[puLocationId] [varchar](100)  NULL,
	[doLocationId] [varchar](100)  NULL,
	[startLon] [float]  NULL,
	[startLat] [float]  NULL,
	[endLon] [float]  NULL,
	[endLat] [float]  NULL,
	[rateCodeId] [int]  NULL,
	[storeAndFwdFlag] [varchar](100)  NULL,
	[paymentType] [varchar](100)  NULL,
	[fareAmount] [float]  NULL,
	[extra] [float]  NULL,
	[mtaTax] [float]  NULL,
	[improvementSurcharge] [varchar](100)  NULL,
	[tipAmount] [float]  NULL,
	[tollsAmount] [float]  NULL,
	[totalAmount] [float]  NULL,
	[puYear] [int]  NULL,
	[puMonth] [int]  NULL
)
WITH
(
	DISTRIBUTION = HASH(tpepPickupDateTime)
	,CLUSTERED COLUMNSTORE INDEX
);

INSERT INTO [dbo].[Taxi2018]
SELECT  CAST(tpepPickupDateTime AS DATE), * FROM [dbo].[stageTaxi2018];

Create Statistics stats_trip_tpepPickupDateTime on [dbo].[Taxi2018] (tpepPickupDateTime) WITH SAMPLE 15 PERCENT;  
Create Statistics stats_trip_date on [dbo].[Taxi2018] ([date]) WITH SAMPLE 15 PERCENT;  

-- Creating a Date Dimension

-- DROP TABLE dbo.Date_Dim;
CREATE TABLE dbo.Date_Dim
WITH
(
 DISTRIBUTION = REPLICATE
 ,CLUSTERED COLUMNSTORE INDEX
)
AS
WITH dates AS (
	SELECT TOP 15000
	DATEADD(day, ROW_NUMBER() OVER(ORDER BY GETDATE()) - 1, '1/1/2000') as [date]
	FROM sys.columns a1 CROSS JOIN sys.columns a2 CROSS JOIN sys.columns a3 CROSS JOIN sys.columns a4 CROSS JOIN sys.columns a5
),
dates_expanded AS (
  SELECT
    date,
    year(date) as year,
    month(date) as month,
    day(date) as day,
   DATEPART(dw,date) as day_of_week 
  FROM dates
)
SELECT
    CAST(date AS DATE) AS [date],
    CAST(year AS SMALLINT) AS [year],
    cast(month(date)/4 + 1 AS SMALLINT) as quarter,
    CAST(month AS SMALLINT) AS month,
    CAST(DATEPART(wk, date) AS SMALLINT) as week_of_year, 
    CAST(day AS SMALLINT) as day,
    CAST(day_of_week AS SMALLINT) as day_of_week,
	CAST(DATENAME(weekday, date) AS VARCHAR(64)) AS day_of_week_s,
	CAST(DATEPART(dayofyear, date) AS SMALLINT) as day_of_year,
    CAST(datediff(day,'1970-01-01',date) AS SMALLINT) as day_of_epoch,
	CAST(CASE WHEN day_of_week IN (1,7) THEN 1 ELSE 0 END AS BIT) AS weekend,
    CAST(CASE WHEN 
      ((month = 1 AND day = 1 AND day_of_week between 2 AND 6) OR (day_of_week = 2 AND month = 1 AND day BETWEEN 1 AND 3)) -- New Year's Day
      OR (month = 1 AND day_of_week = 2 AND day BETWEEN 15 AND 21) -- MLK Jr
      OR (month = 2 AND day_of_week = 2 AND day BETWEEN 15 AND 21) -- President's Day
      OR (month = 5 AND day_of_week = 2 AND day BETWEEN 25 AND 31) -- Memorial Day
      OR ((month = 7 AND day = 4 AND day_of_week between 2 AND 6) OR (day_of_week = 2 AND month = 7 AND day BETWEEN 4 AND 6)) -- Independence Day
      OR (month = 9 AND day_of_week = 2 AND day BETWEEN 1 AND 7) -- Labor Day
      OR ((month = 11 AND day = 11 AND day_of_week between 2 AND 6) OR (day_of_week = 2 AND month = 11 AND day BETWEEN 11 AND 13)) -- Veteran's Day
      OR (month = 11 AND day_of_week = 5 AND day BETWEEN 22 AND 28) -- Thanksgiving
      OR ((month = 12 AND day = 25 AND day_of_week between 1 AND 5) OR (day_of_week = 2 AND month = 12 AND day BETWEEN 25 AND 27)) -- Christmas
	THEN 1 ELSE 0 END AS BIT) as us_holiday
FROM dates_expanded


select 'create statistics stat_'+ convert(varchar(3), b.column_id)+' ON '+ c.name +'.'+ a.name+'('+b.name+') WITH FULLSCAN;'
from sys.tables a
	inner join sys.columns b on a.object_id = b.object_id
	inner join sys.schemas c on c.schema_id = a.schema_id
where a.NAME = 'date_dim'
	and c.name = 'dbo';


create statistics stat_1 ON dbo.Date_Dim(date) WITH FULLSCAN;
create statistics stat_2 ON dbo.Date_Dim(year) WITH FULLSCAN;
create statistics stat_3 ON dbo.Date_Dim(quarter) WITH FULLSCAN;
create statistics stat_4 ON dbo.Date_Dim(month) WITH FULLSCAN;
create statistics stat_5 ON dbo.Date_Dim(week_of_year) WITH FULLSCAN;
create statistics stat_6 ON dbo.Date_Dim(day) WITH FULLSCAN;
create statistics stat_7 ON dbo.Date_Dim(day_of_week) WITH FULLSCAN;
create statistics stat_8 ON dbo.Date_Dim(day_of_week_s) WITH FULLSCAN;
create statistics stat_9 ON dbo.Date_Dim(day_of_year) WITH FULLSCAN;
create statistics stat_10 ON dbo.Date_Dim(day_of_epoch) WITH FULLSCAN;
create statistics stat_11 ON dbo.Date_Dim(weekend) WITH FULLSCAN;
create statistics stat_12 ON dbo.Date_Dim(us_holiday) WITH FULLSCAN;


-- Use Date Dimension

-- Day of Week by Month
SELECT d.month,
    d.day_of_week,
    d.day_of_week_s AS dayname,
    CAST(SUM(totalAmount) AS MONEY) AS totalAmount,
    COUNT(totalAmount) AS totalTrips
FROM [dbo].[Taxi2018] t
    INNER JOIN dbo.date_dim d ON t.[date] = d.[date]
GROUP BY d.[month], d.day_of_week, d.day_of_week_s
ORDER BY 1,2;

-- By month

SELECT d.month,
    CAST(SUM(totalAmount) AS MONEY) AS totalAmount,
    COUNT(totalAmount) AS totalTrips
FROM [dbo].[Taxi2018] t
    INNER JOIN dbo.date_dim d ON t.[date] = d.[date]
GROUP BY d.[month]
ORDER BY 1;


SELECT TOP 100 * FROM [dbo].[Taxi2018];
