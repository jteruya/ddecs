IF OBJECT_ID('ReportingDB.dbo.FactSessions') IS NOT NULL
  DROP TABLE ReportingDB.dbo.FactSessions

--===================================================================================================
-- Source on Sessions Fact table and enforces that each session is tied to an identified user. 
-- * Upstream dependency on DimUsers.
--
-- Minor transformations:
-- 1. Binary Version identified through string parsing. 
-- 2. Device value translated from numeric code to string. 
-- 3. Device Type/OS translated from numeric code to string.
--===================================================================================================

SELECT DISTINCT StartDate Timestamp, S.ApplicationId, GlobalUserId, S.UserId,
CASE WHEN BinaryVersion IS NULL THEN 'v???' WHEN LEFT(BinaryVersion,1) = 'v' THEN 'v'+RIGHT(LEFT(BinaryVersion,4),3) ELSE 'v'+LEFT(BinaryVersion,3) END BinaryVersion,
CASE
  WHEN AppTypeId = 1 THEN 'iPhone'
  WHEN AppTypeId = 2 THEN 'iPad'
  WHEN AppTypeId = 3 THEN 'Android'
  WHEN AppTypeId = 4 THEN 'HTML5'
  WHEN AppTypeId = 5 THEN 'WindowsPhone7'
  WHEN AppTypeId = 6 THEN 'Blackberry'
  ELSE '???'
END Device, CASE WHEN AppTypeId BETWEEN 1 AND 2 THEN 'iOS' WHEN AppTypeId = 3 THEN 'Android' ELSE 'Other' END DeviceType
INTO ReportingDB.dbo.FactSessions
FROM AnalyticsDB.dbo.Sessions S
JOIN ReportingDB.dbo.DimUsers U ON S.UserId = U.UserId
WHERE S.UserId IS NOT NULL

