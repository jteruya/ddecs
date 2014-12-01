IF OBJECT_ID('ReportingDB.dbo.DimTopics','U') IS NOT NULL
  DROP TABLE ReportingDB.dbo.DimTopics

--===============================================================================================
-- Base data on the Topic source records. 
-- * Upstream dependency on DimUsers. 
--
-- The following conditions are applied:
-- 1. Application must be related to a identified User (in the Dimension table)
--===============================================================================================

SELECT DISTINCT A.TopicId, A.ApplicationId, 
RTRIM(LTRIM(A.Name)) AS Name,
RTRIM(LTRIM(A.SEName)) AS ShortName,
CAST(A.Description AS VARCHAR(2000)) AS Description
INTO ReportingDB.dbo.DimTopics
FROM Ratings.dbo.Topic A
JOIN (SELECT DISTINCT ApplicationId FROM ReportingDB.dbo.DimUsers) U ON A.ApplicationId = U.ApplicationId