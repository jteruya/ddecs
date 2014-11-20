IF OBJECT_ID('ReportingDB.dbo.DimItems','U') IS NOT NULL
  DROP TABLE ReportingDB.dbo.DimItems

--===============================================================================================
-- Base data on the Item source records. 
-- * Upstream dependency on DimUsers. 
--
-- The following conditions are applied:
-- 1. Application must be related to a identified User (in the Dimension table)
--===============================================================================================

SELECT DISTINCT A.ItemId, A.ApplicationId, 
RTRIM(LTRIM(A.Name)) AS Name,
RTRIM(LTRIM(A.ShortName)) AS ShortName,
A.Address1,
A.Address2,
A.Suite,
A.City,
A.State,
A.ZipCode,
A.Country,
A.Phone,
A.FAX,
A.Email,
A.lattitude,
A.longitude
INTO ReportingDB.dbo.DimItems
FROM Ratings.dbo.Item A
JOIN (SELECT DISTINCT ApplicationId FROM ReportingDB.dbo.DimUsers) U ON A.ApplicationId = U.ApplicationId


