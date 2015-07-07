DROP TABLE IF EXISTS EventCube.DimItems;

--===============================================================================================
-- Base data on the Item source records. 
-- * Upstream dependency on DimUsers. 
--
-- The following conditions are applied:
-- 1. Application must be related to a identified User (in the Dimension table)
--===============================================================================================

CREATE TABLE EventCube.DimItems AS
SELECT 
A.ItemId, 
A.ApplicationId, 
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
A.Lattitude,
A.Longitude
FROM PUBLIC.Ratings_Item A
JOIN (SELECT DISTINCT ApplicationId FROM EventCube.DimUsers) U ON A.ApplicationId = U.ApplicationId;