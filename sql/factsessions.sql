DROP TABLE IF EXISTS EventCube.FactSessions;

--===================================================================================================
-- Source on Sessions Fact table and enforces that each session is tied to an identified user. 
-- * Upstream dependency on DimUsers.
--
-- Minor transformations:
-- 1. Binary Version identified through string parsing. 
-- 2. Device value translated from numeric code to string. 
-- 3. Device Type/OS translated from numeric code to string.
--===================================================================================================

CREATE TABLE EventCube.FactSessions AS
SELECT 
  S.Start_Date AS Timestamp, 
  S.Application_Id AS ApplicationId, 
  U.GlobalUserId, 
  S.User_Id,
  CASE WHEN S.Binary_Version IS NULL THEN 'v???' WHEN LEFT(S.Binary_Version,1) = 'v' THEN 'v' || RIGHT(LEFT(S.Binary_Version,4),3) ELSE 'v' || LEFT(S.Binary_Version,3) END BinaryVersion,
  CASE
    WHEN S.App_Type_Id = 1 THEN 'iPhone'
    WHEN S.App_Type_Id = 2 THEN 'iPad'
    WHEN S.App_Type_Id = 3 THEN 'Android'
    WHEN S.App_Type_Id = 4 THEN 'HTML5'
    WHEN S.App_Type_Id = 5 THEN 'WindowsPhone7'
    WHEN S.App_Type_Id = 6 THEN 'Blackberry'
    ELSE '???'
  END Device, 
  CASE WHEN S.App_Type_Id BETWEEN 1 AND 2 THEN 'iOS' WHEN S.App_Type_Id = 3 THEN 'Android' ELSE 'Other' END DeviceType,
  S.Device_Id AS DeviceId,
  S.App_Type_Id AS AppTypeId
FROM (SELECT Application_Id, User_Id, Start_Date, Binary_Version, App_Type_Id, Device_Id FROM PUBLIC.Fact_Sessions_Old WHERE User_Id IS NOT NULL) S
JOIN EventCube.DimUsers U ON S.User_Id = U.UserId;

CREATE INDEX ndx_ecs_factsessions_userid_binaryversion ON EventCube.FactSessions (User_Id, BinaryVersion);
CREATE INDEX ndx_ecs_factsessions_userid_apptypeid ON EventCube.FactSessions (User_Id, AppTypeId);
CREATE INDEX ndx_ecs_factsessions_userid ON EventCube.FactSessions (User_Id);



