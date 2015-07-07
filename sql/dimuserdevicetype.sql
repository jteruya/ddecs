--Pre-build aggregates we will use later
DROP TABLE IF EXISTS EventCube.Agg_Sessions_Per_User_AppTypeId;  
CREATE TABLE EventCube.Agg_Sessions_Per_User_AppTypeId AS SELECT S.User_Id AS UserId, S.AppTypeId, COUNT(*) AS Sessions FROM EventCube.FactSessions S GROUP BY S.User_Id, S.AppTypeId;

DROP TABLE IF EXISTS EventCube.Agg_User_PctSessions_byAppType;  
CREATE TABLE EventCube.Agg_User_PctSessions_byAppType AS
SELECT ua.UserId, ua.AppTypeId, (1.0 * ua.Sessions) / u.Sessions AS PctSessions
FROM EventCube.Agg_Sessions_Per_User_AppTypeId ua
JOIN EventCube.Agg_Sessions_Per_User u ON ua.UserId = u.UserId;

CREATE INDEX ndx_ecs_aggUserPctSessionsByAppType_userid ON EventCube.Agg_User_PctSessions_byAppType (UserId);  

--===================================================================================
-- Per User ID, identifies the most common Device Type used across all sessions. 
-- Should no version be identified, defaults to "???".
-- * Upstream dependency on DimUsers.
--===================================================================================
DROP TABLE IF EXISTS EventCube.DimUserDeviceType;

CREATE TABLE EventCube.DimUserDeviceType AS 
SELECT UserId, CASE WHEN Device IN ('iPhone','iPad') THEN 'iOS' WHEN Device = 'Android' THEN 'Android' ELSE 'Other' END DeviceType, Device
FROM
( SELECT DISTINCT UserId, LAST_VALUE(CASE
    WHEN AppTypeId = 1 THEN 'iPhone'
    WHEN AppTypeId = 2 THEN 'iPad'
    WHEN AppTypeId = 3 THEN 'Android'
    WHEN AppTypeId = 4 THEN 'HTML5'
    WHEN AppTypeId = 5 THEN 'WindowsPhone7'
    WHEN AppTypeId = 6 THEN 'Blackberry'
    ELSE '???' END
  )
  OVER (PARTITION BY UserId ORDER BY PctSessions ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) Device
  FROM EventCube.Agg_User_PctSessions_byAppType S
) S;