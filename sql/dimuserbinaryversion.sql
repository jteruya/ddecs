--Pre-build aggregates we will use later
DROP TABLE IF EXISTS EventCube.Agg_Sessions_Per_User;  
CREATE TABLE EventCube.Agg_Sessions_Per_User AS SELECT S.User_Id AS UserId, COUNT(*) AS Sessions FROM EventCube.FactSessions S GROUP BY S.User_Id;

DROP TABLE IF EXISTS EventCube.Agg_Sessions_Per_User_BinaryVersion;  
CREATE TABLE EventCube.Agg_Sessions_Per_User_BinaryVersion AS SELECT S.User_Id AS UserId, S.BinaryVersion, COUNT(*) AS Sessions FROM EventCube.FactSessions S GROUP BY S.User_Id, S.BinaryVersion;

DROP TABLE IF EXISTS EventCube.Agg_User_PctSessions_byBinaryVersion;  
CREATE TABLE EventCube.Agg_User_PctSessions_byBinaryVersion AS
SELECT ubv.UserId, ubv.BinaryVersion, (1.0 * ubv.Sessions) / u.Sessions AS PctSessions
FROM EventCube.Agg_Sessions_Per_User_BinaryVersion ubv
JOIN EventCube.Agg_Sessions_Per_User u ON ubv.UserId = u.UserId;

CREATE INDEX ndx_ecs_aggUserPctSessionsByBinaryVersion_userid ON EventCube.Agg_User_PctSessions_byBinaryVersion (UserId);  

--===================================================================================
-- Per User ID, identifies the most common Binary Version used across all sessions. 
-- Should no version be identified, defaults to "v???".
-- * Upstream dependency on DimUsers.
--===================================================================================
DROP TABLE IF EXISTS EventCube.DimUserBinaryVersion;  

CREATE TABLE EventCube.DimUserBinaryVersion AS
SELECT DISTINCT
UserId, 
LAST_VALUE(CASE WHEN BinaryVersion IS NULL THEN 'v???' WHEN LEFT(BinaryVersion,1) = 'v' THEN 'v' || RIGHT(LEFT(BinaryVersion,4),3) ELSE 'v' || LEFT(BinaryVersion,3) END) OVER (PARTITION BY UserId ORDER BY PctSessions ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) BinaryVersion
FROM EventCube.Agg_User_PctSessions_byBinaryVersion;  