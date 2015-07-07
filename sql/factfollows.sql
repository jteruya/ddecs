DROP TABLE IF EXISTS EventCube.FactFollows;

--===================================================================================================
-- Source on User Trust Fact table and enforces that each Follow is tied to an identified user. 
-- * Upstream dependency on DimUsers.
--===================================================================================================

CREATE TABLE EventCube.FactFollows AS 
SELECT 
S.Created AS Timestamp, 
U.ApplicationId, 
U.GlobalUserId, 
S.UserId, 
T.GlobalUserId AS TargetGlobalUserId, 
T.UserId AS TargetUserId
FROM PUBLIC.Ratings_UserTrust S
JOIN EventCube.DimUsers U ON S.UserId = U.UserId
LEFT OUTER JOIN EventCube.DimUsers T ON S.TrustsThisUserId = T.UserId;