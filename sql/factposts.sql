DROP TABLE IF EXISTS EventCube.FactPosts;

--===================================================================================================
-- Source on User Check-In Fact table and enforces that each Post is tied to an identified user. 
-- * Upstream dependency on DimUsers.
--
-- Minor transformations:
-- 1. List Type translated from numeric code to string value. List Type originates from Items/Topics.
-- 2. Flag to check if user has Image related to the post. 
--===================================================================================================

CREATE TABLE EventCube.FactPosts AS 
SELECT 
S.Created AS Timestamp, 
S.ApplicationId, 
U.GlobalUserId, 
S.UserId,
CASE
  WHEN T.ListTypeId = 0 THEN 'Unspecified'
  WHEN T.ListTypeId = 1 THEN 'Regular'
  WHEN T.ListTypeId = 2 THEN 'Agenda'
  WHEN T.ListTypeId = 3 THEN 'Exhibitors'
  WHEN T.ListTypeId = 4 THEN 'Speakers'
  WHEN T.ListTypeId = 5 THEN 'File'
  ELSE '???'
END ListType, 
S.ItemId,
CASE WHEN I.CheckInId IS NOT NULL THEN 1 ELSE 0 END HasImage
FROM (SELECT CheckInId, Created, ApplicationId, ItemId, UserId FROM PUBLIC.Ratings_UserCheckIns) S
JOIN EventCube.DimUsers U ON S.UserId = U.UserId
LEFT OUTER JOIN (SELECT DISTINCT CheckInId FROM PUBLIC.Ratings_UserCheckInImages) I ON S.CheckInId = I.CheckInId
LEFT OUTER JOIN (SELECT I.ItemId, T.ListTypeId FROM PUBLIC.Ratings_Item I JOIN PUBLIC.Ratings_Topic T ON I.ParentTopicId = T.TopicId) T ON S.ItemId = T.ItemId;

CREATE INDEX ndx_ecs_factposts_userid ON EventCube.FactPosts (UserId);
CREATE INDEX ndx_ecs_factposts_timestamp ON EventCube.FactPosts (Timestamp);