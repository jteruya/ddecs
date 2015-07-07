DROP TABLE IF EXISTS EventCube.FactLikes;

--===================================================================================================
-- Source on User Likes Fact table and enforces that each like is tied to an identified user. 
-- * Upstream dependency on DimUsers.
--
-- Minor transformations:
-- 1. Translation of numeric code to string value. 
-- 2. Flag inidcator that related post has image. 
--===================================================================================================

CREATE TABLE EventCube.FactLikes AS 
SELECT 
S.Created AS Timestamp, 
S.ApplicationId, 
S.GlobalUserId, 
S.UserId,
CASE
  WHEN P.ListTypeId = 0 THEN 'Unspecified'
  WHEN P.ListTypeId = 1 THEN 'Regular'
  WHEN P.ListTypeId = 2 THEN 'Agenda'
  WHEN P.ListTypeId = 3 THEN 'Exhibitors'
  WHEN P.ListTypeId = 4 THEN 'Speakers'
  WHEN P.ListTypeId = 5 THEN 'File'
  ELSE '???'
END ListType, 
P.ItemId,
CASE WHEN I.CheckInId IS NOT NULL THEN 1 ELSE 0 END HasImage
FROM (SELECT S.ApplicationId, S.UserId, S.Created, S.CheckInId, U.GlobalUserId FROM PUBLIC.Ratings_UserCheckInLikes S JOIN EventCube.DimUsers U ON S.UserId = U.UserId) S
LEFT OUTER JOIN (SELECT P.ApplicationId, P.CheckInId, T.ListTypeId, P.ItemId FROM PUBLIC.Ratings_UserCheckIns P JOIN PUBLIC.Ratings_Item I ON P.ItemId = I.ItemId JOIN PUBLIC.Ratings_Topic T ON I.ParentTopicId = T.TopicId) P ON S.CheckInId = P.CheckInId
LEFT OUTER JOIN (SELECT DISTINCT CheckInId FROM PUBLIC.Ratings_UserCheckInImages) I ON S.CheckInId = I.CheckInId;

CREATE INDEX ndx_ecs_factlikes_userid ON EventCube.FactLikes (UserId);