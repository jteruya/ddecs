DROP TABLE IF EXISTS EventCube.FactComments;

--===================================================================================================
-- Source on User Comments Fact table and enforces that each comment is tied to an identified user. 
-- * Upstream dependency on DimUsers.
--
-- Minor transformations:
-- 1. Translation of numeric code to string value. 
-- 2. Flag Indicator for Image related to the post. 
--===================================================================================================

CREATE TABLE EventCube.FactComments AS 
SELECT 
S.Created AS Timestamp, 
P.ApplicationId, 
U.GlobalUserId, 
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
FROM PUBLIC.Ratings_UserCheckInComments S
LEFT OUTER JOIN (SELECT P.ApplicationId, P.CheckInId, T.ListTypeId, P.ItemId FROM PUBLIC.Ratings_UserCheckIns P JOIN PUBLIC.Ratings_Item I ON P.ItemId = I.ItemId JOIN PUBLIC.Ratings_Topic T ON I.ParentTopicId = T.TopicId) P ON S.CheckInId = P.CheckInId
LEFT OUTER JOIN (SELECT DISTINCT CheckInId FROM PUBLIC.Ratings_UserCheckInImages) I ON P.CheckInId = I.CheckInId
JOIN EventCube.DimUsers U ON S.UserId = U.UserId;
