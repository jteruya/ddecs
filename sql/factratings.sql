DROP TABLE IF EXISTS EventCube.FactRatings;

--===================================================================================================
-- Source on User Ratings Fact table and enforces that each rating is tied to an identified user. 
-- * Upstream dependency on DimUsers.
--
-- Minor transformations:
-- 1. Translation of numeric code to string value. 
-- 2. Flag inidcator Review Comment is tied to rating. 
--===================================================================================================

CREATE TABLE EventCube.FactRatings AS 
SELECT * FROM (
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
S.Rating, 
CASE WHEN S.Comments != '' AND S.Comments IS NOT NULL THEN 1 ELSE 0 END HasReview
FROM PUBLIC.Ratings_ItemRatings S
LEFT OUTER JOIN PUBLIC.Ratings_Item I ON S.ItemId = I.ItemId
LEFT OUTER JOIN PUBLIC.Ratings_Topic T ON I.ParentTopicId = T.TopicId
JOIN EventCube.DimUsers U ON S.UserId = U.UserId
) t WHERE HasReview = 1 ORDER BY Timestamp
;

CREATE INDEX ndx_ecs_factratings_userid ON EventCube.FactRatings (UserId);
CREATE INDEX ndx_ecs_factratings_timestamp ON EventCube.FactRatings (Timestamp);
