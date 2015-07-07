DROP TABLE IF EXISTS EventCube.FactBookmarks;

--===================================================================================================
-- Source on User Favorites Fact table and enforces that each favorite is tied to an identified user. 
-- * Upstream dependency on DimUsers.
--
-- Minor transformations:
-- 1. Translation of numeric code to string value. 
--===================================================================================================

CREATE TABLE EventCube.FactBookmarks AS 
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
<<<<<<< HEAD
END ListType, 
S.ItemId
FROM PUBLIC.Ratings_UserFavorites S
LEFT OUTER JOIN PUBLIC.Ratings_Item I ON S.ItemId = I.ItemId
LEFT OUTER JOIN PUBLIC.Ratings_Topic T ON I.ParentTopicId = T.TopicId
JOIN EventCube.DimUsers U ON S.UserId = U.UserId;
=======
END ListType, S.ItemId
INTO ReportingDB.dbo.FactBookmarks
FROM Ratings.dbo.UserFavorites S
LEFT OUTER JOIN Ratings.dbo.Item I ON S.ItemId = I.ItemId
LEFT OUTER JOIN Ratings.dbo.Topic T ON I.ParentTopicId = T.TopicId
JOIN ReportingDB.dbo.DimUsers U ON S.UserId = U.UserId
WHERE IsImported = 0

>>>>>>> upstream/master
