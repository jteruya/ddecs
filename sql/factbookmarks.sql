IF OBJECT_ID('ReportingDB.dbo.FactBookmarks','U') IS NOT NULL
  DROP TABLE ReportingDB.dbo.FactBookmarks

--===================================================================================================
-- Source on User Favorites Fact table and enforces that each favorite is tied to an identified user. 
-- * Upstream dependency on DimUsers.
--
-- Minor transformations:
-- 1. Translation of numeric code to string value. 
--===================================================================================================

SELECT DISTINCT S.Created Timestamp, S.ApplicationId, GlobalUserId, S.UserId,
CASE
  WHEN ListTypeId = 0 THEN 'Unspecified'
  WHEN ListTypeId = 1 THEN 'Regular'
  WHEN ListTypeId = 2 THEN 'Agenda'
  WHEN ListTypeId = 3 THEN 'Exhibitors'
  WHEN ListTypeId = 4 THEN 'Speakers'
  WHEN ListTypeId = 5 THEN 'File'
  ELSE '???'
END ListType, S.ItemId
INTO ReportingDB.dbo.FactBookmarks
FROM Ratings.dbo.UserFavorites S
LEFT OUTER JOIN Ratings.dbo.Item I ON S.ItemId = I.ItemId
LEFT OUTER JOIN Ratings.dbo.Topic T ON I.ParentTopicId = T.TopicId
JOIN ReportingDB.dbo.DimUsers U ON S.UserId = U.UserId
WHERE IsImported = 0

