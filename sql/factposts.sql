IF OBJECT_ID('ReportingDB.dbo.FactPosts') IS NOT NULL
  DROP TABLE ReportingDB.dbo.FactPosts

--===================================================================================================
-- Source on User Check-In Fact table and enforces that each Post is tied to an identified user. 
-- * Upstream dependency on DimUsers.
--
-- Minor transformations:
-- 1. List Type translated from numeric code to string value. List Type originates from Items/Topics.
-- 2. Flag to check if user has Image related to the post. 
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
END ListType, S.ItemId,
CASE WHEN I.CheckInId IS NOT NULL THEN 1 ELSE 0 END HasImage
INTO ReportingDB.dbo.FactPosts
FROM Ratings.dbo.UserCheckIns S
JOIN ReportingDB.dbo.DimUsers U ON S.UserId = U.UserId
LEFT OUTER JOIN (SELECT DISTINCT CheckInId FROM Ratings.dbo.UserCheckInImages) I ON S.CheckInId = I.CheckInId
LEFT OUTER JOIN (SELECT DISTINCT ItemId, ListTypeId FROM Ratings.dbo.Item I JOIN Ratings.dbo.Topic T ON I.ParentTopicId = T.TopicId) T ON S.ItemId = T.ItemId

