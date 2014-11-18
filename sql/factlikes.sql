IF OBJECT_ID('ReportingDB.dbo.FactLikes','U') IS NOT NULL
  DROP TABLE ReportingDB.dbo.FactLikes

--===================================================================================================
-- Source on User Likes Fact table and enforces that each like is tied to an identified user. 
-- * Upstream dependency on DimUsers.
--
-- Minor transformations:
-- 1. Translation of numeric code to string value. 
-- 2. Flag inidcator that related post has image. 
--===================================================================================================

SELECT DISTINCT S.Created Timestamp, P.ApplicationId, GlobalUserId, S.UserId,
CASE
  WHEN ListTypeId = 0 THEN 'Unspecified'
  WHEN ListTypeId = 1 THEN 'Regular'
  WHEN ListTypeId = 2 THEN 'Agenda'
  WHEN ListTypeId = 3 THEN 'Exhibitors'
  WHEN ListTypeId = 4 THEN 'Speakers'
  WHEN ListTypeId = 5 THEN 'File'
  ELSE '???'
END ListType, ItemId,
CASE WHEN I.CheckInId IS NOT NULL THEN 1 ELSE 0 END HasImage
INTO ReportingDB.dbo.FactLikes
FROM Ratings.dbo.UserCheckInLikes S
LEFT OUTER JOIN (SELECT DISTINCT P.ApplicationId, CheckInId, ListTypeId, P.ItemId FROM Ratings.dbo.UserCheckIns P JOIN Ratings.dbo.Item I ON P.ItemId = I.ItemId JOIN Ratings.dbo.Topic T ON I.ParentTopicId = T.TopicId) P ON S.CheckInId = P.CheckInId
LEFT OUTER JOIN (SELECT DISTINCT CheckInId FROM Ratings.dbo.UserCheckInImages) I ON P.CheckInId = I.CheckInId
JOIN ReportingDB.dbo.DimUsers U ON S.UserId = U.UserId

