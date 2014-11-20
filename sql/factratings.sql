IF OBJECT_ID('ReportingDB.dbo.FactRatings','U') IS NOT NULL
  DROP TABLE ReportingDB.dbo.FactRatings

--===================================================================================================
-- Source on User Ratings Fact table and enforces that each rating is tied to an identified user. 
-- * Upstream dependency on DimUsers.
--
-- Minor transformations:
-- 1. Translation of numeric code to string value. 
-- 2. Flag inidcator Review Comment is tied to rating. 
--===================================================================================================

SELECT DISTINCT S.DateEntered Timestamp, S.ApplicationId, GlobalUserId, S.UserId,
CASE
  WHEN ListTypeId = 0 THEN 'Unspecified'
  WHEN ListTypeId = 1 THEN 'Regular'
  WHEN ListTypeId = 2 THEN 'Agenda'
  WHEN ListTypeId = 3 THEN 'Exhibitors'
  WHEN ListTypeId = 4 THEN 'Speakers'
  WHEN ListTypeId = 5 THEN 'File'
  ELSE '???'
END ListType, S.ItemId,
Rating, CASE WHEN Comments != '' AND Comments IS NOT NULL THEN 1 ELSE 0 END HasReview
INTO ReportingDB.dbo.FactRatings
FROM Ratings.dbo.ItemRatings S
LEFT OUTER JOIN Ratings.dbo.Item I ON S.ItemId = I.ItemId
LEFT OUTER JOIN Ratings.dbo.Topic T ON I.ParentTopicId = T.TopicId
JOIN ReportingDB.dbo.DimUsers U ON S.UserId = U.UserId

