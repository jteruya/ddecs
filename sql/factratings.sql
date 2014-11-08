IF OBJECT_ID('ReportingDB.dbo.NewEventCube_FactRatings','U') IS NOT NULL
  DROP TABLE ReportingDB.dbo.NewEventCube_FactRatings

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
INTO ReportingDB.dbo.NewEventCube_FactRatings
FROM Ratings.dbo.ItemRatings S
LEFT OUTER JOIN Ratings.dbo.Item I ON S.ItemId = I.ItemId
LEFT OUTER JOIN Ratings.dbo.Topic T ON I.ParentTopicId = T.TopicId
JOIN ReportingDB.dbo.NewEventCube_DimUsers U ON S.UserId = U.UserId

