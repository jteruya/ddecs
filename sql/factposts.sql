IF OBJECT_ID('ReportingDB.dbo.NewEventCube_FactPosts') IS NOT NULL
  DROP TABLE ReportingDB.dbo.NewEventCube_FactPosts

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
INTO ReportingDB.dbo.NewEventCube_FactPosts
FROM Ratings.dbo.UserCheckIns S
JOIN ReportingDB.dbo.NewEventCube_DimUsers U ON S.UserId = U.UserId
LEFT OUTER JOIN (SELECT DISTINCT CheckInId FROM Ratings.dbo.UserCheckInImages) I ON S.CheckInId = I.CheckInId
LEFT OUTER JOIN (SELECT DISTINCT ItemId, ListTypeId FROM Ratings.dbo.Item I JOIN Ratings.dbo.Topic T ON I.ParentTopicId = T.TopicId) T ON S.ItemId = T.ItemId

