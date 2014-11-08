IF OBJECT_ID('ReportingDB.dbo.NewEventCube_FactFollows','U') IS NOT NULL
  DROP TABLE ReportingDB.dbo.NewEventCube_FactFollows

SELECT DISTINCT S.CreatedOn Timestamp, U.ApplicationId, U.GlobalUserId, S.UserId, T.GlobalUserId TargetGlobalUserId, T.UserId TargetUserId
INTO ReportingDB.dbo.NewEventCube_FactFollows
FROM Ratings.dbo.UserTrust S
JOIN ReportingDB.dbo.NewEventCube_DimUsers U ON S.UserId = U.UserId
LEFT OUTER JOIN ReportingDB.dbo.NewEventCube_DimUsers T ON S.TrustsThisUserId = T.UserId

