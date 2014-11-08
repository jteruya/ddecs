IF OBJECT_ID('ReportingDB.dbo.NewEventCube_DimUserBinaryVersion','U') IS NOT NULL
  DROP TABLE ReportingDB.dbo.NewEventCube_DimUserBinaryVersion

SELECT DISTINCT UserId, LAST_VALUE(CASE WHEN BinaryVersion IS NULL THEN 'v???' WHEN LEFT(BinaryVersion,1) = 'v' THEN 'v'+RIGHT(LEFT(BinaryVersion,4),3) ELSE 'v'+LEFT(BinaryVersion,3) END) OVER (PARTITION BY UserId ORDER BY PctSessions ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) BinaryVersion
INTO ReportingDB.dbo.NewEventCube_DimUserBinaryVersion
FROM
( SELECT DISTINCT S.UserId, BinaryVersion, 1.0*COUNT(*) OVER (PARTITION BY S.UserId, BinaryVersion)/COUNT(*) OVER (PARTITION BY S.UserId) PctSessions
  FROM AnalyticsDB.dbo.Sessions S
  JOIN ReportingDB.dbo.NewEventCube_DimUsers U ON S.UserId = U.UserId
) S

