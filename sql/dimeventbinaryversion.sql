IF OBJECT_ID('ReportingDB.dbo.NewEventCube_DimEventBinaryVersion','U') IS NOT NULL
  DROP TABLE ReportingDB.dbo.NewEventCube_DimEventBinaryVersion

SELECT DISTINCT ApplicationId,
LAST_VALUE(BinaryVersion) OVER (PARTITION BY ApplicationId ORDER BY PctUsers ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) BinaryVersion
INTO ReportingDB.dbo.NewEventCube_DimEventBinaryVersion
FROM
( SELECT DISTINCT ApplicationId, BinaryVersion,
  1.0*COUNT(*) OVER (PARTITION BY ApplicationId, BinaryVersion)/COUNT(*) OVER (PARTITION BY ApplicationId) PctUsers
  FROM ReportingDB.dbo.NewEventCube_UserCubeSummary
) B

