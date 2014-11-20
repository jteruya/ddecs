IF OBJECT_ID('ReportingDB.dbo.DimEventBinaryVersion','U') IS NOT NULL
  DROP TABLE ReportingDB.dbo.DimEventBinaryVersion

--===============================================================================================================================================
-- Per Application, identify the Binary Version by checking which version is most common from the User data.  
-- * Upstream dependency on UserCubeSummary. 
--===============================================================================================================================================

SELECT DISTINCT ApplicationId,
LAST_VALUE(BinaryVersion) OVER (PARTITION BY ApplicationId ORDER BY PctUsers ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) BinaryVersion
INTO ReportingDB.dbo.DimEventBinaryVersion
FROM
( SELECT DISTINCT ApplicationId, BinaryVersion,
  1.0*COUNT(*) OVER (PARTITION BY ApplicationId, BinaryVersion)/COUNT(*) OVER (PARTITION BY ApplicationId) PctUsers
  FROM ReportingDB.dbo.UserCubeSummary
) B

