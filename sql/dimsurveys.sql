IF OBJECT_ID('ReportingDB.dbo.DimSurveys','U') IS NOT NULL
  DROP TABLE ReportingDB.dbo.DimSurveys

--===============================================================================================
-- Base data on the Survey source records. 
-- * Upstream dependency on DimUsers. 
--
-- The following conditions are applied:
-- 1. Application must be related to a identified User (in the Dimension table)
--===============================================================================================

SELECT DISTINCT A.SurveyId, A.ApplicationId, 
RTRIM(LTRIM(A.Name)) AS Name,
RTRIM(LTRIM(A.Description)) AS Description,
A.ItemId,
A.PostCheckInPrompt,
A.PostCheckInDelay,
A.IsDisabled,
A.IsPoll
INTO ReportingDB.dbo.DimSurveys
FROM Ratings.dbo.Surveys A
JOIN (SELECT DISTINCT ApplicationId FROM ReportingDB.dbo.DimUsers) U ON A.ApplicationId = U.ApplicationId

