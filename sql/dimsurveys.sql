DROP TABLE IF EXISTS EventCube.DimSurveys;

--===============================================================================================
-- Base data on the Survey source records. 
-- * Upstream dependency on DimUsers. 
--
-- The following conditions are applied:
-- 1. Application must be related to a identified User (in the Dimension table)
--===============================================================================================

CREATE TABLE EventCube.DimSurveys AS
SELECT
A.SurveyId, 
A.ApplicationId, 
RTRIM(LTRIM(A.Name)) AS Name,
RTRIM(LTRIM(A.Description)) AS Description,
A.ItemId,
A.PostCheckInPrompt,
A.PostCheckInDelay,
A.IsDisabled,
A.IsPoll
FROM PUBLIC.Ratings_Surveys A
JOIN (SELECT DISTINCT ApplicationId FROM EventCube.DimUsers) U ON A.ApplicationId = U.ApplicationId

