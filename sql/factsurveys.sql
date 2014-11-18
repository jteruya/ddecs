IF OBJECT_ID('ReportingDB.dbo.FactSurveys','U') IS NOT NULL
  DROP TABLE ReportingDB.dbo.FactSurveys

--===================================================================================================
-- Source on Survey Responses Fact table and enforces that each rating is tied to an identified user. 
-- * Upstream dependency on DimUsers.
--
-- Minor transformations:
-- 1. Metadata tied to Survey Responses (Question, Survey itself)
-- 2. Filtered on whether the Survey is considered a Poll
--===================================================================================================

SELECT DISTINCT R.Created Timestamp, S.ApplicationId, GlobalUserId, R.UserId, Q.SurveyId, Questions
INTO ReportingDB.dbo.FactSurveys
FROM Ratings.dbo.SurveyResponses R
JOIN Ratings.dbo.SurveyQuestions Q ON R.SurveyQuestionId = Q.SurveyQuestionId
JOIN Ratings.dbo.Surveys S ON Q.SurveyId = S.SurveyId
LEFT OUTER JOIN
( SELECT SurveyId, COUNT(*) Questions
  FROM Ratings.dbo.SurveyQuestions
  GROUP BY SurveyId
) N
ON Q.SurveyId = N.SurveyId
JOIN ReportingDB.dbo.DimUsers U ON R.UserId = U.UserId
WHERE IsPoll = 0

