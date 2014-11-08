IF OBJECT_ID('ReportingDB.dbo.NewEventCube_FactSurveys','U') IS NOT NULL
  DROP TABLE ReportingDB.dbo.NewEventCube_FactSurveys

SELECT DISTINCT R.Created Timestamp, S.ApplicationId, GlobalUserId, R.UserId, Q.SurveyId, Questions
INTO ReportingDB.dbo.NewEventCube_FactSurveys
FROM Ratings.dbo.SurveyResponses R
JOIN Ratings.dbo.SurveyQuestions Q ON R.SurveyQuestionId = Q.SurveyQuestionId
JOIN Ratings.dbo.Surveys S ON Q.SurveyId = S.SurveyId
LEFT OUTER JOIN
( SELECT SurveyId, COUNT(*) Questions
  FROM Ratings.dbo.SurveyQuestions
  GROUP BY SurveyId
) N
ON Q.SurveyId = N.SurveyId
JOIN ReportingDB.dbo.NewEventCube_DimUsers U ON R.UserId = U.UserId
WHERE IsPoll = 0

