DROP TABLE IF EXISTS EventCube.FactSurveys;

--===================================================================================================
-- Source on Survey Responses Fact table and enforces that each rating is tied to an identified user. 
-- * Upstream dependency on DimUsers.
--
-- Minor transformations:
-- 1. Metadata tied to Survey Responses (Question, Survey itself)
-- 2. Filtered on whether the Survey is considered a Poll
--===================================================================================================

CREATE TABLE EventCube.FactSurveys AS 
SELECT 
R.Created AS Timestamp, 
S.ApplicationId, 
U.GlobalUserId, 
R.UserId, 
Q.SurveyId, 
N.Questions
FROM PUBLIC.Ratings_SurveyResponses R
JOIN PUBLIC.Ratings_SurveyQuestions Q ON R.SurveyQuestionId = Q.SurveyQuestionId
JOIN PUBLIC.Ratings_Surveys S ON Q.SurveyId = S.SurveyId
LEFT OUTER JOIN
( SELECT SurveyId, COUNT(*) AS Questions
  FROM PUBLIC.Ratings_SurveyQuestions
  GROUP BY SurveyId
) N
ON Q.SurveyId = N.SurveyId
JOIN EventCube.DimUsers U ON R.UserId = U.UserId
WHERE S.IsPoll IS FALSE;