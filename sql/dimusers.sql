DROP TABLE IF EXISTS EventCube.TempDimUsers;

--================================================================================================================
-- Create an initial Temporary set of User Dimension data that will be standardized/aggregated from fact data. 
-- The data is collected across:
-- 1. Sessions
-- 2. User Check Ins
-- 3. User Likes
-- 4. User Comments
-- 5. User Favorites
-- 6. Trust
-- 7. Show Ups
-- 8. Survey Responses
--
-- This temporary dataset is then transformed in the following ways to create the User Dimension data:
-- A. Identify the Global User ID via IS_USER
-- B. Filter out the Test Event users
-- C. Filter the fact data that is older than May 16, 2013. 
-- D. Filter out any records without a Global User ID
-- E. Filter out any User IDs where the User ID is tied to more than one Application ID. 
-- F. Filter out any User ID of value 0
--================================================================================================================

CREATE TABLE EventCube.TempDimUsers AS
SELECT MIN(FirstTimestamp) FirstTimestamp, MAX(LastTimestamp) LastTimestamp, F.ApplicationId, U.GlobalUserId, F.UserId
FROM
( 
  SELECT * FROM  EventCube.Agg_Session_per_AppUser
  
  UNION
  
  SELECT ApplicationId, UserId, MIN(Created) FirstTimestamp, MAX(Created) LastTimestamp
  FROM PUBLIC.Ratings_UserCheckins
  GROUP BY ApplicationId, UserId
  
  UNION

  SELECT L.ApplicationId, L.UserId, MIN(L.Created) FirstTimestamp, MAX(L.Created) LastTimestamp
  FROM PUBLIC.Ratings_UserCheckInLikes L
  GROUP BY L.ApplicationId, L.UserId
  
  UNION
  
  SELECT C.ApplicationId, C.UserId, MIN(C.Created) FirstTimestamp, MAX(C.Created) LastTimestamp
  FROM PUBLIC.Ratings_UserCheckInComments C
  GROUP BY C.ApplicationId, C.UserId
  
  UNION

  SELECT ApplicationId, UserId, MIN(Created) FirstTimestamp, MAX(Created) LastTimestamp
  FROM PUBLIC.Ratings_UserFavorites
  GROUP BY ApplicationId, UserId
  
  UNION

  SELECT U.ApplicationId, F.UserId, F.FirstTimestamp, F.LastTimestamp
  FROM (SELECT F.UserId, MIN(F.Created) FirstTimestamp, MAX(F.Created) LastTimestamp FROM PUBLIC.Ratings_UserTrust F GROUP BY F.UserId) F
  JOIN PUBLIC.AuthDB_IS_Users U ON F.UserId = U.UserId
  
  UNION

  SELECT ApplicationId, UserId, MIN(Created) FirstTimestamp, MAX(Created) LastTimestamp
  FROM PUBLIC.Ratings_ShowUps
  GROUP BY ApplicationId, UserId
  
  UNION
  
  SELECT S.ApplicationId, R.UserId, MIN(R.Created) FirstTimestamp, MAX(R.Created) LastTimestamp
  FROM PUBLIC.Ratings_SurveyResponses R
  JOIN PUBLIC.Ratings_SurveyQuestions Q ON R.SurveyQuestionId = Q.SurveyQuestionId
  JOIN PUBLIC.Ratings_Surveys S ON Q.SurveyId = S.SurveyId
  GROUP BY S.ApplicationId, R.UserId
) F 
LEFT OUTER JOIN PUBLIC.AuthDB_IS_Users U ON F.ApplicationId = U.ApplicationId AND F.UserId = U.UserId
WHERE F.ApplicationId NOT IN (SELECT ApplicationId FROM EventCube.TestEvents)
GROUP BY F.ApplicationId, GlobalUserId, F.UserId
HAVING MIN(FirstTimestamp) >= '2013-05-16' AND MAX(LastTimestamp) <= CURRENT_DATE;

-- Get first timestamp of Flock 3, when tracking is considered as starting to be reliable
-- SELECT DISTINCT
--   FIRST_VALUE(BinaryVersion) OVER (ORDER BY StartDate ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FirstBinaryVersion,
--   FIRST_VALUE(startdate) OVER (ORDER BY StartDate ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FirstStartDate
-- FROM AnalyticsDB.dbo.Sessions
-- WHERE LEFT(BinaryVersion,1) = '3'

-- FirstBinaryVersion  FirstStartDate       
-- ------------------  -------------------  
-- 3.3.2               2013-05-16 23:21:42  

-- IF OBJECT_ID('ReportingDB.dbo.DimBadUsers') IS NOT NULL
--   DROP TABLE ReportingDB.dbo.DimBadUsers
-- 
-- SELECT *
-- INTO ReportingDB.dbo.DimBadUsers
-- FROM ReportingDB.dbo.TempDimUsers
-- WHERE GlobalUserId IS NULL
-- OR UserId IN (SELECT UserId FROM ReportingDB.dbo.TempDimUsers GROUP BY UserId HAVING COUNT(DISTINCT ApplicationId) > 1)
-- OR UserId = 0

DROP TABLE IF EXISTS EventCube.DimUsers;

CREATE TABLE EventCube.DimUsers AS
SELECT *
FROM EventCube.TempDimUsers U WHERE 1=1
-- WHERE NOT EXISTS (SELECT 1 FROM ReportingDB.dbo.DimBadUsers B WHERE U.ApplicationId = B.ApplicationId AND U.GlobalUserId = B.GlobalUserId AND U.UserId = B.UserId) -- Why the eff doesn't this work
AND GlobalUserId IS NOT NULL
AND NOT EXISTS (SELECT 1 FROM (SELECT UserId FROM EventCube.TempDimUsers GROUP BY UserId HAVING COUNT(DISTINCT ApplicationId) > 1) B WHERE U.UserId = B.UserId)
AND UserId != 0;

/*
AND ApplicationId NOT IN (
SELECT DISTINCT a.ApplicationId
FROM ReportingDB.dbo.TempDimUsers a
JOIN ReportingDB.dbo.DimEvents b ON a.ApplicationId = b.ApplicationId
WHERE (LOWER(Name) LIKE '%test%' OR LOWER(Name) LIKE '%dext%' OR LOWER(Name) LIKE '%do not use%') AND LOWER(Name) NOT LIKE '%testing conference%' AND LOWER(Name) NOT LIKE '%contest%'
*/

--DROP TABLE IF EXISTS EventCube.TempDimUsers;

CREATE INDEX ndx_ecs_dimusers ON EventCube.DimUsers (UserId);
CREATE INDEX ndx_ecs_dimusers_applicationid ON EventCube.DimUsers (ApplicationId);