IF OBJECT_ID('ReportingDB.dbo.TempDimUsers') IS NOT NULL
  DROP TABLE ReportingDB.dbo.TempDimUsers

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

SELECT MIN(FirstTimestamp) FirstTimestamp, MAX(LastTimestamp) LastTimestamp, F.ApplicationId, GlobalUserId, F.UserId
INTO ReportingDB.dbo.TempDimUsers
FROM
( SELECT ApplicationId, UserId, MIN(StartDate) FirstTimestamp, MAX(StartDate) LastTimestamp
  FROM AnalyticsDB.dbo.Sessions
  WHERE UserId IS NOT NULL
  GROUP BY ApplicationId, UserId
  UNION
  SELECT ApplicationId, UserId, MIN(Created) FirstTimestamp, MAX(Created) LastTimestamp
  FROM Ratings.dbo.UserCheckIns
  GROUP BY ApplicationId, UserId
  UNION
  SELECT ApplicationId, L.UserId, MIN(L.Created) FirstTimestamp, MAX(L.Created) LastTimestamp
  FROM Ratings.dbo.UserCheckInLikes L
  JOIN Ratings.dbo.UserCheckIns P ON L.CheckInId = P.CheckInId
  GROUP BY ApplicationId, L.UserId
  UNION
  SELECT ApplicationId, C.UserId, MIN(C.Created) FirstTimestamp, MAX(C.Created) LastTimestamp
  FROM Ratings.dbo.UserCheckInComments C
  JOIN Ratings.dbo.UserCheckIns P ON C.CheckInId = P.CheckInId
  GROUP BY ApplicationId, C.UserId
  UNION
  SELECT ApplicationId, UserId, MIN(Created) FirstTimestamp, MAX(Created) LastTimestamp
  FROM Ratings.dbo.UserFavorites
  GROUP BY ApplicationId, UserId
  UNION
  SELECT ApplicationId, F.UserId, MIN(CreatedOn) FirstTimestamp, MAX(CreatedOn) LastTimestamp
  FROM Ratings.dbo.UserTrust F
  JOIN AuthDB.dbo.IS_Users U ON F.UserId = U.UserId
  GROUP BY ApplicationId, F.UserId
  UNION
  SELECT ApplicationId, UserId, MIN(Created) FirstTimestamp, MAX(Created) LastTimestamp
  FROM Ratings.dbo.ShowUps
  GROUP BY ApplicationId, UserId
  UNION
  SELECT ApplicationId, UserId, MIN(R.Created) FirstTimestamp, MAX(R.Created) LastTimestamp
  FROM Ratings.dbo.SurveyResponses R
  JOIN Ratings.dbo.SurveyQuestions Q ON R.SurveyQuestionId = Q.SurveyQuestionId
  JOIN Ratings.dbo.Surveys S ON Q.SurveyId = S.SurveyId
  GROUP BY ApplicationId, UserId
) F 
LEFT OUTER JOIN AuthDB.dbo.IS_Users U ON F.ApplicationId = U.ApplicationId AND F.UserId = U.UserId
WHERE F.ApplicationId NOT IN (SELECT ApplicationId FROM ReportingDB.dbo.TestEvents)
GROUP BY F.ApplicationId, GlobalUserId, F.UserId
HAVING MIN(FirstTimestamp) >= '2013-05-16' AND MAX(LastTimestamp) <= GETUTCDATE()

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

IF OBJECT_ID('ReportingDB.dbo.DimUsers') IS NOT NULL
  DROP TABLE ReportingDB.dbo.DimUsers

SELECT *
INTO ReportingDB.dbo.DimUsers
FROM ReportingDB.dbo.TempDimUsers U WHERE 1=1
-- WHERE NOT EXISTS (SELECT 1 FROM ReportingDB.dbo.DimBadUsers B WHERE U.ApplicationId = B.ApplicationId AND U.GlobalUserId = B.GlobalUserId AND U.UserId = B.UserId) -- Why the eff doesn't this work
AND GlobalUserId IS NOT NULL
AND NOT EXISTS (SELECT 1 FROM (SELECT UserId FROM ReportingDB.dbo.TempDimUsers GROUP BY UserId HAVING COUNT(DISTINCT ApplicationId) > 1) B WHERE U.UserId = B.UserId)
AND UserId != 0

IF OBJECT_ID('ReportingDB.dbo.TempDimUsers') IS NOT NULL
  DROP TABLE ReportingDB.dbo.TempDimUsers

