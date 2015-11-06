--======================================================================================================================================================--

--===========--
-- TRANSFORM --
--===========--

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

CREATE TEMPORARY TABLE TempDimUsers AS

        SELECT ApplicationId, UserId, FirstTimestamp, LastTimestamp 
        FROM EventCube.Agg_Session_per_AppUser
        UNION
        SELECT base.ApplicationId, base.UserId, MIN(base.Created) FirstTimestamp, MAX(base.Created) LastTimestamp
        FROM PUBLIC.Ratings_UserCheckins base
        JOIN (SELECT ApplicationId FROM EventCube.BaseApps WHERE ApplicationId NOT IN (SELECT ApplicationId FROM EventCube.TestEvents)) app ON base.ApplicationId = app.ApplicationId
        GROUP BY base.ApplicationId, base.UserId
        UNION
        SELECT L.ApplicationId, L.UserId, MIN(L.Created) FirstTimestamp, MAX(L.Created) LastTimestamp
        FROM PUBLIC.Ratings_UserCheckInLikes L
        JOIN (SELECT ApplicationId FROM EventCube.BaseApps WHERE ApplicationId NOT IN (SELECT ApplicationId FROM EventCube.TestEvents)) app ON L.ApplicationId = app.ApplicationId
        GROUP BY L.ApplicationId, L.UserId
        UNION
        SELECT C.ApplicationId, C.UserId, MIN(C.Created) FirstTimestamp, MAX(C.Created) LastTimestamp
        FROM PUBLIC.Ratings_UserCheckInComments C
        JOIN (SELECT ApplicationId FROM EventCube.BaseApps WHERE ApplicationId NOT IN (SELECT ApplicationId FROM EventCube.TestEvents)) app ON C.ApplicationId = app.ApplicationId
        GROUP BY C.ApplicationId, C.UserId
        UNION
        SELECT F.ApplicationId, F.UserId, MIN(F.Created) FirstTimestamp, MAX(F.Created) LastTimestamp
        FROM PUBLIC.Ratings_UserFavorites F
        JOIN (SELECT ApplicationId FROM EventCube.BaseApps WHERE ApplicationId NOT IN (SELECT ApplicationId FROM EventCube.TestEvents)) app ON F.ApplicationId = app.ApplicationId
        GROUP BY F.ApplicationId, F.UserId
        UNION
        SELECT U.ApplicationId, F.UserId, F.FirstTimestamp, F.LastTimestamp
        FROM (SELECT F.UserId, MIN(F.Created) FirstTimestamp, MAX(F.Created) LastTimestamp FROM PUBLIC.Ratings_UserTrust F GROUP BY F.UserId) F
        JOIN PUBLIC.AuthDB_IS_Users U ON F.UserId = U.UserId
        JOIN (SELECT ApplicationId FROM EventCube.BaseApps WHERE ApplicationId NOT IN (SELECT ApplicationId FROM EventCube.TestEvents)) app ON U.ApplicationId = app.ApplicationId
        UNION
        SELECT SU.ApplicationId, SU.UserId, MIN(SU.Created) FirstTimestamp, MAX(SU.Created) LastTimestamp
        FROM PUBLIC.Ratings_ShowUps SU
        JOIN (SELECT ApplicationId FROM EventCube.BaseApps WHERE ApplicationId NOT IN (SELECT ApplicationId FROM EventCube.TestEvents)) app ON SU.ApplicationId = app.ApplicationId
        GROUP BY SU.ApplicationId, SU.UserId
        UNION
        SELECT S.ApplicationId, R.UserId, MIN(R.Created) FirstTimestamp, MAX(R.Created) LastTimestamp
        FROM PUBLIC.Ratings_SurveyResponses R
        JOIN PUBLIC.Ratings_SurveyQuestions Q ON R.SurveyQuestionId = Q.SurveyQuestionId
        JOIN PUBLIC.Ratings_Surveys S ON Q.SurveyId = S.SurveyId
        JOIN (SELECT ApplicationId FROM EventCube.BaseApps WHERE ApplicationId NOT IN (SELECT ApplicationId FROM EventCube.TestEvents)) app ON S.ApplicationId = app.ApplicationId
        GROUP BY S.ApplicationId, R.UserId
;

--Wrap the base set as an overall set aggregated to the User level

CREATE TEMPORARY TABLE TempDimUsers_Agg AS
SELECT MIN(FirstTimestamp) FirstTimestamp, MAX(LastTimestamp) LastTimestamp, F.ApplicationId, U.GlobalUserId, F.UserId
FROM TempDimUsers F
LEFT OUTER JOIN PUBLIC.AuthDB_IS_Users U ON F.ApplicationId = U.ApplicationId AND F.UserId = U.UserId
WHERE F.ApplicationId NOT IN (SELECT ApplicationId FROM EventCube.TestEvents)
GROUP BY F.ApplicationId, GlobalUserId, F.UserId
HAVING MIN(FirstTimestamp) >= '2013-05-16' AND MAX(LastTimestamp) <= CURRENT_DATE;

--======================================================================================================================================================--

--=======--
-- STAGE --
--=======--
TRUNCATE TABLE EventCube.STG_DimUsers;
VACUUM EventCube.STG_DimUsers;
INSERT INTO EventCube.STG_DimUsers
SELECT *
FROM TempDimUsers_Agg
WHERE GlobalUserId IS NOT NULL
AND UserId NOT IN (SELECT UserId FROM TempDimUsers_Agg WHERE UserID IS NOT NULL GROUP BY UserId HAVING COUNT(DISTINCT ApplicationId) > 1)
AND UserId != 0;

--======================================================================================================================================================--

--========--
-- UPSERT --
--========--
--Identify the Events that were not previously marked as Test Events, but are now considered Test Events based on today's Sessions calculation
TRUNCATE TABLE EventCube.STG_DimUsers_INSERT;
VACUUM EventCube.STG_DimUsers_INSERT;
INSERT INTO EventCube.STG_DimUsers_INSERT
SELECT FirstTimestamp, LastTimestamp, ApplicationId, GlobalUserId, UserId, CURRENT_TIMESTAMP FROM (
SELECT a.*, b.UserId AS bUserId FROM EventCube.STG_DimUsers a 
LEFT JOIN (SELECT DISTINCT UserId FROM EventCube.DimUsers) b ON a.UserId = b.UserId
) t WHERE bUserId IS NULL;

--Identify the Events that were previously marked as Test Events, but are no longer considered Test Events based on today's Sessions calculation
TRUNCATE TABLE EventCube.STG_DimUsers_UPDATE;
VACUUM EventCube.STG_DimUsers_UPDATE;
INSERT INTO EventCube.STG_DimUsers_UPDATE
SELECT * FROM EventCube.DimUsers 
WHERE ApplicationId IN (SELECT ApplicationId FROM EventCube.BaseApps)
AND UserId IN (SELECT UserId FROM EventCube.STG_DimUsers);

INSERT INTO EventCube.DimUsers SELECT * FROM EventCube.STG_DimUsers_INSERT;

UPDATE EventCube.DimUsers du 
SET 
  FirstTimestamp = EventCube.STG_DimUsers_UPDATE.FirstTimestamp,
  LastTimestamp = EventCube.STG_DimUsers_UPDATE.LastTimestamp,
  Updated = CURRENT_TIMESTAMP
FROM EventCube.STG_DimUsers_UPDATE
WHERE EventCube.STG_DimUsers_UPDATE.UserId = du.UserId;

--======================================================================================================================================================--
