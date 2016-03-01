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
-- B. Filter out the Test Event users (Deprecitated 2/26/2016 - PA-481)
-- C. Filter the fact data that is older than May 16, 2013. 
-- D. Filter out any records without a Global User ID
-- E. Filter out any User IDs where the User ID is tied to more than one Application ID. 
-- F. Filter out any User ID of value 0
--================================================================================================================

-- Identify just the cases per each datasource where (a) the Timestamp is outside the original bounds of DimUsers or (b) is not present.

CREATE TEMPORARY TABLE TempDimUsers TABLESPACE FastStorage AS
        SELECT base.ApplicationId, base.UserId, base.FirstTimestamp AS TS, U.GlobalUserId
        FROM EventCube.Agg_Session_per_AppUser base
        LEFT JOIN EventCube.DimUsers du ON base.ApplicationId = du.ApplicationId AND base.UserId = du.UserId
        LEFT OUTER JOIN PUBLIC.AuthDB_IS_Users U ON base.ApplicationId = U.ApplicationId AND base.UserId = U.UserId
        WHERE /*base.ApplicationId NOT IN (SELECT ApplicationId FROM EventCube.TestEvents)
        AND*/ (base.FirstTimestamp < du.FirstTimestamp OR base.FirstTimestamp > du.LastTimestamp OR du.UserId IS NULL)
        UNION ALL
        SELECT base.ApplicationId, base.UserId, base.LastTimestamp AS TS, U.GlobalUserId
        FROM EventCube.Agg_Session_per_AppUser base
        LEFT JOIN EventCube.DimUsers du ON base.ApplicationId = du.ApplicationId AND base.UserId = du.UserId
        LEFT OUTER JOIN PUBLIC.AuthDB_IS_Users U ON base.ApplicationId = U.ApplicationId AND base.UserId = U.UserId
        WHERE /*base.ApplicationId NOT IN (SELECT ApplicationId FROM EventCube.TestEvents)
        AND*/ (base.LastTimestamp < du.FirstTimestamp OR base.LastTimestamp > du.LastTimestamp OR du.UserId IS NULL)
        UNION ALL
        SELECT base.ApplicationId, base.UserId, base.Created AS TS, U.GlobalUserId
        FROM PUBLIC.Ratings_UserCheckins base
        LEFT JOIN EventCube.DimUsers du ON base.ApplicationId = du.ApplicationId AND base.UserId = du.UserId
        JOIN (SELECT ApplicationId FROM EventCube.BaseApps /*WHERE ApplicationId NOT IN (SELECT ApplicationId FROM EventCube.TestEvents)*/) app ON base.ApplicationId = app.ApplicationId
        LEFT OUTER JOIN PUBLIC.AuthDB_IS_Users U ON base.ApplicationId = U.ApplicationId AND base.UserId = U.UserId
        WHERE (base.Created < du.FirstTimestamp OR base.Created > du.LastTimestamp OR du.UserId IS NULL)
        UNION ALL
        SELECT L.ApplicationId, L.UserId, L.Created AS TS, U.GlobalUserId
        FROM PUBLIC.Ratings_UserCheckInLikes L
        LEFT JOIN EventCube.DimUsers du ON L.ApplicationId = du.ApplicationId AND L.UserId = du.UserId
        JOIN (SELECT ApplicationId FROM EventCube.BaseApps /*WHERE ApplicationId NOT IN (SELECT ApplicationId FROM EventCube.TestEvents)*/) app ON L.ApplicationId = app.ApplicationId
        LEFT OUTER JOIN PUBLIC.AuthDB_IS_Users U ON L.ApplicationId = U.ApplicationId AND L.UserId = U.UserId
        WHERE (L.Created < du.FirstTimestamp OR L.Created > du.LastTimestamp OR du.UserId IS NULL)
        UNION ALL
        SELECT C.ApplicationId, C.UserId, C.Created AS TS, U.GlobalUserId
        FROM PUBLIC.Ratings_UserCheckInComments C
        LEFT JOIN EventCube.DimUsers du ON C.ApplicationId = du.ApplicationId AND C.UserId = du.UserId
        JOIN (SELECT ApplicationId FROM EventCube.BaseApps /*WHERE ApplicationId NOT IN (SELECT ApplicationId FROM EventCube.TestEvents)*/) app ON C.ApplicationId = app.ApplicationId
        LEFT OUTER JOIN PUBLIC.AuthDB_IS_Users U ON C.ApplicationId = U.ApplicationId AND C.UserId = U.UserId
        WHERE (C.Created < du.FirstTimestamp OR C.Created > du.LastTimestamp OR du.UserId IS NULL)
        UNION ALL
        SELECT F.ApplicationId, F.UserId, F.Created AS TS, U.GlobalUserId
        FROM PUBLIC.Ratings_UserFavorites F
        LEFT JOIN EventCube.DimUsers du ON F.ApplicationId = du.ApplicationId AND F.UserId = du.UserId
        JOIN (SELECT ApplicationId FROM EventCube.BaseApps /*WHERE ApplicationId NOT IN (SELECT ApplicationId FROM EventCube.TestEvents)*/) app ON F.ApplicationId = app.ApplicationId
        LEFT OUTER JOIN PUBLIC.AuthDB_IS_Users U ON F.ApplicationId = U.ApplicationId AND F.UserId = U.UserId
        WHERE (F.Created < du.FirstTimestamp OR F.Created > du.LastTimestamp OR du.UserId IS NULL)
        UNION ALL
        SELECT U.ApplicationId, F.UserId, F.TS AS TS, U.GlobalUserId
        FROM (SELECT F.UserId, F.Created AS TS FROM PUBLIC.Ratings_UserTrust F) F
        JOIN PUBLIC.AuthDB_IS_Users U ON F.UserId = U.UserId
        LEFT JOIN EventCube.DimUsers du ON U.ApplicationId = du.ApplicationId AND F.UserId = du.UserId
        JOIN (SELECT ApplicationId FROM EventCube.BaseApps /*WHERE ApplicationId NOT IN (SELECT ApplicationId FROM EventCube.TestEvents)*/) app ON U.ApplicationId = app.ApplicationId
        WHERE (F.TS < du.FirstTimestamp OR F.TS > du.LastTimestamp OR du.UserId IS NULL)
        UNION ALL
        SELECT SU.ApplicationId, SU.UserId, SU.Created AS TS, U.GlobalUserId
        FROM PUBLIC.Ratings_ShowUps SU
        LEFT JOIN EventCube.DimUsers du ON SU.ApplicationId = du.ApplicationId AND SU.UserId = du.UserId
        JOIN (SELECT ApplicationId FROM EventCube.BaseApps /*WHERE ApplicationId NOT IN (SELECT ApplicationId FROM EventCube.TestEvents)*/) app ON SU.ApplicationId = app.ApplicationId
        LEFT OUTER JOIN PUBLIC.AuthDB_IS_Users U ON SU.ApplicationId = U.ApplicationId AND SU.UserId = U.UserId
        WHERE (SU.Created < du.FirstTimestamp OR SU.Created > du.LastTimestamp OR du.UserId IS NULL)
        UNION ALL
        SELECT S.ApplicationId, R.UserId, R.Created AS TS, U.GlobalUserId
        FROM PUBLIC.Ratings_SurveyResponses R
        JOIN PUBLIC.Ratings_SurveyQuestions Q ON R.SurveyQuestionId = Q.SurveyQuestionId
        JOIN PUBLIC.Ratings_Surveys S ON Q.SurveyId = S.SurveyId
        LEFT JOIN EventCube.DimUsers du ON S.ApplicationId = du.ApplicationId AND R.UserId = du.UserId
        JOIN (SELECT ApplicationId FROM EventCube.BaseApps /*WHERE ApplicationId NOT IN (SELECT ApplicationId FROM EventCube.TestEvents)*/) app ON S.ApplicationId = app.ApplicationId
        LEFT OUTER JOIN PUBLIC.AuthDB_IS_Users U ON S.ApplicationId = U.ApplicationId AND R.UserId = U.UserId
        WHERE (R.Created < du.FirstTimestamp OR R.Created > du.LastTimestamp OR du.UserId IS NULL)
;

--Wrap the base set as an overall set aggregated to the User level
CREATE TEMPORARY TABLE TempDimUsers_Agg TABLESPACE FastStorage AS
SELECT MIN(F.TS) AS FirstTimestamp, MAX(F.TS) AS LastTimestamp, F.ApplicationId, F.GlobalUserId, F.UserId
FROM TempDimUsers F
WHERE F.TS >= '2013-05-16' AND F.TS <= CURRENT_DATE
GROUP BY F.ApplicationId, F.GlobalUserId, CAST(F.UserId AS INT);

--======================================================================================================================================================--

--=======--
-- STAGE --
--=======--
TRUNCATE TABLE EventCube.STG_DimUsers;
VACUUM EventCube.STG_DimUsers;
INSERT INTO EventCube.STG_DimUsers
SELECT FirstTimestamp, LastTimestamp, ApplicationId, GlobalUserId, UserId
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
