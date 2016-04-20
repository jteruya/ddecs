--======================================================================================================================================================--

--======--
-- BASE --
--======--

--Identify the set of events for which we will be performing any Cubes (Events with Start Dates within the last N months, the future, or not yet set)
DROP TABLE IF EXISTS EventCube.BaseApps;
CREATE TABLE EventCube.BaseApps AS 
SELECT A.ApplicationId, A.Name FROM AuthDB_Applications A
JOIN PUBLIC.AuthDB_Bundles B ON A.BundleId = B.BundleId
WHERE StartDate >= CAST(EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL'13 months')||'-'||EXTRACT(MONTH FROM CURRENT_DATE - INTERVAL'13 months')||'-01 00:00:00' AS TIMESTAMP) --Past 13 months
--== Static Test Event filters
--AND A.Name NOT LIKE '%DoubleDutch%' AND B.Name NOT LIKE '%DoubleDutch%' AND UPPER(B.Name) NOT IN ('PRIDE','DDQA')
--AND A.BundleId NOT IN ('00000000-0000-0000-0000-000000000000','025AA15B-CE74-40AA-A4CC-04028401C8B3','89FD8F03-0D59-41AB-A6A7-2237D8AC4EB2','5A46600A-156A-441E-B594-40F7DEFB54F2','F95FE4A7-E86A-4661-AC59-8B423F1F540A','34B4E501-3F31-46A0-8F2A-0FB6EA5E4357','09E25995-8D8F-4C2D-8F55-15BA22595E11','5637BE65-6E3F-4095-BEB8-115849B5584A','9F3489D7-C93C-4C8B-8603-DDA6A9061116','D0F56154-E8E7-4566-A845-D3F47B8B35CC','BC35D4CE-C571-4F91-834A-A8136CA137C4','3E3FDA3D-A606-4013-8DDF-711A1871BD12','75CE91A5-BCC0-459A-B479-B3956EA09ABC','384D052E-0ABD-44D1-A643-BC590135F5A0','B752A5B3-AA53-4BCF-9F52-D5600474D198','15740A5A-25D8-4DC6-A9ED-7F610FF94085','0CBC9D00-1E6D-4DB3-95FC-C5FBB156C6DE','F0C4B2DB-A743-4FB2-9E8F-A80463E52B55','8A995A58-C574-421B-8F82-E3425D9054B0','6DBB91C8-6544-48EF-8B8D-A01B435F3757','F21325D8-3A43-4275-A8B8-B4B6E3F62DE0','DE8D1832-B4EA-4BD2-AB4B-732321328B04','7E289A59-E573-454C-825B-CF31B74C8506')
;

--Get the initial set of Sessions we'll be working with (~5 minutes) for all aggregates
CREATE TEMPORARY TABLE BaseSessions AS 
SELECT 
SRC, ApplicationId, UserId, DeviceId, AppTypeId, StartDate, EndDate,
CASE 
  WHEN a.BinaryVersion ~ '[A-Za-z]' AND a.BinaryVersion LIKE '%-%' THEN SUBSTRING(REGEXP_REPLACE(a.BinaryVersion,'[A-Za-z]','','g'),0,POSITION('-' IN REGEXP_REPLACE(a.BinaryVersion,'[A-Za-z]','','g')))
  WHEN a.BinaryVersion ~ '[A-Za-z]' AND a.BinaryVersion NOT LIKE '%-%' THEN SUBSTRING(REGEXP_REPLACE(a.BinaryVersion,'[A-Za-z]','','g'),0,POSITION('[A-Za-z]' IN REGEXP_REPLACE(a.BinaryVersion,'[A-Za-z]','','g')))
  ELSE a.BinaryVersion
END AS Binary_Version,
CASE
    WHEN a.BinaryVersion IS NULL THEN NULL
    WHEN a.BinaryVersion ~ '[A-Za-z]' THEN NULL
    WHEN LENGTH(a.BinaryVersion) - LENGTH(REGEXP_REPLACE(a.BinaryVersion,'\.','','g')) / LENGTH('.') = 1 THEN (CAST(SUBSTRING(a.BinaryVersion,0,POSITION('.' IN a.BinaryVersion)) AS INT) * 10000) + (CAST(SUBSTRING(a.BinaryVersion,POSITION('.' IN a.BinaryVersion) + 1,999) AS INT) * 100)
    WHEN LENGTH(a.BinaryVersion) - LENGTH(REGEXP_REPLACE(a.BinaryVersion,'\.','','g')) / LENGTH('.') = 2 THEN (CAST(SUBSTRING(a.BinaryVersion,0,POSITION('.' IN a.BinaryVersion)) AS INT) * 10000) + (CAST(SUBSTRING(SUBSTRING(a.BinaryVersion,POSITION('.' IN a.BinaryVersion)+1,999),0,POSITION('.' IN SUBSTRING(a.BinaryVersion,POSITION('.' IN a.BinaryVersion)+1,999))) AS INT) * 100) + (CAST(CASE WHEN SUBSTRING(SUBSTRING(a.BinaryVersion,POSITION('.' IN a.BinaryVersion)+1,999),CAST(POSITION('.' IN SUBSTRING(a.BinaryVersion,POSITION('.' IN a.BinaryVersion)+1,999)) AS INT) + 1,999) LIKE '%.%' THEN SUBSTRING(SUBSTRING(SUBSTRING(a.BinaryVersion,POSITION('.' IN a.BinaryVersion)+1,999),CAST(POSITION('.' IN SUBSTRING(a.BinaryVersion,POSITION('.' IN a.BinaryVersion)+1,999)) AS INT) + 1,999),0,POSITION('.' IN SUBSTRING(SUBSTRING(a.BinaryVersion,POSITION('.' IN a.BinaryVersion)+1,999),CAST(POSITION('.' IN SUBSTRING(a.BinaryVersion,POSITION('.' IN a.BinaryVersion)+1,999)) AS INT) + 1,999))) ELSE SUBSTRING(SUBSTRING(a.BinaryVersion,POSITION('.' IN a.BinaryVersion)+1,999),CAST(POSITION('.' IN SUBSTRING(a.BinaryVersion,POSITION('.' IN a.BinaryVersion)+1,999)) AS INT) + 1,999) END AS INT))
    WHEN LENGTH(a.BinaryVersion) - LENGTH(REGEXP_REPLACE(a.BinaryVersion,'\.','','g')) / LENGTH('.') = 3 THEN (CAST(SUBSTRING(a.BinaryVersion,0,POSITION('.' IN a.BinaryVersion)) AS INT) * 10000) + (CAST(SUBSTRING(SUBSTRING(a.BinaryVersion,POSITION('.' IN a.BinaryVersion)+1,999),0,POSITION('.' IN SUBSTRING(a.BinaryVersion,POSITION('.' IN a.BinaryVersion)+1,999))) AS INT) * 100) + (CAST(CASE WHEN SUBSTRING(SUBSTRING(a.BinaryVersion,POSITION('.' IN a.BinaryVersion)+1,999),CAST(POSITION('.' IN SUBSTRING(a.BinaryVersion,POSITION('.' IN a.BinaryVersion)+1,999)) AS INT) + 1,999) LIKE '%.%' THEN SUBSTRING(SUBSTRING(SUBSTRING(a.BinaryVersion,POSITION('.' IN a.BinaryVersion)+1,999),CAST(POSITION('.' IN SUBSTRING(a.BinaryVersion,POSITION('.' IN a.BinaryVersion)+1,999)) AS INT) + 1,999),0,POSITION('.' IN SUBSTRING(SUBSTRING(a.BinaryVersion,POSITION('.' IN a.BinaryVersion)+1,999),CAST(POSITION('.' IN SUBSTRING(a.BinaryVersion,POSITION('.' IN a.BinaryVersion)+1,999)) AS INT) + 1,999))) ELSE SUBSTRING(SUBSTRING(a.BinaryVersion,POSITION('.' IN a.BinaryVersion)+1,999),CAST(POSITION('.' IN SUBSTRING(a.BinaryVersion,POSITION('.' IN a.BinaryVersion)+1,999)) AS INT) + 1,999) END AS INT))
  END AS BinaryVersionInt
FROM EventCube.Sessions a
WHERE (a.BinaryVersion IS NULL OR UPPER(a.BinaryVersion) NOT LIKE '%FLOCK%') --Don't include any FLOCK/Test eapps
AND a.GlobalUserId IS NOT NULL
AND a.ApplicationId IN (SELECT ApplicationId FROM EventCube.BaseApps) --Base Apps we'll be cubing  
AND a.StartDate <= CURRENT_DATE
;
--======================================================================================================================================================--

--===========--
-- TRANSFORM --
--===========--
--Identify Devices Aggregate at Event-level (~ minutes)
DROP TABLE IF EXISTS EventCube.Agg_Devices_per_App CASCADE;
CREATE TABLE EventCube.Agg_Devices_per_App AS 
SELECT ApplicationId, COUNT(*) AS Devices FROM (SELECT DISTINCT ApplicationId, DeviceId FROM BaseSessions) t GROUP BY 1;

CREATE INDEX ndx_agg_devices_App ON EventCube.Agg_Devices_per_App(ApplicationId);

--Identify Devices Aggregate at User-level
DROP TABLE IF EXISTS EventCube.Agg_Devices_per_User CASCADE;
CREATE TABLE EventCube.Agg_Devices_per_User AS 
SELECT UserId, COUNT(*) AS Devices FROM (SELECT DISTINCT UserId, DeviceId FROM BaseSessions WHERE UserId IS NOT NULL) t GROUP BY 1; --UserId is NULL for pre-logged in users

CREATE INDEX ndx_agg_devices_User ON EventCube.Agg_Devices_per_User(UserId);

--Identify Session Aggregate at User-level
DROP TABLE IF EXISTS EventCube.Agg_Session_per_AppUser CASCADE;
CREATE TABLE EventCube.Agg_Session_per_AppUser AS 
SELECT ApplicationId, UserId, 
MIN(StartDate) AS FirstTimestamp, MAX(EndDate) AS LastTimestamp, 
MIN(Binary_Version) AS FirstBinaryVersion, MAX(Binary_Version) AS LastBinaryVersion, 
MIN(BinaryVersionInt) AS FirstBinaryVersionInt, MAX(BinaryVersionInt) AS LastBinaryVersionInt, 
COUNT(*) AS Sessions,
COUNT(CASE WHEN a.StartDate >= b.StartDate AND a.StartDate <= b.EndDate THEN 1 ELSE NULL END) AS EventSessions,
SUM(CASE WHEN AppTypeId = 1 THEN 1 ELSE 0 END) AS iPhone_Sessions,
SUM(CASE WHEN AppTypeId = 2 THEN 1 ELSE 0 END) AS iPad_Sessions,
SUM(CASE WHEN AppTypeId = 3 THEN 1 ELSE 0 END) AS Android_Sessions,
SUM(CASE WHEN AppTypeId = 4 THEN 1 ELSE 0 END) AS HTML5_Sessions
FROM BaseSessions
GROUP BY ApplicationId, UserId;

CREATE INDEX ndx_agg_sessions_User ON EventCube.Agg_Session_per_AppUser(ApplicationId);

--Identify Session Aggregate at User/AppTypeId-level
DROP TABLE IF EXISTS EventCube.Agg_Session_per_AppUserAppTypeId CASCADE;
CREATE TABLE EventCube.Agg_Session_per_AppUserAppTypeId AS 
SELECT *,
CASE
  WHEN iPhone_Sessions  > iPad_Sessions    AND iPhone_Sessions > Android_Sessions AND iPhone_Sessions > HTML5_Sessions   THEN 'iPhone'
  WHEN iPad_Sessions    > iPhone_Sessions  AND iPad_Sessions > Android_Sessions   AND iPad_Sessions > HTML5_Sessions     THEN 'iPad'
  WHEN Android_Sessions > iPhone_Sessions  AND Android_Sessions > iPad_Sessions   AND Android_Sessions > HTML5_Sessions  THEN 'Android'
  WHEN HTML5_Sessions   > iPhone_Sessions  AND HTML5_Sessions > iPad_Sessions     AND HTML5_Sessions > Android_Sessions  THEN 'HTML5'
  WHEN iPhone_Sessions  = iPad_Sessions    AND iPhone_Sessions > Android_Sessions AND iPhone_Sessions > HTML5_Sessions   THEN 'iPhone/iPad'
  WHEN iPhone_Sessions  = Android_Sessions AND iPhone_Sessions > iPad_Sessions    AND iPhone_Sessions > HTML5_Sessions   THEN 'iPhone/Android'
  WHEN iPhone_Sessions  = HTML5_Sessions   AND iPhone_Sessions > iPad_Sessions    AND iPhone_Sessions > Android_Sessions THEN 'iPhone/HTML5'
  WHEN iPad_Sessions    = Android_Sessions AND iPad_Sessions > iPhone_Sessions    AND iPad_Sessions > HTML5_Sessions     THEN 'iPad/Android'
  WHEN iPad_Sessions    = HTML5_Sessions   AND iPad_Sessions > iPhone_Sessions    AND iPad_Sessions > Android_Sessions   THEN 'iPad/HTML5'
  WHEN Android_Sessions = HTML5_Sessions   AND Android_Sessions > iPhone_Sessions AND Android_Sessions > iPad_Sessions   THEN 'Android/HTML5'
  ELSE 'Multiple Devices'
END AS DevicePreference
FROM EventCube.Agg_Session_per_AppUser;

CREATE INDEX ndx_agg_sessions_UserAppTypeId ON EventCube.Agg_Session_per_AppUserAppTypeId(ApplicationId);

--Identify User/Month Aggregate for Session Counts (for past 6 months)
DROP TABLE IF EXISTS EventCube.Agg_Session_perUser_perMonth CASCADE;
CREATE TABLE EventCube.Agg_Session_perUser_perMonth AS 
SELECT CAST(EXTRACT(Year FROM CAST(StartDate AS Date)) AS TEXT)||'-'||CASE WHEN CAST(EXTRACT(Month FROM CAST(StartDate AS Date)) AS INT) < 10 THEN '0' ELSE '' END||CAST(EXTRACT(Month FROM CAST(StartDate AS Date)) AS TEXT) AS YYYY_MM, UserId, COUNT(*) AS Sessions
FROM BaseSessions
WHERE /*ApplicationId NOT IN (SELECT ApplicationId FROM EventCube.TestEvents) 
AND*/ StartDate >= CAST(CAST(EXTRACT(YEAR FROM CAST(CURRENT_DATE AS TIMESTAMP) - INTERVAL'6 months') AS TEXT) || '-' || CASE WHEN EXTRACT(MONTH FROM CAST(CURRENT_DATE AS TIMESTAMP) - INTERVAL'6 months') < 10 THEN '0' ELSE '' END || CAST(EXTRACT(MONTH FROM CAST(CURRENT_DATE AS TIMESTAMP) - INTERVAL'6 months') AS TEXT) || '-01 00:00:00' AS TIMESTAMP)
GROUP BY 1,2;

--======================================================================================================================================================--

--=======--
-- STAGE --
--=======--

--Stage the list of Test Events we've found
TRUNCATE TABLE EventCube.STG_TestEvents;
VACUUM EventCube.STG_TestEvents;
INSERT INTO EventCube.STG_TestEvents
SELECT S.*
FROM
--============================================================================================================
-- Identify the Test Events through two methods:
-- 1a. Identify if the naming of the Event has anything to do with a DoubleDutch test/internal/QA Event
-- 1b. Identify if the specific Bundle Unique ID is tied to a test event (as specified by internal users)
-- 2a. Check if the Event has 20 or fewer Users across all Event sessions (or no Event sessions at all)...
-- 2b. ... or if the Event does have < 20 Users across all Event sessions, whether it was entered in SalesForce as a legit event
--============================================================================================================
( SELECT DISTINCT ApplicationId, TRIM(A.Name) AS NAME
  FROM PUBLIC.AuthDB_Applications A
  JOIN PUBLIC.AuthDB_Bundles B ON A.BundleId = B.BundleId
  
  -- 1a --
  WHERE A.Name LIKE '%DoubleDutch%'
  OR B.Name LIKE '%DoubleDutch%'
  OR B.Name IN ('pride','DDQA')
  
  -- 1b --
  OR A.BundleId IN ('00000000-0000-0000-0000-000000000000','025AA15B-CE74-40AA-A4CC-04028401C8B3','89FD8F03-0D59-41AB-A6A7-2237D8AC4EB2','5A46600A-156A-441E-B594-40F7DEFB54F2','F95FE4A7-E86A-4661-AC59-8B423F1F540A','34B4E501-3F31-46A0-8F2A-0FB6EA5E4357','09E25995-8D8F-4C2D-8F55-15BA22595E11','5637BE65-6E3F-4095-BEB8-115849B5584A','9F3489D7-C93C-4C8B-8603-DDA6A9061116','D0F56154-E8E7-4566-A845-D3F47B8B35CC','BC35D4CE-C571-4F91-834A-A8136CA137C4','3E3FDA3D-A606-4013-8DDF-711A1871BD12','75CE91A5-BCC0-459A-B479-B3956EA09ABC','384D052E-0ABD-44D1-A643-BC590135F5A0','B752A5B3-AA53-4BCF-9F52-D5600474D198','15740A5A-25D8-4DC6-A9ED-7F610FF94085','0CBC9D00-1E6D-4DB3-95FC-C5FBB156C6DE','F0C4B2DB-A743-4FB2-9E8F-A80463E52B55','8A995A58-C574-421B-8F82-E3425D9054B0','6DBB91C8-6544-48EF-8B8D-A01B435F3757','F21325D8-3A43-4275-A8B8-B4B6E3F62DE0','DE8D1832-B4EA-4BD2-AB4B-732321328B04','7E289A59-E573-454C-825B-CF31B74C8506')
  UNION
  
  -- 2 --
  SELECT ApplicationId, TRIM(Name) AS NAME
  FROM (
        -- 2a --
        SELECT A.ApplicationId, A.Name, COUNT(*)
        FROM EventCube.BaseApps A
        LEFT JOIN EventCube.Agg_Session_per_AppUser S ON A.ApplicationId = S.ApplicationId
        -- 2b --
        --WHERE A.ApplicationId NOT IN (
        
                -- Identify the events for which the opportunity looks like a regular app that will be cut for a real event
                --SELECT DISTINCT UPPER(a.SF_Event_Id_CMS__c) AS ApplicationId
                --FROM Integrations.Implementation__c a
                --JOIN Integrations.Opportunity b ON a.SF_Opportunity__c = b.SF_Id
                --WHERE b.SF_StageName = 'Closed Won'
                --AND a.SF_Stage__c IN ('3. Assembly','4. Pre-Release','5. Released','COMPLETE')
        --)
        GROUP BY 1,2
        HAVING COUNT(*) <= 10
  ) t
) S;

--======================================================================================================================================================--

--=========--
-- DELSERT --
--=========--
--Identify the Events that were not previously marked as Test Events, but are now considered Test Events based on today's Sessions calculation
TRUNCATE TABLE EventCube.STG_TestEvents_INSERT;
VACUUM EventCube.STG_TestEvents_INSERT;
INSERT INTO EventCube.STG_TestEvents_INSERT
SELECT * FROM EventCube.STG_TestEvents WHERE ApplicationId NOT IN (SELECT ApplicationId FROM EventCube.TestEvents);

--Identify the Events that were previously marked as Test Events, but are no longer considered Test Events based on today's Sessions calculation
TRUNCATE TABLE EventCube.STG_TestEvents_DELETE;
VACUUM EventCube.STG_TestEvents_DELETE;
INSERT INTO EventCube.STG_TestEvents_DELETE
SELECT * FROM EventCube.TestEvents 
WHERE ApplicationId IN (SELECT ApplicationId FROM EventCube.BaseApps)
AND ApplicationId NOT IN (SELECT ApplicationId FROM EventCube.STG_TestEvents);

INSERT INTO EventCube.TestEvents SELECT * FROM EventCube.STG_TestEvents_INSERT;
DELETE FROM EventCube.TestEvents WHERE ApplicationId IN (SELECT ApplicationId FROM EventCube.STG_TestEvents_DELETE);
VACUUM EventCube.TestEvents;

--======================================================================================================================================================--
