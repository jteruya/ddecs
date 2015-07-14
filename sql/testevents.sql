-- Identfy the number of users with sessions per each Event
DROP TABLE IF EXISTS EventCube.Agg_Session_per_AppUser;

CREATE TABLE EventCube.Agg_Session_per_AppUser AS 
SELECT Application_Id AS ApplicationId, User_Id AS UserId, MIN(Start_Date) FirstTimestamp, MAX(Start_Date) LastTimestamp
FROM PUBLIC.Fact_Sessions_Old
WHERE User_Id IS NOT NULL
GROUP BY Application_Id, User_Id;

CREATE INDEX ndx_agg_sessions_applicationid ON EventCube.Agg_Session_per_AppUser(ApplicationId);

--Begin to build the list of Test Events
DROP TABLE IF EXISTS EventCube.TestEvents;

CREATE TABLE EventCube.TestEvents AS
SELECT S.*
FROM
--============================================================================================================
-- Identify the Test Events through two methods:
-- 1a. Identify if the naming of the Event has anything to do with a DoubleDutch test/internal/QA Event
-- 1b. Identify if the specific Bundle Unique ID is tied to a test event (as specified by internal users)
-- 2.  Check if the Event has 20 or fewer Users across all Event sessions (or no Event sessions at all)
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
  SELECT A.ApplicationId, TRIM(A.Name) AS NAME
  FROM PUBLIC.AuthDB_Applications A
  LEFT JOIN EventCube.Agg_Session_per_AppUser S ON A.ApplicationId = S.ApplicationId
  GROUP BY 1,2
  HAVING COUNT(*) <= 20

) S
